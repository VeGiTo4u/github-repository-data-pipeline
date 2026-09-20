"""GitHub Repository Analytics DAG — orchestration only.

Extracts metadata from multiple GitHub repositories in parallel using
Airflow Dynamic Task Mapping. Each repo gets an independent mapped task
with its own watermark for incremental extraction.

After all repos are extracted to S3 raw, triggers a Databricks serverless
job to load raw data into Bronze Delta tables, then runs dbt build to
process the Silver layer (staging → intermediate → snapshots → marts + tests).

Zero business logic in this file.
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=12),
}

with DAG(
    dag_id="github_repository_analytics",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=2,
    max_active_tasks=3,
    default_args=default_args,
    tags=["github", "analytics", "phase1", "phase2"],
) as dag:

    def _extract_and_load_repo(repo: str, **context):
        """One mapped-task instance per repo — independent watermark, independent failure."""
        from ingestion.extractor import extract_repository_metadata, extract_pr_details
        from ingestion.s3_writer import write_raw_to_s3
        from airflow.hooks.base import BaseHook
        import os

        # --- AWS credentials ---
        try:
            aws_conn = BaseHook.get_connection("aws_default")
            aws_access_key_id = aws_conn.login
            aws_secret_access_key = aws_conn.password
            aws_region = aws_conn.extra_dejson.get("region_name", "us-east-1")
        except Exception:
            aws_access_key_id = None
            aws_secret_access_key = None
            aws_region = "us-east-1"

        github_token = Variable.get("GITHUB_TOKEN")
        s3_bucket = Variable.get("s3_bucket_name")
        run_id = context["run_id"]
        # data_interval_end is the day the @daily run executes (not ds, which is yesterday)
        ingestion_date = context["data_interval_end"].strftime("%Y-%m-%d")

        # --- per-repo watermark (atomic scalar Variable, no shared JSON blob) ---
        # ponytail: one Variable per repo eliminates the read-modify-write race
        repo_slug = repo.replace("/", "_")
        since = Variable.get(f"watermark_{repo_slug}", default_var=None)
        current_run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # --- extract all resources for this repo ---
        results, pr_numbers = extract_repository_metadata(
            repo_full_name=repo,
            extraction_run_id=run_id,
            github_token=github_token,
            s3_bucket=s3_bucket,
            since=since,
            ingestion_date=ingestion_date,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_region=aws_region,
        )

        # --- extract PR details if any new PRs were fetched ---
        if pr_numbers:
            extract_pr_details(
                repo_full_name=repo,
                pr_numbers=pr_numbers,
                extraction_run_id=run_id,
                github_token=github_token,
                s3_bucket=s3_bucket,
                ingestion_date=ingestion_date,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_region=aws_region,
            )

        # ponytail: save watermark BEFORE uploading temp files. Tempfile resources (repos, languages) 
        # don't use 'since', so they re-fetch safely for 1 API call on retry. Streamed resources 
        # (issues, PRs) use 'since' and are already on S3, so this prevents massive re-fetching.
        Variable.set(f"watermark_{repo_slug}", current_run_ts)

        # --- upload each resource file to S3 ---
        for temp_file_path, s3_key, resource_type in results:
            if temp_file_path is None:
                # Already streamed directly to S3 (part files)
                continue
            if os.path.exists(temp_file_path):
                write_raw_to_s3(
                    temp_file_path=temp_file_path,
                    s3_key=s3_key,
                    bucket=s3_bucket,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    aws_region=aws_region,
                    run_id=run_id,
                )
                # only unlink on success so retry can skip if we implement local caching later
                os.unlink(temp_file_path)

    # Dynamic Task Mapping — one task instance per repo in repo_list
    # ponytail: falls back to all repos in REPO_REGISTRY when Variable is unset
    from ingestion.repo_config import REPO_REGISTRY
    _repo_list = Variable.get(
        "repo_list", deserialize_json=True, default_var=list(REPO_REGISTRY.keys())
    )

    extract_repos = PythonOperator.partial(
        task_id="extract_repo",
        python_callable=_extract_and_load_repo,
    ).expand(
        op_kwargs=[{"repo": r} for r in _repo_list],
    )

    # Bronze load — triggered after all repos finish extraction
    # Runs on Databricks serverless (no cluster_id = serverless default)
    run_bronze = DatabricksSubmitRunOperator(
        task_id="run_bronze_load",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "bronze_load_{{ ds }}",
            "tasks": [
                {
                    "task_key": "load_bronze",
                    "notebook_task": {
                        "notebook_path": Variable.get(
                            "databricks_notebook_path",
                            default_var="/Workspace/Repos/default/Github-Repository-Data-Analysis/databricks/bronze/bronze_notebook",
                        ),
                        "base_parameters": {
                            "s3_bucket": Variable.get("s3_bucket_name"),
                            "catalog": Variable.get("databricks_catalog"),
                            "schema": Variable.get("databricks_schema"),
                            "ingestion_date": "{{ data_interval_end | ds }}",
                        },
                    },
                }
            ],
        },
    )

    # Silver layer — dbt build (staging views → intermediate tables →
    # SCD2 snapshots → silver marts + all tests) in DAG-resolved order.
    # Runs inside the Airflow container where dbt-databricks is installed.
    # profiles.yml + env vars are already configured via docker-compose.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt && dbt clean && dbt build --profiles-dir /opt/airflow/dbt",
        execution_timeout=timedelta(hours=2),
    )

    # Export KPI tables to S3 as Parquet
    export_kpis = DatabricksSubmitRunOperator(
        task_id="export_kpis",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "export_kpis_{{ ds }}",
            "tasks": [
                {
                    "task_key": "export_kpis",
                    "notebook_task": {
                        "notebook_path": Variable.get(
                            "databricks_export_notebook_path",
                            default_var=Variable.get(
                                "databricks_notebook_path",
                                default_var="/Workspace/Repos/default/Github-Repository-Data-Analysis/databricks/bronze/bronze_notebook"
                            ).replace("/bronze/bronze_notebook", "/export/export_kpis_notebook"),
                        ),
                        "base_parameters": {
                            "s3_bucket": Variable.get("s3_bucket_name"),
                            "catalog": Variable.get("databricks_catalog"),
                            "schema": Variable.get("databricks_schema"),
                        },
                    },
                }
            ],
        },
    )

    extract_repos >> run_bronze >> dbt_build >> export_kpis
