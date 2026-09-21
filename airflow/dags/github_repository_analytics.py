"""GitHub Repository Analytics DAG.
We orchestrate using Airflow to leverage Dynamic Task Mapping, allowing us to ingest 
multiple repositories in parallel. We enforce a strict separation of concerns here: 
Airflow only handles scheduling and retries, while all business logic resides in 
the ingestion package or dbt, preventing vendor lock-in to the orchestrator.
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
    max_active_runs=1,
    max_active_tasks=3,
    default_args=default_args,
    tags=["github", "analytics", "phase1", "phase2"],
) as dag:

    def _extract_and_load_repo(repo: str, **context):
        """Extracts and loads a single repository.
        Captures the extraction boundary timestamp before extraction begins
        so watermarks represent the actual source boundary, not clock time
        of a later task."""
        from ingestion.extractor import extract_repository_metadata, extract_pr_details
        from airflow.hooks.base import BaseHook
        import os

        # --- Capture extraction boundary BEFORE extraction begins ---
        extraction_boundary = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
        repo_slug = repo.replace("/", "_")
        since = Variable.get(f"watermark_{repo_slug}", default_var=None)
        if since:
            since_dt = datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ")
            since = (since_dt - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")

        # --- extract all resources for this repo ---
        pr_numbers = extract_repository_metadata(
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

        # Return extraction boundary via XCom for downstream watermark commit
        return {"repo": repo, "extraction_boundary": extraction_boundary}



    # Dynamic Task Mapping — one task instance per repo in repo_list
    # Falls back to all repos in REPO_REGISTRY when Variable is unset
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
        bash_command="cd /opt/airflow/dbt && dbt build --profiles-dir /opt/airflow/dbt",
        execution_timeout=timedelta(hours=2),
    )

    def _advance_watermarks(**context):
        from airflow.models import Variable
        import json
        # Read extraction boundaries from XCom (pushed by each extract_repo task)
        ti = context["ti"]
        boundaries = ti.xcom_pull(task_ids="extract_repo", key="return_value")
        if boundaries:
            for entry in boundaries:
                if entry and isinstance(entry, dict):
                    repo = entry["repo"]
                    extraction_boundary = entry["extraction_boundary"]
                    repo_slug = repo.replace("/", "_")
                    Variable.set(f"watermark_{repo_slug}", extraction_boundary)

    advance_watermarks = PythonOperator(
        task_id="advance_watermarks",
        python_callable=_advance_watermarks,
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
                            "schema": "gold",
                        },
                    },
                }
            ],
        },
    )

    extract_repos >> run_bronze >> advance_watermarks >> dbt_build >> export_kpis
