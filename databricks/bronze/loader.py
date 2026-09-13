"""Generic Bronze loader — reads raw JSON from S3, lands into Bronze Delta tables.

No business logic, no DQ checks. Bronze is a faithful landing zone per Medallion.
Row counts come from Delta Log transaction metrics, not .count().
"""

from pyspark.sql.functions import current_timestamp, col, input_file_name
from delta.tables import DeltaTable

from databricks.bronze.bronze_config import BRONZE_REGISTRY


def run_bronze_load(
    resource_type: str,
    s3_raw_path: str,
    s3_bronze_path: str,
    bronze_table: str,
    ingestion_date: str,
    spark,
) -> None:
    """Loads one resource type from S3 raw into a Bronze Delta table.

    Args:
        resource_type:  e.g. "repositories", "issues"
        s3_raw_path:    e.g. s3://github-repository-data-analysis/raw/repositories/ingestion_date=2026-09-13/
        s3_bronze_path: e.g. s3://github-repository-data-analysis/bronze/repositories
        bronze_table:   e.g. github_analytics.bronze.repositories
        ingestion_date: e.g. 2026-09-13
        spark:          Active SparkSession (Databricks runtime).
    """
    raw_df = spark.read.json(s3_raw_path)

    if raw_df.isEmpty():
        print(f"[bronze] SKIP {resource_type}: no raw files at {s3_raw_path}")
        return

    bronze_df = (
        raw_df
        .withColumn("_bronze_ingest_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_raw_s3_uri", col("lineage.source_s3_uri"))
        .withColumn("_extraction_run_id", col("lineage.extraction_run_id"))
        .withColumn("_ingestion_date", col("lineage.ingestion_date"))
        .withColumn("_payload_checksum", col("lineage.payload_checksum"))
    )

    # Idempotent: replaceWhere on partition = same date re-run replaces, not appends
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_ingestion_date = '{ingestion_date}'")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .save(s3_bronze_path)
    )

    # Register/refresh the external table pointer (no-op after first run)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {bronze_table}
        USING DELTA
        LOCATION '{s3_bronze_path}'
    """)

    # Log row count from Delta Log — no .count() scan
    _log_delta_metrics(spark, s3_bronze_path, resource_type)


def run_all_bronze_loads(
    bucket: str,
    catalog: str,
    schema: str,
    ingestion_date: str,
    spark,
) -> None:
    """Iterates BRONZE_REGISTRY and loads all resource types.

    Args:
        bucket:         S3 bucket name.
        catalog:        Unity Catalog catalog name.
        schema:         Unity Catalog schema name.
        ingestion_date: e.g. 2026-09-13
        spark:          Active SparkSession.
    """
    summary = []

    for resource_type, config in BRONZE_REGISTRY.items():
        s3_raw_path = f"s3://{bucket}/{config['raw_prefix']}/ingestion_date={ingestion_date}/"
        s3_bronze_path = f"s3://{bucket}/{config['bronze_prefix']}"
        bronze_table = config["table_name"].format(catalog=catalog, schema=schema)

        print(f"[bronze] Loading {resource_type}: {s3_raw_path} -> {s3_bronze_path}")

        try:
            run_bronze_load(
                resource_type=resource_type,
                s3_raw_path=s3_raw_path,
                s3_bronze_path=s3_bronze_path,
                bronze_table=bronze_table,
                ingestion_date=ingestion_date,
                spark=spark,
            )
            rows = _get_last_write_rows(spark, s3_bronze_path)
            summary.append((resource_type, bronze_table, rows, "OK"))
        except Exception as e:
            print(f"[bronze] FAILED {resource_type}: {e}")
            summary.append((resource_type, bronze_table, 0, f"FAILED: {e}"))
            raise

    # Summary log
    print("\n[bronze] === Load Summary ===")
    for resource_type, table, rows, status in summary:
        print(f"  {resource_type:<16} | {table:<45} | rows={rows:<8} | {status}")
    print("[bronze] === Done ===\n")


def _log_delta_metrics(spark, s3_bronze_path: str, resource_type: str) -> None:
    """Reads the last Delta Log entry to log write metrics without scanning the table."""
    try:
        dt = DeltaTable.forPath(spark, s3_bronze_path)
        history = dt.history(1).select("operationMetrics").collect()
        if history and history[0][0]:
            metrics = history[0][0]
            rows_written = metrics.get("numOutputRows", "unknown")
            bytes_written = metrics.get("numOutputBytes", "unknown")
            print(
                f"[bronze] Delta metrics for {resource_type}: "
                f"rows_written={rows_written}, bytes_written={bytes_written}"
            )
    except Exception as e:
        # Non-fatal — metrics are informational
        print(f"[bronze] Could not read Delta metrics for {resource_type}: {e}")


def _get_last_write_rows(spark, s3_bronze_path: str) -> int:
    """Returns numOutputRows from the last Delta Log entry, or 0 if unavailable."""
    try:
        dt = DeltaTable.forPath(spark, s3_bronze_path)
        history = dt.history(1).select("operationMetrics").collect()
        if history and history[0][0]:
            return int(history[0][0].get("numOutputRows", 0))
    except Exception:
        pass
    return 0
