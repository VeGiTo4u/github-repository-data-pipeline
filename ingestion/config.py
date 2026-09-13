import os
from dotenv import load_dotenv


def load_config() -> dict:
    """Loads config from .env for local dev. Returns a dict of config values.

    Inside Airflow, these values come from Variables/Connections instead —
    this function is only used for local testing.
    """
    load_dotenv()
    return {
        "github_token": os.environ.get("GITHUB_TOKEN"),
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.environ.get("AWS_REGION", "us-east-1"),
        "s3_bucket_name": os.environ.get("S3_BUCKET_NAME"),
        "databricks_host": os.environ.get("DATABRICKS_HOST"),
        "databricks_token": os.environ.get("DATABRICKS_TOKEN"),
        "databricks_catalog": os.environ.get("DATABRICKS_CATALOG"),
        "databricks_schema": os.environ.get("DATABRICKS_SCHEMA"),
    }
