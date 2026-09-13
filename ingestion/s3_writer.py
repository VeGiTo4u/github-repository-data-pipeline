import json
import boto3
from ingestion.logger import get_logger


def write_raw_to_s3(
    temp_file_path: str,
    s3_key: str,
    bucket: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str = "us-east-1",
    run_id: str | None = None,
) -> str:
    """Writes a JSON Lines tempfile to S3 raw zone (Phase-1.md Section 8.3).

    Idempotency: same key on re-run = overwrite (PutObject to same key).

    Args:
        temp_file_path: Path to the local JSON Lines file.
        s3_key: The destination S3 key.
        bucket: S3 bucket name.
        aws_access_key_id: Optional — if None, uses boto3 default credential chain.
        aws_secret_access_key: Optional — same as above.
        aws_region: AWS region.
        run_id: For structured logging.

    Returns:
        The S3 key that was written.
    """
    log = get_logger(__name__, run_id=run_id)

    client_kwargs = {"region_name": aws_region}
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs["aws_access_key_id"] = aws_access_key_id
        client_kwargs["aws_secret_access_key"] = aws_secret_access_key

    s3 = boto3.client("s3", **client_kwargs)

    s3.upload_file(
        Filename=temp_file_path,
        Bucket=bucket,
        Key=s3_key,
        ExtraArgs={"ContentType": "application/x-ndjson"},
    )

    log.info(f"Wrote s3://{bucket}/{s3_key} from {temp_file_path}")
    return s3_key
