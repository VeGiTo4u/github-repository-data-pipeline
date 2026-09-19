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


# ponytail: chunk size trades off S3 PUT count vs resilience — 5000 records
# (~5-10 MB) is small enough for fast uploads, large enough to avoid thousands of PUTs
STREAM_CHUNK_SIZE = 5000


def stream_records_to_s3(
    records,
    s3_key_prefix: str,
    bucket: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str = "us-east-1",
    run_id: str | None = None,
    chunk_size: int = STREAM_CHUNK_SIZE,
) -> tuple[int, list[str]]:
    """Streams envelope records directly to S3 as part files.

    Buffers `chunk_size` records, writes each chunk as a part file:
        {s3_key_prefix}/part_001.json, part_002.json, ...

    Already-uploaded parts survive if a later page fails — Airflow retry
    resumes extraction but stale parts from previous runs are harmless
    because Bronze → Staging deduplicates via row_number().

    Args:
        records:    Generator/iterable of envelope dicts.
        s3_key_prefix: e.g. "raw/issues/ingestion_date=2026-09-19/apache_spark"
        bucket:     S3 bucket name.
        chunk_size: Records per part file (default 5000).

    Returns:
        (total_record_count, list_of_s3_keys_written)
    """
    log = get_logger(__name__, run_id=run_id)

    client_kwargs = {"region_name": aws_region}
    if aws_access_key_id and aws_secret_access_key:
        client_kwargs["aws_access_key_id"] = aws_access_key_id
        client_kwargs["aws_secret_access_key"] = aws_secret_access_key

    s3 = boto3.client("s3", **client_kwargs)

    # ponytail: delete existing objects in prefix to prevent dangling parts on retry
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=s3_key_prefix):
            if "Contents" in page:
                delete_keys = [{"Key": obj["Key"]} for obj in page["Contents"]]
                if delete_keys:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_keys})
                    log.info(f"Purged {len(delete_keys)} old parts from s3://{bucket}/{s3_key_prefix}")
    except Exception as e:
        log.warning(f"Failed to purge old parts in {s3_key_prefix}: {e}")

    buffer = []
    part_num = 0
    total_count = 0
    keys_written = []

    def _flush():
        nonlocal part_num
        if not buffer:
            return
        part_num += 1
        part_key = f"{s3_key_prefix}/part_{part_num:03d}.json"
        body = ("\n".join(json.dumps(rec) for rec in buffer) + "\n").encode("utf-8")
        s3.put_object(Bucket=bucket, Key=part_key, Body=body, ContentType="application/x-ndjson")
        keys_written.append(part_key)
        log.info(f"Streamed part {part_num} ({len(buffer)} records) to s3://{bucket}/{part_key}")
        buffer.clear()

    for record in records:
        buffer.append(record)
        total_count += 1
        if len(buffer) >= chunk_size:
            _flush()

    _flush()  # remaining records

    log.info(f"Stream complete: {total_count} records in {part_num} parts to s3://{bucket}/{s3_key_prefix}/")
    return total_count, keys_written

