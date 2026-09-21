import json
import logging
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Formats log records as JSON lines (Section 10 of Phase-1)."""

    def format(self, record):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "run_id": getattr(record, "run_id", None),
            "message": record.getMessage(),
        }
        extra = getattr(record, "extra", None)
        if extra:
            entry["extra"] = extra
        return json.dumps(entry)


def get_logger(name: str, run_id: str | None = None) -> logging.Logger:
    """Returns a structured JSON-lines logger.
    We enforce JSON formatting so downstream observability tools (like DataDog or 
    CloudWatch) can easily index the logs without complex regex parsing.
    
    Args:
        name: Logger name (typically __name__ of the calling module).
        run_id: Airflow run_id or a UUID for local runs.
    """
    
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    # Attach run_id so every message from this logger includes it
    return logging.LoggerAdapter(logger, {"run_id": run_id})
