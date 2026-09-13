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
    old_factory = logger.makeRecord.__func__ if hasattr(logger.makeRecord, '__func__') else None

    class _RunIdAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            kwargs.setdefault("extra", {})
            kwargs["extra"]["run_id"] = run_id
            return msg, kwargs

    adapter = _RunIdAdapter(logger, {"run_id": run_id})
    return adapter
