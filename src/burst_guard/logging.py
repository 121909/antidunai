from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "event": getattr(record, "event", record.getMessage()),
        }
        fields = getattr(record, "fields", {})
        data.update({key: value for key, value in fields.items() if value is not None})
        if record.exc_info:
            data["error_type"] = record.exc_info[0].__name__ if record.exc_info[0] else "unknown"
        return json.dumps(data, ensure_ascii=True, separators=(",", ":"))


def configure_logging(level: str) -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    return logging.getLogger("burst_guard")


def log_event(logger: logging.Logger, level: int, event: str, **fields: object) -> None:
    logger.log(level, event, extra={"event": event, "fields": fields})
