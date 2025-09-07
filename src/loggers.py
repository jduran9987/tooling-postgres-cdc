"""
Custom JSON logger configuration.

Provides a `JsonLogFormatter` to output logs as structured JSON
with timestamp, level, logger name, and message. Sets up a logger
named `postgres_cdc` that writes to stdout.
"""
import json
import logging
import sys
from datetime import datetime, timezone


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as a JSON string.

        :params:
        record (logging.LogRecord) - Log record to format.

        :returns:
        str - Log record serialized as a JSON string with timestamp,
            level, name, and message fields.
        """
        log_record = {
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage()
        }

        return json.dumps(log_record)

logger = logging.getLogger("postgres_cdc")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonLogFormatter())

logger.addHandler(handler)
