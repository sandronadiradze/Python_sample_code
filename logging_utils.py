import logging
import uuid
from typing import Dict, Any, Optional
import json


class CorrelatedLogger:
    """
    Extends standard logging with correlation ID and contextual information.

    Enables tracing of log entries across multiple services or components
    by attaching a unique correlation identifier to each log record.

    Attributes:
        _logger (logging.Logger): Internal logger instance
        _correlation_id (str): Unique identifier for log entry tracing
    """

    def __init__(
        self, name: str, level: int = logging.INFO, correlation_id: Optional[str] = None
    ):
        """
        Initializes a logger with optional correlation tracking.

        Args:
            name (str): Name of the logger (typically module name)
            level (int, optional): Logging level. Defaults to INFO
            correlation_id (str, optional): Custom correlation ID.
            Generates a new UUID if not provided
        """
        # Existing implementation
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(JsonFormatter())
        self._logger.handlers.clear()
        self._logger.addHandler(console_handler)
        self._correlation_id = correlation_id or str(uuid.uuid4())

    def _log(self, level: int, message: str, extra: Optional[Dict[str, Any]] = None):
        extra = extra or {}
        extra["correlation_id"] = self._correlation_id

        self._logger.log(level, message, extra=extra)

    def info(self, message: str, **kwargs):
        """Log info message with optional extra context."""
        self._log(logging.INFO, message, kwargs)

    def error(self, message: str, **kwargs):
        """Log error message with optional extra context."""
        self._log(logging.ERROR, message, kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with optional extra context."""
        self._log(logging.WARNING, message, kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message with optional extra context."""
        self._log(logging.DEBUG, message, kwargs)


class JsonFormatter(logging.Formatter):
    """Custom JSON log formatter."""

    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "module": record.module,
            "correlation_id": getattr(record, "correlation_id", "N/A"),
        }

        # Include any additional context
        if hasattr(record, "extra"):
            log_record.update(record.extra)

        return json.dumps(log_record)
