"""
Logging Configuration
=====================

Production-ready logging for the novadhruv data pipeline.

Features:
- JSON structured logging by default (LOG_FORMAT=text for plain text in dev)
- Stdout-first (12-factor): file logging is opt-in via LOG_FILE env var
- LOG_LEVEL env var sets the default level; overridable at call time
- TenantLoggerAdapter injects tenant_id, partition, and stage into every record
- @functools.wraps on decorators; exc_info=True captures stack traces
"""

import functools
import logging
import logging.handlers
import os
import sys
from typing import Optional

from pythonjsonlogger import jsonlogger


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
) -> None:
    """
    Configure root logger for the application.

    Resolution order for log level:
      1. log_level argument (explicit override)
      2. LOG_LEVEL environment variable
      3. Default: INFO

    File logging is opt-in: set log_file or LOG_FILE env var.
    If neither is set, logs go to stdout only (12-factor principle).

    Args:
        log_level: Optional level string (DEBUG / INFO / WARNING / ERROR).
        log_file: Optional path for rotating file handler.
    """
    effective_level_str = log_level or os.environ.get("LOG_LEVEL", "INFO")
    numeric_level = getattr(logging, effective_level_str.upper(), logging.INFO)

    log_format = os.environ.get("LOG_FORMAT", "json").lower()
    effective_log_file = log_file or os.environ.get("LOG_FILE")

    # Remove any existing handlers to avoid duplicates on repeated calls
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handlers: list[logging.Handler] = []

    # --- Stdout handler (always present) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    if log_format == "text":
        console_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%H:%M:%S",
            )
        )
    else:
        console_handler.setFormatter(
            jsonlogger.JsonFormatter(
                fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
    handlers.append(console_handler)

    # --- Rotating file handler (opt-in) ---
    if effective_log_file:
        import pathlib
        pathlib.Path(effective_log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            effective_log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(
            jsonlogger.JsonFormatter(
                fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        handlers.append(file_handler)

    root_logger.setLevel(numeric_level)
    for h in handlers:
        root_logger.addHandler(h)

    # Quiet noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Logging initialised",
        extra={"log_level": effective_level_str, "log_format": log_format},
    )


# ---------------------------------------------------------------------------
# Logger factory
# ---------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """Return a standard logger for the given module name."""
    return logging.getLogger(name)


# ---------------------------------------------------------------------------
# Tenant-aware logger adapter
# ---------------------------------------------------------------------------

class TenantLoggerAdapter(logging.LoggerAdapter):
    """
    LoggerAdapter that injects tenant_id, partition, and stage into every
    log record via the extra dict.

    Usage:
        logger = TenantLoggerAdapter(
            get_logger(__name__),
            tenant_id="acme",
            stage="extract",
        )
        logger.info("Starting extract")
        # JSON output: {..., "tenant_id": "acme", "stage": "extract", ...}
    """

    def __init__(
        self,
        logger: logging.Logger,
        tenant_id: str = "",
        partition: str = "",
        stage: str = "",
    ) -> None:
        extra = {
            "tenant_id": tenant_id,
            "partition": partition,
            "stage": stage,
        }
        super().__init__(logger, extra)

    def process(
        self, msg: str, kwargs: dict
    ) -> tuple[str, dict]:
        kwargs.setdefault("extra", {}).update(self.extra)
        return msg, kwargs

    def with_context(self, **kwargs: str) -> "TenantLoggerAdapter":
        """Return a new adapter with updated context fields."""
        new_extra = {**self.extra, **kwargs}
        return TenantLoggerAdapter(
            self.logger,
            tenant_id=new_extra.get("tenant_id", ""),
            partition=new_extra.get("partition", ""),
            stage=new_extra.get("stage", ""),
        )


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

def log_function_call(func):
    """Decorator to log synchronous function entry, exit, and errors."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        logger.debug(f"Entering {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"Exiting {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper


def log_async_function_call(func):
    """Decorator to log async function entry, exit, and errors."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        logger.debug(f"Entering async {func.__name__}")
        try:
            result = await func(*args, **kwargs)
            logger.debug(f"Exiting async {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in async {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper
