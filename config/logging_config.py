"""
Logging configuration setup.
Extract setup_logging from incremental_utils.py
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from config.settings import LOG_LEVEL, LOGS_DIR


def setup_logging(
    name: str,
    log_file: Optional[str] = None,
    level: Optional[str] = None,
    rotation: str = "size",  # "size" or "time"
) -> logging.Logger:
    """
    Setup logging configuration.

    Args:
        name: Logger name (usually __name__)
        log_file: Optional log file name (will be created in logs/ directory)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        rotation: Log rotation type ("size" or "time")

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level or LOG_LEVEL))

    # Clear existing handlers
    logger.handlers = []

    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if log_file specified)
    if log_file:
        # Ensure logs directory exists
        log_path = LOGS_DIR / log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)

        if rotation == "time":
            # Rotate daily, keep 30 days
            file_handler = TimedRotatingFileHandler(
                filename=str(log_path),
                when="midnight",
                interval=1,
                backupCount=30,
                encoding="utf-8"
            )
        else:
            # Rotate at 10MB, keep 5 files
            file_handler = RotatingFileHandler(
                filename=str(log_path),
                maxBytes=10 * 1024 * 1024,  # 10MB
                backupCount=5,
                encoding="utf-8"
            )

        file_handler.setLevel(getattr(logging, level or LOG_LEVEL))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """
    Get or create a logger with standard configuration.

    Args:
        name: Logger name
        log_file: Optional log file name

    Returns:
        Configured logger
    """
    return setup_logging(name, log_file)
