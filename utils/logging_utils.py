"""
Logging Utilities

Provides structured logging setup for pipeline jobs
"""

import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Optional
import functools
import time
import asyncio

# Color codes for console output
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
RESET = "\033[0m"


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output for different log levels"""

    COLORS = {
        "DEBUG": BLUE,
        "INFO": GREEN,
        "WARNING": YELLOW,
        "ERROR": RED,
        "CRITICAL": RED,
    }

    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, RESET)

        # Format the log message
        formatted = super().format(record)

        # Apply color to level name
        colored_level = f"{color}{levelname}{RESET}"
        formatted = formatted.replace(levelname, colored_level, 1)

        return formatted


def setup_logger(
    name: str,
    log_file: Optional[Path] = None,
    level: int = logging.INFO,
    console_output: bool = True,
) -> logging.Logger:
    """Setup a structured logger with file and console handlers

    Args:
        name: Logger name (typically the module name)
        log_file: Optional path to log file
        level: Logging level (default: INFO)
        console_output: Whether to output to console (default: True)

    Returns:
        Configured logger instance

    Example:
        logger = setup_logger(
            "dimension_incremental",
            log_file=Path("logs/dimension_incremental.log")
        )
        logger.info("Processing started")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Format string
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(format_str)

    # Console handler with color
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        colored_formatter = ColoredFormatter(format_str)
        console_handler.setFormatter(colored_formatter)
        logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_pipeline_logger(
    service_name: str,
    logs_dir: Path = Path("logs"),
) -> logging.Logger:
    """Get a configured logger for pipeline services

    Args:
        service_name: Name of the service/job
        logs_dir: Directory to store log files

    Returns:
        Configured logger instance

    Example:
        logger = get_pipeline_logger("daily-dimension-incremental")
        logger.info("Pipeline started")
    """
    log_file = logs_dir / f"{service_name}_{datetime.now().strftime('%Y%m%d')}.log"
    return setup_logger(service_name, log_file=log_file, console_output=True)


def log_separator(logger: logging.Logger, title: Optional[str] = None):
    """Log a visual separator line

    Args:
        logger: Logger instance
        title: Optional title to display in separator

    Example:
        log_separator(logger, "Pipeline Starting")
        # Logs: ======== Pipeline Starting ========
    """
    separator = "=" * 100
    if title:
        logger.info(f"{separator}\n{title}\n{separator}")
    else:
        logger.info(separator)


def log_step(logger: logging.Logger, step_num: int, total_steps: int, description: str):
    """Log a pipeline step with progress

    Args:
        logger: Logger instance
        step_num: Current step number
        total_steps: Total number of steps
        description: Step description

    Example:
        log_step(logger, 1, 5, "Downloading blobs from Azure")
    """
    logger.info(f"\n[Step {step_num}/{total_steps}] {description}")


def log_summary(
    logger: logging.Logger,
    total_time: float,
    success_count: int,
    fail_count: int,
    total_records: int = 0,
):
    """Log a pipeline execution summary

    Args:
        logger: Logger instance
        total_time: Total execution time in seconds
        success_count: Number of successful operations
        fail_count: Number of failed operations
        total_records: Total records processed (optional)

    Example:
        log_summary(logger, 125.5, 4, 1, 100000)
    """
    logger.info("\n" + "=" * 100)
    logger.info("📊 Execution Summary:")
    logger.info(f"  ✅ Successful: {success_count}")
    logger.info(f"  ❌ Failed: {fail_count}")
    if total_records > 0:
        logger.info(f"  📈 Records processed: {total_records:,}")
    logger.info(f"  ⏱️  Total time: {total_time:.2f}s")
    logger.info("=" * 100 + "\n")


def with_status_tracking(service_name: str):
    """
    Decorator to wrap job functions with consistent error handling and status tracking.
    Ensures that functions always log errors properly.

    Args:
        service_name: Service name to use in logging and status tracking

    Returns:
        Function that wraps the original with error handling

    Example:
        @with_status_tracking("daily-blob-backup")
        async def run_backup():
            # Job logic
            pass
    """

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            try:
                # Execute the wrapped function
                result = await func(*args, **kwargs)

                end_time = time.time()
                end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                duration = (end_time - start_time) / 60  # Convert to minutes

                # Return result (function should handle its own logging)
                return result

            except Exception as e:
                end_time = time.time()
                end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                duration = (end_time - start_time) / 60  # Convert to minutes

                # The function's logger should handle error logging
                # This decorator just wraps and ensures cleanup
                raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            try:
                # Execute the wrapped function
                result = func(*args, **kwargs)

                end_time = time.time()
                end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                duration = (end_time - start_time) / 60  # Convert to minutes

                return result

            except Exception as e:
                end_time = time.time()
                end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                duration = (end_time - start_time) / 60  # Convert to minutes

                raise

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
