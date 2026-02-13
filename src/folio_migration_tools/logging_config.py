"""Logging configuration for folio_migration_tools.

This module provides centralized logging configuration for the package.
It sets up a package-level logger with RichHandler for console output
that properly coordinates with Rich progress bars.

Usage:
    from folio_migration_tools.logging_config import setup_logging

    # In __main__.py or entry point:
    setup_logging(debug=False)
"""

import logging
from pathlib import Path
from typing import Optional

from rich.logging import RichHandler

# Package-level logger name
PACKAGE_LOGGER_NAME = "folio_migration_tools"

# Custom log level for data issues
DATA_ISSUE_LVL_NUM = 26
logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")


class ExcludeLevelFilter(logging.Filter):
    """Filter that excludes log records at a specific level."""

    def __init__(self, level: int) -> None:
        """Initialize the filter.

        Args:
            level: The log level to exclude.
        """
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        """Filter out records at the specified level.

        Args:
            record: The log record to filter.

        Returns:
            True if the record should be logged, False otherwise.
        """
        return record.levelno != self.level


class IncludeLevelFilter(logging.Filter):
    """Filter that includes only log records at a specific level."""

    def __init__(self, level: int) -> None:
        """Initialize the filter.

        Args:
            level: The log level to include.
        """
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        """Include only records at the specified level.

        Args:
            record: The log record to filter.

        Returns:
            True if the record should be logged, False otherwise.
        """
        return record.levelno == self.level


class TaskNameFilter(logging.Filter):
    """Filter that adds task_configuration_name to log records."""

    def __init__(self, task_name: str) -> None:
        """Initialize the filter.

        Args:
            task_name: The task name to add to records.
        """
        super().__init__()
        self.task_name = task_name

    def filter(self, record: logging.LogRecord) -> bool:
        """Add task name to record and allow it through.

        Args:
            record: The log record to filter.

        Returns:
            Always True (allows all records through).
        """
        record.task_configuration_name = self.task_name
        return True


def setup_logging(
    debug: bool = False,
    log_file: Optional[Path] = None,
    data_issues_file: Optional[Path] = None,
    task_name: Optional[str] = None,
) -> logging.Logger:
    """Set up logging for the folio_migration_tools package.

    Configures a package-level logger with RichHandler for console output
    that coordinates properly with Rich progress bars and attaches the same
    handlers to the root logger so third-party libraries (e.g., folio_data_import)
    also emit through them. Optionally sets up file handlers for persistent logging.

    Args:
        debug: Enable debug-level logging.
        log_file: Path to write general log output.
        data_issues_file: Path to write data issues (level 26) output.
        task_name: Task name to include in log records.

    Returns:
        The configured package logger.
    """
    # Get or create the package logger
    package_logger = logging.getLogger(PACKAGE_LOGGER_NAME)

    # Clear any existing handlers to avoid duplicates on re-initialization
    package_logger.handlers.clear()

    # Set level and propagate so records reach the root handlers we configure below
    package_logger.setLevel(logging.DEBUG if debug else logging.INFO)
    package_logger.propagate = True

    # Console handler using RichHandler for proper progress bar coordination
    console_handler = RichHandler(
        show_level=False,
        show_time=False,
        omit_repeated_times=False,
        show_path=False,
        rich_tracebacks=True,
    )
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    # Exclude DATA_ISSUES level from console (goes to separate file)
    console_handler.addFilter(ExcludeLevelFilter(DATA_ISSUE_LVL_NUM))
    if task_name:
        console_handler.addFilter(TaskNameFilter(task_name))
    handlers = [console_handler]

    # File handler for general logs (if path provided)
    if log_file:
        file_formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s")
        file_handler = logging.FileHandler(filename=log_file, mode="w")
        file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        file_handler.setFormatter(file_formatter)
        file_handler.addFilter(ExcludeLevelFilter(DATA_ISSUE_LVL_NUM))
        if task_name:
            file_handler.addFilter(TaskNameFilter(task_name))
        handlers.append(file_handler)

    # Separate file handler for data issues (if path provided)
    if data_issues_file:
        data_issues_handler = logging.FileHandler(filename=data_issues_file, mode="w")
        data_issues_handler.setLevel(DATA_ISSUE_LVL_NUM)
        data_issues_handler.addFilter(IncludeLevelFilter(DATA_ISSUE_LVL_NUM))
        data_issues_handler.setFormatter(logging.Formatter("%(message)s"))
        handlers.append(data_issues_handler)

    # Attach handlers to the root logger so third-party module loggers (e.g., folio_data_import)
    # also emit through the same Rich/file handlers.
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.DEBUG if debug else logging.INFO)
    for handler in handlers:
        root_logger.addHandler(handler)

    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("pymarc").setLevel(logging.WARNING)

    return package_logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger for a module within the package.

    This is a convenience function that ensures module loggers
    are properly namespaced under the package logger.

    Args:
        name: The module's __name__.

    Returns:
        A logger for the module.
    """
    return logging.getLogger(name)


# Add data_issues method to Logger class
def _data_issues(self, message: str, *args, **kwargs) -> None:
    """Log a data issue at the custom DATA_ISSUES level (26).

    Args:
        self: The logger instance.
        message: The message to log.
        *args: Arguments to format into the message.
        **kwargs: Keyword arguments for the logging call.
    """
    if self.isEnabledFor(DATA_ISSUE_LVL_NUM):
        self._log(DATA_ISSUE_LVL_NUM, message, args, **kwargs)


# Monkey-patch the data_issues method onto Logger
logging.Logger.data_issues = _data_issues
