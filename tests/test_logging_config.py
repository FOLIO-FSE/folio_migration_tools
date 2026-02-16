import logging

from rich.logging import RichHandler

from folio_migration_tools.logging_config import (
    ExcludeLevelFilter,
    IncludeLevelFilter,
    TaskNameFilter,
    get_logger,
    setup_logging,
)


def test_level_filters_include_and_exclude():
    record = logging.LogRecord(
        "folio_migration_tools.test", logging.INFO, __file__, 0, "msg", args=(), exc_info=None
    )
    exclude_filter = ExcludeLevelFilter(logging.INFO)
    include_filter = IncludeLevelFilter(logging.INFO)

    assert exclude_filter.filter(record) is False
    assert include_filter.filter(record) is True

    other_record = logging.LogRecord(
        "folio_migration_tools.test", logging.WARNING, __file__, 0, "msg", args=(), exc_info=None
    )
    assert exclude_filter.filter(other_record) is True
    assert include_filter.filter(other_record) is False


def test_task_name_filter_sets_attribute():
    record = logging.LogRecord(
        "folio_migration_tools.test", logging.INFO, __file__, 0, "msg", args=(), exc_info=None
    )
    task_filter = TaskNameFilter("ExampleTask")

    assert task_filter.filter(record) is True
    assert record.task_configuration_name == "ExampleTask"


def test_setup_logging_separates_data_issues(tmp_path):
    general_log = tmp_path / "general.log"
    data_issues_log = tmp_path / "data_issues.log"

    logger = setup_logging(
        debug=True,
        log_file=general_log,
        data_issues_file=data_issues_log,
        task_name="ExampleTask",
    )

    module_logger = get_logger("folio_migration_tools.sample")
    module_logger.debug("debug message")
    module_logger.info("info message")
    module_logger.data_issues("data issue occurred")

    for handler in logger.handlers:
        if hasattr(handler, "flush"):
            handler.flush()

    general_content = general_log.read_text()
    data_issue_content = data_issues_log.read_text()

    assert "debug message" in general_content
    assert "info message" in general_content
    assert "data issue occurred" not in general_content
    assert "data issue occurred" in data_issue_content

    # Handlers are attached to the root logger for third-party library compatibility
    root_logger = logging.getLogger()
    rich_handler = next(h for h in root_logger.handlers if isinstance(h, RichHandler))
    assert any(isinstance(f, ExcludeLevelFilter) for f in rich_handler.filters)

    file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
    assert any(isinstance(f, TaskNameFilter) for h in file_handlers for f in h.filters)


def test_get_logger_returns_namespaced_logger():
    setup_logging()
    module_logger = get_logger("folio_migration_tools.custom")
    module_logger.info("hello")
    assert module_logger.name == "folio_migration_tools.custom"


def test_data_issues_method_logs_at_custom_level(tmp_path):
    """Test that the data_issues method logs at the custom DATA_ISSUES level (26)."""
    data_issues_log = tmp_path / "data_issues.log"

    # Setup logging with data issues file
    setup_logging(
        debug=False,
        log_file=None,
        data_issues_file=data_issues_log,
        task_name="TestTask",
    )

    module_logger = get_logger("folio_migration_tools.data_test")
    module_logger.data_issues("Test data issue message")

    # Flush all handlers attached to the root logger
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if hasattr(handler, "flush"):
            handler.flush()

    data_issue_content = data_issues_log.read_text()
    assert "Test data issue message" in data_issue_content


def test_setup_logging_without_files():
    """Test setup_logging works without file handlers."""
    package_logger = setup_logging(debug=True)
    assert package_logger.name == "folio_migration_tools"
