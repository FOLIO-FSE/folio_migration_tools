import logging

from folio_migration_tools.helper import Helper

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_log_dats_issue(caplog):
    caplog.set_level(26)
    Helper.log_data_issue("test id", "test log message", "legacy value")
    assert "test log message" in caplog.text
    assert "test id" in caplog.text
    assert "legacy value" in caplog.text
