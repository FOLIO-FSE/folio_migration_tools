"""Tests for the ExtradataWriter module."""

import json
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.extradata_writer import ExtradataWriter


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the ExtradataWriter singleton before each test."""
    ExtradataWriter._ExtradataWriter__instance = None
    ExtradataWriter._ExtradataWriter__inited = False
    yield
    # Cleanup after test
    ExtradataWriter._ExtradataWriter__instance = None
    ExtradataWriter._ExtradataWriter__inited = False


def test_write_basic(tmp_path):
    """Test basic write operation to extradata file."""
    extradata_file = tmp_path / "test.extradata"
    writer = ExtradataWriter(extradata_file)

    data = {"id": "123", "field": "value"}
    writer.write("test_type", data)
    writer.flush()

    assert extradata_file.exists()
    content = extradata_file.read_text()
    assert "test_type" in content
    assert "123" in content


def test_write_with_flush_on_cache_limit(tmp_path, caplog):
    """Test that write flushes when cache exceeds limit."""
    extradata_file = tmp_path / "test_cache_limit.extradata"

    writer = ExtradataWriter(extradata_file)
    # Force inject a lot of records into the cache to simulate a full batch
    writer.cache = [f"test_type\t{{\"id\": \"{i}\"}}\n" for i in range(1001)]

    # Now write one more which should trigger the flush
    writer.write("test_type", {"id": "final"})

    # Check that flush was triggered - file exists and cache is cleared
    assert extradata_file.exists()
    assert len(writer.cache) == 0  # Cache should be empty after flush
    assert extradata_file.stat().st_size > 0


def test_write_exception_logs_error(tmp_path, caplog):
    """Test that write logs an error when exception occurs."""
    extradata_file = tmp_path / "test.extradata"
    writer = ExtradataWriter(extradata_file)

    # Mock the open function to raise an exception during write
    with caplog.at_level("ERROR"):
        with patch("builtins.open", side_effect=IOError("Disk full")):
            with pytest.raises(TransformationProcessError):
                # Force a flush by writing with flush=True
                writer.write("test_type", {"id": "123"}, flush=True)

    assert "Something went wrong in extradata Writer" in caplog.text


def test_flush_removes_empty_file(tmp_path, caplog):
    """Test that flush removes empty extradata file and logs the action."""
    extradata_file = tmp_path / "test.extradata"
    # Create an empty file
    extradata_file.touch()

    writer = ExtradataWriter(extradata_file)

    with caplog.at_level("INFO"):
        writer.flush()

    assert "Removing extradata file since it is empty" in caplog.text
    assert not extradata_file.exists()


def test_flush_keeps_non_empty_file(tmp_path):
    """Test that flush keeps non-empty extradata file."""
    extradata_file = tmp_path / "test.extradata"
    writer = ExtradataWriter(extradata_file)

    writer.write("test_type", {"id": "123"})
    writer.flush()

    assert extradata_file.exists()
    assert extradata_file.stat().st_size > 0
