import pytest
from migration_tools.custom_exceptions import TransformationRecordFailedError
from migration_tools.mapper_base import MapperBase


def test_validate_required_properties():

    schema = {"required": ["d", "h", "i"]}
    record = {
        "a": None,
        "b": [],
        "c": "",
        "d": {"e": None, "f": "aaa"},
        "g": {},
        "h": "actual value",
        "type": "object",
    }
    with pytest.raises(TransformationRecordFailedError):
        MapperBase.validate_required_properties("", record, schema)


def test_validate_required_properties_remove_object():
    schema = {"required": ["d", "h"]}
    record = {
        "a": None,
        "b": [],
        "c": "",
        "d": {"e": None, "f": "aaa"},
        "g": {},
        "h": "actual value",
        "type": "object",
    }
    clean_record = MapperBase.validate_required_properties("", record, schema)
    assert "type" not in clean_record


def test_clean_none_props():
    record = {
        "a": None,
        "b": [],
        "c": "",
        "d": {"e": None, "f": "aaa"},
        "g": {},
        "h": "actual value",
        "i": ["", "j"],
        "k": {"l": "", "m": "aaa"},
    }
    cleaned = MapperBase.clean_none_props(record)
    for ck in ["a"]:
        assert ck not in cleaned.keys()
    assert "e" not in cleaned["d"]
    assert len(cleaned["i"]) == 1
    assert next(iter(cleaned["i"])) == "j"
    assert "l" in cleaned["k"]
