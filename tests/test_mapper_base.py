from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.mapper_base import MapperBase


def test_validate_required_properties():
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "",
        "type": "object",
        "required": ["d", "h", "i"],
        "properties": {},
    }
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
        MapperBase.validate_required_properties("", record, schema, FOLIONamespaces.other)


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
    clean_record = MapperBase.validate_required_properties(
        "", record, schema, FOLIONamespaces.other
    )
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


def test_add_legacy_identifier_to_admin_note():
    folio_record = {}
    legacy_id = "legacy_ID"
    mapper = Mock(spec=MapperBase)
    MapperBase.add_legacy_id_to_admin_note(mapper, folio_record, legacy_id)
    assert f"{MapperBase.legacy_id_template} {legacy_id}" in folio_record["administrativeNotes"]


def test_add_legacy_identifier_to_admin_note_additional_id():
    folio_record = {"administrativeNotes": [f"{MapperBase.legacy_id_template} legacy_ID"]}
    legacy_id = "legacy_ID_2"
    mapper = Mock(spec=MapperBase)
    MapperBase.add_legacy_id_to_admin_note(mapper, folio_record, legacy_id)
    assert len(folio_record["administrativeNotes"]) == 1
    assert (
        f"{MapperBase.legacy_id_template} legacy_ID, legacy_ID_2"
        in folio_record["administrativeNotes"]
    )


def test_add_legacy_identifier_to_admin_note_dupe():
    folio_record = {"administrativeNotes": [f"{MapperBase.legacy_id_template} legacy_ID"]}
    legacy_id = "legacy_ID"
    mapper = Mock(spec=MapperBase)
    MapperBase.add_legacy_id_to_admin_note(mapper, folio_record, legacy_id)
    assert f"{MapperBase.legacy_id_template} legacy_ID" in folio_record["administrativeNotes"]
