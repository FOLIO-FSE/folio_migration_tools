import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.migration_tasks.instance_csv_transformer import (
    InstanceCsvTransformer,
)


def test_get_object_type():
    assert InstanceCsvTransformer.get_object_type() == FOLIONamespaces.instances


def test_id_map_entry_without_hrid():
    assert InstanceCsvTransformer.get_id_map_entry("l1", {"id": "u1"}) == ("l1", "u1")


def test_id_map_entry_with_hrid():
    entry = InstanceCsvTransformer.get_id_map_entry("l1", {"id": "u1", "hrid": "in1"})
    assert entry == ("l1", "u1", "in1")


def test_handle_notes_removes_empty_notes():
    rec = {
        "notes": [
            {"instanceNoteTypeId": "t1", "note": "keep"},
            {"instanceNoteTypeId": "t1", "note": ""},
        ]
    }
    InstanceCsvTransformer.handle_notes(rec)
    assert rec["notes"] == [{"instanceNoteTypeId": "t1", "note": "keep"}]


def test_handle_notes_deletes_when_all_empty():
    rec = {"notes": [{"instanceNoteTypeId": "t1", "note": ""}]}
    InstanceCsvTransformer.handle_notes(rec)
    assert "notes" not in rec


def test_handle_notes_missing_type_fails():
    with pytest.raises(TransformationProcessError):
        InstanceCsvTransformer.handle_notes({"notes": [{"note": "text"}]})
