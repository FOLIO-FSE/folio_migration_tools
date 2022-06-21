import uuid

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer


def test_handle_circiulation_notes_wrong_type():
    folio_rec = {
        "circulationNotes": [{"id": "someId", "noteType": "Check inn", "note": "some note"}]
    }
    with pytest.raises(TransformationProcessError):
        ItemsTransformer.handle_circiulation_notes(folio_rec, str(uuid.uuid4()))


def test_handle_circiulation_notes_no_note():
    folio_rec = {"circulationNotes": [{"id": "someId", "noteType": "Check in", "note": ""}]}
    ItemsTransformer.handle_circiulation_notes(folio_rec, str(uuid.uuid4()))
    assert "circulationNotes" not in folio_rec


def test_handle_circiulation_notes_happy_path():
    folio_rec = {
        "circulationNotes": [
            {"id": "someId", "noteType": "Check in", "note": "some_note"},
            {"id": "someId", "noteType": "Check in", "note": ""},
        ]
    }
    ItemsTransformer.handle_circiulation_notes(folio_rec, str(uuid.uuid4()))
    assert folio_rec["circulationNotes"][0]["note"] == "some_note"
    assert len(folio_rec["circulationNotes"]) == 1


def test_get_object_type():
    assert ItemsTransformer.get_object_type() == FOLIONamespaces.items
