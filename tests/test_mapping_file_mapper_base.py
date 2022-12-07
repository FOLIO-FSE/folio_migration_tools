import csv
import functools
import io
from pathlib import Path
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer
from folio_migration_tools.test_infrastructure import mocked_classes


@pytest.fixture(scope="session", autouse=True)
def mocked_folio_client(pytestconfig):
    return mocked_classes.mocked_folio_client()


# flake8: noqa
class MyTestableFileMapper(MappingFileMapperBase):
    def __init__(self, schema: dict, record_map: dict, mocked_folio_client):
        mock_conf = Mock(spec=LibraryConfiguration)
        mock_conf.multi_field_delimiter = "<delimiter>"
        super().__init__(
            mocked_folio_client,
            schema,
            record_map,
            None,
            FOLIONamespaces.holdings,
            mock_conf,
        )

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
        if len(legacy_item_keys) == 1 and folio_prop_name in self.mapped_from_values:
            return self.mapped_from_values.get(folio_prop_name, "")
        legacy_values = MappingFileMapperBase.get_legacy_vals(legacy_item, legacy_item_keys)
        return " ".join(legacy_values).strip()


def test_validate_required_properties_sub_pro_missing_uri(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": ["title"],
        "properties": {
            "formerIds": {
                "type": "array",
                "description": "Previous ID(s) assigned to the holdings record",
                "items": {"type": "string"},
                "uniqueItems": True,
            },
            "title": {
                "type": "string",
                "description": "",
            },
            "subtitle": {
                "type": "string",
                "description": "",
            },
            "electronicAccess": {
                "description": "List of electronic access items",
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "uri": {
                            "type": "string",
                            "description": "uniform resource identifier (URI) is a string of characters designed for unambiguous identification of resources",
                        },
                        "relationshipId": {
                            "type": "string",
                            "description": "relationship between the electronic resource at the location identified and the item described in the record as a whole",
                        },
                    },
                    "additionalProperties": False,
                    "required": ["uri"],
                },
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "title",
                "legacy_field": "title_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "subtitle",
                "legacy_field": "subtitle_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[0]",
                "legacy_field": "formerIds_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[1]",
                "legacy_field": "formerIds_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].relationshipId",
                "legacy_field": "",
                "value": "f5d0068e-6272-458e-8a81-b85e7b9a14aa",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].uri",
                "legacy_field": "link_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[1].relationshipId",
                "legacy_field": "",
                "value": "f5d0068e-000-458e-8a81-b85e7b9a14aa",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[1].uri",
                "legacy_field": "link_2",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {
        "link_": "some_link",
        "formerIds_1": "id1",
        "formerIds_2": "id2",
        "title_": "actual value",
        "subtitle_": "object",
        "link_2": "",
        "id": "11",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 1
    assert folio_id == "11"
    assert folio_rec["id"] == "f00d59ac-4cfc-56d6-9c62-dc9084c18003"


def test_validate_required_properties_sub_pro_missing_uri_and_more(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": ["title"],
        "properties": {
            "formerIds": {
                "type": "array",
                "description": "Previous ID(s) assigned to the holdings record",
                "items": {"type": "string"},
                "uniqueItems": True,
            },
            "title": {
                "type": "string",
                "description": "",
            },
            "subtitle": {
                "type": "string",
                "description": "",
            },
            "electronicAccess": {
                "description": "List of electronic access items",
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "uri": {
                            "type": "string",
                            "description": "uniform resource identifier (URI) is a string of characters designed for unambiguous identification of resources",
                        },
                        "relationshipId": {
                            "type": "string",
                            "description": "relationship between the electronic resource at the location identified and the item described in the record as a whole",
                        },
                        "third_prop": {
                            "type": "string",
                            "description": "relationship between the electronic resource at the location identified and the item described in the record as a whole",
                        },
                    },
                    "additionalProperties": False,
                    "required": ["uri"],
                },
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "title",
                "legacy_field": "title_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "subtitle",
                "legacy_field": "subtitle_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[0]",
                "legacy_field": "formerIds_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[1]",
                "legacy_field": "formerIds_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].relationshipId",
                "legacy_field": "",
                "value": "f5d0068e-6272-458e-8a81-b85e7b9a14aa",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].third_prop",
                "legacy_field": "third_0",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].uri",
                "legacy_field": "link_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[1].relationshipId",
                "legacy_field": "",
                "value": "f5d0068e-000-458e-8a81-b85e7b9a14aa",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[1].uri",
                "legacy_field": "link_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[1].third_prop",
                "legacy_field": "third_",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {
        "link_": "some_link",
        "formerIds_1": "id1",
        "formerIds_2": "id2",
        "title_": "actual value",
        "subtitle_": "object",
        "link_2": "",
        "id": "11",
        "third_0": "",
        "third_1": "",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 1


def test_validate_required_properties_item_notes(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "notes": {
                "type": "array",
                "description": "Notes about action, copy, binding etc.",
                "items": {
                    "type": "object",
                    "properties": {
                        "itemNoteTypeId": {
                            "type": "string",
                            "description": "ID of the type of note",
                        },
                        "itemNoteType": {
                            "description": "Type of item's note",
                            "type": "object",
                            "folio:$ref": "itemnotetype.json",
                            "javaType": "org.folio.rest.jaxrs.model.itemNoteTypeVirtual",
                            "readonly": True,
                            "folio:isVirtual": True,
                            "folio:linkBase": "item-note-types",
                            "folio:linkFromField": "itemNoteTypeId",
                            "folio:linkToField": "id",
                            "folio:includedElement": "itemNoteTypes.0",
                        },
                        "note": {
                            "type": "string",
                            "description": "Text content of the note",
                        },
                        "staffOnly": {
                            "type": "boolean",
                            "description": "If true, determines that the note should not be visible for others than staff",
                            "default": False,
                        },
                    },
                },
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "notes[0].note",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].staffOnly",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "notes[0].itemNoteTypeId",
                "legacy_field": "",
                "value": "A UUID",
                "description": "",
            },
            {
                "folio_field": "notes[1].note",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[1].staffOnly",
                "legacy_field": "",
                "value": False,
                "description": "",
            },
            {
                "folio_field": "notes[1].itemNoteTypeId",
                "legacy_field": "",
                "value": "Another UUID",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"note_1": "my note", "note_2": "", "id": "12"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_item_notes_unmapped(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "notes": {
                "type": "array",
                "description": "Notes about action, copy, binding etc.",
                "items": {
                    "type": "object",
                    "properties": {
                        "itemNoteTypeId": {
                            "type": "string",
                            "description": "ID of the type of note",
                        },
                        "itemNoteType": {
                            "description": "Type of item's note",
                            "type": "object",
                            "folio:$ref": "itemnotetype.json",
                            "javaType": "org.folio.rest.jaxrs.model.itemNoteTypeVirtual",
                            "readonly": True,
                            "folio:isVirtual": True,
                            "folio:linkBase": "item-note-types",
                            "folio:linkFromField": "itemNoteTypeId",
                            "folio:linkToField": "id",
                            "folio:includedElement": "itemNoteTypes.0",
                        },
                        "note": {
                            "type": "string",
                            "description": "Text content of the note",
                        },
                        "staffOnly": {
                            "type": "boolean",
                            "description": "If true, determines that the note should not be visible for others than staff",
                            "default": False,
                        },
                    },
                },
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "notes[0].note",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].staffOnly",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "notes[0].itemNoteTypeId",
                "legacy_field": "",
                "value": "A UUID",
                "description": "",
            },
            {
                "folio_field": "notes[1].note",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[1].staffOnly",
                "legacy_field": "",
                "value": False,
                "description": "",
            },
            {
                "folio_field": "notes[1].itemNoteTypeId",
                "legacy_field": "",
                "value": "UUID",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"note_1": "my note", "id": "12"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_item_notes_unmapped_2(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "notes": {
                "type": "array",
                "description": "Notes about action, copy, binding etc.",
                "items": {
                    "type": "object",
                    "properties": {
                        "itemNoteTypeId": {
                            "type": "string",
                            "description": "ID of the type of note",
                        },
                        "itemNoteType": {
                            "description": "Type of item's note",
                            "type": "object",
                            "folio:$ref": "itemnotetype.json",
                            "javaType": "org.folio.rest.jaxrs.model.itemNoteTypeVirtual",
                            "readonly": True,
                            "folio:isVirtual": True,
                            "folio:linkBase": "item-note-types",
                            "folio:linkFromField": "itemNoteTypeId",
                            "folio:linkToField": "id",
                            "folio:includedElement": "itemNoteTypes.0",
                        },
                        "note": {
                            "type": "string",
                            "description": "Text content of the note",
                        },
                        "staffOnly": {
                            "type": "boolean",
                            "description": "If true, determines that the note should not be visible for others than staff",
                            "default": False,
                        },
                    },
                },
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "notes[0].note",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].staffOnly",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "notes[0].itemNoteTypeId",
                "legacy_field": "",
                "value": "A UUID",
                "description": "",
            },
            {
                "folio_field": "notes[1].note",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[1].staffOnly",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[1].itemNoteTypeId",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"note_1": "my note", "id": "12"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_obj(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": ["title"],
        "properties": {
            "formerIds": {
                "type": "array",
                "description": "Previous ID(s) assigned to the holdings record",
                "items": {"type": "string"},
                "uniqueItems": True,
            },
            "title": {
                "type": "string",
                "description": "",
            },
            "subtitle": {
                "type": "string",
                "description": "",
            },
            "electronicAccessObj": {
                "type": "object",
                "properties": {
                    "uri": {
                        "type": "string",
                        "description": "uniform resource identifier (URI) is a string of characters designed for unambiguous identification of resources",
                    },
                    "relationshipId": {
                        "type": "string",
                        "description": "relationship between the electronic resource at the location identified and the item described in the record as a whole",
                    },
                    "third_prop": {
                        "type": "string",
                        "description": "relationship between the electronic resource at the location identified and the item described in the record as a whole",
                    },
                },
                "additionalProperties": False,
                "required": ["uri"],
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "title",
                "legacy_field": "title_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "subtitle",
                "legacy_field": "subtitle_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[0]",
                "legacy_field": "formerIds_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[1]",
                "legacy_field": "formerIds_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccessObj.relationshipId",
                "legacy_field": "",
                "value": "f5d0068e-6272-458e-8a81-b85e7b9a14aa",
                "description": "",
            },
            {
                "folio_field": "electronicAccessObj.third_prop",
                "legacy_field": "third_0",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccessObj.uri",
                "legacy_field": "link_",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {
        "link_": "some_link",
        "formerIds_1": "id1",
        "formerIds_2": "id2",
        "title_": "actual value",
        "subtitle_": "object",
        "id": "11",
        "third_0": "",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["electronicAccessObj"]["uri"] == "some_link"


def test_validate_required_properties_item_notes_split_on_delimiter_notes(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "notes": {
                "type": "array",
                "description": "Notes about action, copy, binding etc.",
                "items": {
                    "type": "object",
                    "properties": {
                        "itemNoteTypeId": {
                            "type": "string",
                            "description": "ID of the type of note",
                        },
                        "itemNoteType": {
                            "description": "Type of item's note",
                            "type": "object",
                            "folio:$ref": "itemnotetype.json",
                            "javaType": "org.folio.rest.jaxrs.model.itemNoteTypeVirtual",
                            "readonly": True,
                            "folio:isVirtual": True,
                            "folio:linkBase": "item-note-types",
                            "folio:linkFromField": "itemNoteTypeId",
                            "folio:linkToField": "id",
                            "folio:includedElement": "itemNoteTypes.0",
                        },
                        "note": {
                            "type": "string",
                            "description": "Text content of the note",
                        },
                        "staffOnly": {
                            "type": "boolean",
                            "description": "If true, determines that the note should not be visible for others than staff",
                            "default": False,
                        },
                    },
                },
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "notes[0].note",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].staffOnly",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "notes[0].itemNoteTypeId",
                "legacy_field": "",
                "value": "A UUID",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"note_1": "my note<delimiter>my second note", "id": "12"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 2
    assert folio_rec["notes"][0]["note"] == "my note"
    assert folio_rec["notes"][0]["staffOnly"] == True
    assert folio_rec["notes"][0]["itemNoteTypeId"] == "A UUID"
    assert "hrid" not in folio_rec

    assert folio_rec["notes"][1]["note"] == "my second note"
    assert folio_rec["notes"][1]["staffOnly"] == True
    assert folio_rec["notes"][1]["itemNoteTypeId"] == "A UUID"


def test_multiple_repeated_split_on_delimiter_electronic_access(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": ["electronicAccess"],
        "properties": {
            "electronicAccess": {
                "description": "List of electronic access items",
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "uri": {
                            "type": "string",
                            "description": "uniform resource identifier (URI) is a string of characters designed for unambiguous identification of resources",
                        },
                        "relationshipId": {
                            "type": "string",
                            "description": "relationship between the electronic resource at the location identified and the item described in the record as a whole",
                        },
                        "linkText": {
                            "type": "string",
                            "description": "the value of the MARC tag field 856 2nd indicator, where the values are: no information provided, resource, version of resource, related resource, no display constant generated",
                        },
                    },
                    "additionalProperties": False,
                    "required": ["uri"],
                },
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].relationshipId",
                "legacy_field": "",
                "value": "f5d0068e-6272-458e-8a81-b85e7b9a14aa",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].linkText",
                "legacy_field": "title_",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].uri",
                "legacy_field": "link_",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {
        "link_": "uri1<delimiter>uri2",
        "title_": "title1<delimiter>title2",
        "subtitle_": "object",
        "id": "11",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 2
    assert folio_id == "11"
    assert folio_rec["id"] == "f00d59ac-4cfc-56d6-9c62-dc9084c18003"

    assert folio_rec["electronicAccess"][0]["uri"] == "uri1"
    assert folio_rec["electronicAccess"][0]["linkText"] == "title1"
    assert (
        folio_rec["electronicAccess"][0]["relationshipId"]
        == "f5d0068e-6272-458e-8a81-b85e7b9a14aa"
    )

    assert folio_rec["electronicAccess"][1]["uri"] == "uri2"
    assert folio_rec["electronicAccess"][1]["linkText"] == "title2"
    assert (
        folio_rec["electronicAccess"][1]["relationshipId"]
        == "f5d0068e-6272-458e-8a81-b85e7b9a14aa"
    )


def test_validate_required_properties_item_notes_split_on_delimiter_plain_object(
    mocked_folio_client,
):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "uber_prop": {
                "type": "object",
                "properties": {
                    "prop1": {
                        "description": "",
                        "type": "string",
                    },
                    "prop2": {
                        "description": "",
                        "type": "string",
                    },
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "uber_prop.prop1",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "uber_prop.prop2",
                "legacy_field": "",
                "value": "Some value",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"note_1": "my note<delimiter>my second note", "id": "12"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert folio_rec["uber_prop"]["prop1"] == "my note<delimiter>my second note"
    assert folio_rec["uber_prop"]["prop2"] == "Some value"


def test_concatenate_fields_if_mapped_multiple_times(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "uber_prop": {
                "type": "object",
                "properties": {
                    "prop1": {
                        "description": "",
                        "type": "string",
                    }
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "uber_prop.prop1",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "uber_prop.prop1",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"note_1": "my note", "note_2": "my second note", "id": "12"}
    # Loop to make sure the right order occurs the first time.
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    for _ in range(2000):
        folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
        assert folio_rec["uber_prop"]["prop1"] == "my note my second note"


def test_concatenate_fields_if_mapped_multiple_times_and_data_is_in_random_order(
    mocked_folio_client,
):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "uber_prop": {
                "type": "object",
                "properties": {
                    "prop1": {
                        "description": "",
                        "type": "string",
                    }
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "uber_prop.prop1",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "uber_prop.prop1",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"note_2": "my second note", "id": "12", "note_1": "my note"}
    # Loop to make sure the right order occurs the first time.
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    for _ in range(2000):
        folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
        assert folio_rec["uber_prop"]["prop1"] == "my note my second note"


def test_zip3(mocked_folio_client):
    o = {"p1": "a<delimiter>b", "p2": "c<delimiter>d<delimiter>e", "p3": "same for both"}
    d = "<delimiter>"
    l = ["p1", "p2"]
    s = MappingFileMapperBase.split_obj_by_delim(d, o, l)
    assert s[0] == {"p1": "a", "p2": "c", "p3": "same for both"}
    assert s[1] == {"p1": "b", "p2": "d", "p3": "same for both"}
    assert len(s) == 2


def test_do_not_split_string_prop(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "properties": {
            "formerId": {
                "type": "string",
                "description": "",
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "formerId",
                "legacy_field": "formerIds_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {
        "formerIds_1": "id2<delimiter>id3",
        "id": "11",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["formerId"] == "id2<delimiter>id3"


def test_split_former_ids(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": ["formerIds"],
        "properties": {
            "formerIds": {
                "type": "array",
                "description": "Previous ID(s) assigned to the holdings record",
                "items": {"type": "string"},
                "uniqueItems": True,
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "formerIds[0]",
                "legacy_field": "formerIds_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[1]",
                "legacy_field": "formerIds_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {
        "formerIds_1": "id1",
        "formerIds_2": "id2<delimiter>id3",
        "id": "11",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["formerIds"]) == 3
    assert "id1" in folio_rec["formerIds"]
    assert "id2" in folio_rec["formerIds"]
    assert "id3" in folio_rec["formerIds"]


def test_validate_no_leakage_between_properties(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "holdingsStatements": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "statement": {
                            "type": "string",
                        },
                        "note": {
                            "type": "string",
                        },
                        "staffNote": {
                            "type": "string",
                        },
                    },
                },
            },
            "holdingsStatementsForIndexes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "statement": {
                            "type": "string",
                        },
                        "note": {
                            "type": "string",
                        },
                        "staffNote": {
                            "type": "string",
                        },
                    },
                },
            },
            "holdingsStatementsForSupplements": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "statement": {
                            "type": "string",
                        },
                        "note": {
                            "type": "string",
                        },
                        "staffNote": {
                            "type": "string",
                        },
                    },
                },
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "holdingsStatements[0].statement",
                "legacy_field": "stmt_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForSupplements[0].statement",
                "legacy_field": "stmt_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForIndexes[0].statement",
                "legacy_field": "stmt_3",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"stmt_1": "stmt", "id": "12", "stmt_2": "suppl", "stmt_3": "idx"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["holdingsStatements"]) == 1
    assert folio_rec["holdingsStatements"][0]["statement"] == "stmt"
    assert len(folio_rec["holdingsStatementsForIndexes"]) == 1
    assert folio_rec["holdingsStatementsForIndexes"][0]["statement"] == "idx"
    assert len(folio_rec["holdingsStatementsForSupplements"]) == 1
    assert folio_rec["holdingsStatementsForSupplements"][0]["statement"] == "suppl"


delimited_data_tab = """\
header_1\theader_2\theader_3
\t\t
value_1\tvalue_2\tvalue_3
"""
delimited_data_comma = """\
header_1,header_2,header_3
,,
value_1,value_2,value_3
"""
delimited_file_tab = (Path("/tmp/delimited_data.tsv"), io.StringIO(delimited_data_tab))
delimited_file_comma = (Path("/tmp/delimited_data.csv"), io.StringIO(delimited_data_comma))


def test__get_delimited_file_reader():
    csv.register_dialect("tsv", delimiter="\t")
    for file in (delimited_file_tab, delimited_file_comma):
        total_rows, empty_rows, reader = MappingFileMapperBase._get_delimited_file_reader(
            file[1], file[0]
        )
        assert total_rows == 2 and empty_rows == 1
        for idx, row in enumerate(reader):
            if idx == 0:
                for key in row.keys():
                    assert row[key] == ""
            if idx == 1:
                assert (
                    row["header_1"] == "value_1"
                    and row["header_2"] == "value_2"
                    and row["header_3"] == "value_3"
                )


def test_map_string_first_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": ["title"],
        "properties": {
            "title": {
                "type": "string",
                "description": "",
            }
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "title",
                "legacy_field": "title_",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"title_": "actual value", "id": "id"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["title"] == "actual value"


def test_map_string_array_first_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "stringArray": {
                "type": "array",
                "description": "",
                "items": {"type": "string"},
                "uniqueItems": True,
            },
        },
    }
    fake_holdings_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "stringArray[0]",
                "legacy_field": "title_",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"title_": "actual value", "id": "id"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["stringArray"][0] == "actual value"


def test_map_string_second_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "secondLevel": {
                        "description": "",
                        "type": "string",
                    }
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.secondLevel",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note"}
    # Loop to make sure the right order occurs the first time.
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    for _ in range(2000):
        folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
        assert folio_rec["firstLevel"]["secondLevel"] == "my note"


def test_map_string_array_second_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "stringArray": {
                        "type": "array",
                        "description": "",
                        "items": {"type": "string"},
                        "uniqueItems": True,
                    },
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.stringArray[0]",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note"]


def test_map_string_array_second_level_multiple_values(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "stringArray": {
                        "type": "array",
                        "description": "",
                        "items": {"type": "string"},
                        "uniqueItems": True,
                    },
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.stringArray[0]",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.stringArray[1]",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note", "note_2": "my note 2"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note", "my note 2"]


def test_map_string_array_second_level_multiple_additional_split_values(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "stringArray": {
                        "type": "array",
                        "description": "",
                        "items": {"type": "string"},
                        "uniqueItems": True,
                    },
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.stringArray[0]",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.stringArray[1]",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note", "note_2": "my note 2<delimiter>my note 3"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note", "my note 2", "my note 3"]


def test_map_string_array_second_level_split_values(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "stringArray": {
                        "type": "array",
                        "description": "",
                        "items": {"type": "string"},
                        "uniqueItems": True,
                    },
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.stringArray[0]",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.stringArray[1]",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_2": "my note 2<delimiter>my note 3"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note 2", "my note 3"]


def test_map_array_of_objects_with_string_array(mocked_folio_client):
    schema = {
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "secondLevel": {
                            "type": "array",
                            "items": {"type": "string"},
                        }
                    },
                    "additionalProperties": False,
                },
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel[0].secondLevel[0]",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel[0].secondLevel[1]",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note", "note_2": "my note 2"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"][0]["secondLevel"] == ["my note", "my note 2"]


def test_map_array_of_objects_with_string_array_delimiter(mocked_folio_client):
    schema = {
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "secondLevel": {
                            "type": "array",
                            "items": {"type": "string"},
                        }
                    },
                    "additionalProperties": False,
                },
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel[0].secondLevel[0]",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel[0].secondLevel[1]",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note", "note_2": "my note 2<delimiter>my note 3"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"][0]["secondLevel"] == ["my note", "my note 2", "my note 3"]


def test_map_string_third_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "secondLevel": {
                        "type": "object",
                        "properties": {
                            "thirdLevel": {
                                "type": "string",
                            }
                        },
                    }
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert (
        folio_rec["firstLevel"]["secondLevel"]["thirdLevel"] == "my note"
    )  # No mapping on third level yet...


def test_map_string_and_array_of_strings_fourth_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "secondLevel": {
                        "type": "object",
                        "properties": {
                            "thirdLevel": {
                                "type": "object",
                                "properties": {
                                    "fourthLevel": {
                                        "type": "string",
                                    },
                                    "fourthLevelArr": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                            }
                        },
                    }
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevel",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevelArr[0]",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevelArr[1]",
                "legacy_field": "note_3",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note", "note_2": "my note 2", "note_3": "my note 3"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["secondLevel"]["thirdLevel"]["fourthLevel"] == "my note"
    assert folio_rec["firstLevel"]["secondLevel"]["thirdLevel"]["fourthLevelArr"] == [
        "my note 2",
        "my note 3",
    ]  # No mapping on third level yet...


def test_map_object_and_array_of_strings_fourth_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "object",
                "properties": {
                    "secondLevel": {
                        "type": "object",
                        "properties": {
                            "thirdLevel": {
                                "type": "object",
                                "properties": {
                                    "fourthLevel": {
                                        "type": "object",
                                        "properties": {
                                            "fifthLevel1": {
                                                "type": "string",
                                            },
                                            "fifthLevel2": {
                                                "type": "string",
                                            },
                                        },
                                    },
                                    "fourthLevelArr": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                            }
                        },
                    }
                },
                "additionalProperties": False,
            },
        },
    }
    fake_item_map = {
        "data": [
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevel.fifthLevel2",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevel.fifthLevel1",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevel",
                "legacy_field": "note_1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevelArr[0]",
                "legacy_field": "note_2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "firstLevel.secondLevel.thirdLevel.fourthLevelArr[1]",
                "legacy_field": "note_3",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record = {"id": "12", "note_1": "my note", "note_2": "my note 2", "note_3": "my note 3"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert (
        folio_rec["firstLevel"]["secondLevel"]["thirdLevel"]["fourthLevel"]["fifthLevel1"]
        == "my note"
    )
    assert folio_rec["firstLevel"]["secondLevel"]["thirdLevel"]["fourthLevelArr"] == [
        "my note 2",
        "my note 3",
    ]  # No mapping on third level yet...


# TODO Make this run successfully.
def test_map_array_object_array_object_string(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "contacts": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "addresses": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "addressLine1": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    record = {
        "id": "id1",
        "contact_person": "Jane",
        "contact_address_line1": "My Street",
        "contact_address_town": "Gothenburg",
        "contact_address_types": "support<delimiter>sales"
    }
    org_map = {
        "data": [
            {
                "folio_field": "contacts[0].firstName",
                "legacy_field": "contact_person",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].addresses[0].addressLine1",
                "legacy_field": "contact_address_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].addresses[0].categories",
                "legacy_field": "contact_address_types",
                "value": "",
                "description": ""
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            }
        ]
    }

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["contacts"][0]["addresses"][0]["addressLine1"] == "My Street"

# TODO Make this run successfully.
def test_map_array_object_array_object_array_string(mocked_folio_client):
    schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "The record of an organization",
    "type": "object",
    "properties": {
        "contacts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "addresses": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "categories": {
                                    "type": "array",
                                    "items": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
    record = {
        "id": "id1",
        "contact_person": "Jane",
        "contact_address_line1": "My Street",
        "contact_address_types": "support<delimiter>sales",
    }
    org_map = {
        "data": [
            {
                "folio_field": "contacts[0].firstName",
                "legacy_field": "contact_person",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].addresses[0].addressLine1",
                "legacy_field": "contact_address_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].addresses[0].categories",
                "legacy_field": "contact_address_types",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            }
        ]
    }

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)
    
    assert folio_rec["contacts"][0]["addresses"][0]["categories"] == ["support", "sales"]


def test_set_default(mocked_folio_client):
    d1 = {"level1": {"level2": {}}}
    d1["level1"]["level2"] = {"apa": 1}
    d1["level1"]["level2"].setdefault("papa", 2)
    assert d1["level1"]["level2"] == {"apa": 1, "papa": 2}


def test_get_prop_multiple_legacy_identifiers_only_one(mocked_folio_client):
    record_map = {
        "data": [
            {
                "folio_field": "firstLevel",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "string",
                "additionalProperties": False,
            },
        },
    }
    legacy_record = {"firstLevel": "user_name_1", "id": "1 1"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["id"] == "6ba39416-de70-591e-a45f-62c9ca4e2d98"


def test_get_prop_multiple_legacy_identifiers(mocked_folio_client):
    record_map = {
        "data": [
            {
                "folio_field": "firstLevel",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id2",
                "value": "",
                "description": "",
            },
        ]
    }
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "firstLevel": {
                "type": "string",
                "additionalProperties": False,
            },
        },
    }
    legacy_record = {"firstLevel": "user_name_1", "id": "1", "id2": "1"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["id"] == "6ba39416-de70-591e-a45f-62c9ca4e2d98"


def test_value_mapped_enum_properties(mocked_folio_client):
    record_map = {
        "data": [
            {
                "folio_field": "my_enum",
                "legacy_field": "",
                "value": "014/EAN",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "my_enum": {
                "type": "string",
                "enum": ["014/EAN", "31B/US-SAN", "091/Vendor-assigned", "092/Customer-assigned"],
            },
        },
    }
    legacy_record = {"id": "1"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["my_enum"] == "014/EAN"


def test_value_mapped_non_enum_properties(mocked_folio_client):
    record_map = {
        "data": [
            {
                "folio_field": "my_enum",
                "legacy_field": "",
                "value": "014/EAN",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "my_enum": {
                "type": "string",
            },
        },
    }
    legacy_record = {"id": "1"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["my_enum"] == "014/EAN"


def test_value_not_mapped_mapped_non_enum_properties(mocked_folio_client):
    record_map = {
        "data": [
            {
                "folio_field": "my_enum",
                "legacy_field": "Not mapped",
                "value": "014/EAN",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "A holdings record",
        "type": "object",
        "required": [],
        "properties": {
            "my_enum": {
                "type": "string",
            },
        },
    }
    legacy_record = {"id": "1"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["my_enum"] == "014/EAN"
