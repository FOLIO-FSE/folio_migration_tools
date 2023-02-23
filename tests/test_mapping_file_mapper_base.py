import csv
import functools
import io
from pathlib import Path
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_report import MigrationReport
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
        if len(legacy_values) > 0 and all(isinstance(v, bool) for v in legacy_values):
            return legacy_values[0]
        return " ".join(legacy_values).strip()


def test_validate_required_properties_sub_pro_missing_uri(mocked_folio_client: FolioClient):
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
                "value": "23d1669c-a32d-5bd0-b232-ac40181a5c7e",
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
                "value": "23d1669c-a32d-5bd0-b232-ac40181a5c7e",
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
        "id": "32",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 1
    assert folio_id == "32"
    assert folio_rec["id"] == "ddafc006-e8ce-5f12-b0eb-9bb543509b78"


def test_validate_required_properties_sub_pro_missing_uri_and_more(
    mocked_folio_client: FolioClient,
):
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
        "id": "33",
        "third_0": "",
        "third_1": "",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 1


def test_validate_required_properties_item_notes(mocked_folio_client: FolioClient):
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


def test_validate_required_properties_item_notes_unmapped(mocked_folio_client: FolioClient):
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
    record = {"note_1": "my note", "id": "34"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_item_notes_unmapped_2(mocked_folio_client: FolioClient):
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
    record = {"note_1": "my note", "id": "35"}
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_obj(mocked_folio_client: FolioClient):
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
        "id": "36",
        "third_0": "",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["electronicAccessObj"]["uri"] == "some_link"


def test_validate_required_properties_item_notes_split_on_delimiter_notes(
    mocked_folio_client: FolioClient,
):
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
    record = {"note_1": "my note<delimiter>my second note", "id": "37"}
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


def test_validate_remove_and_report_incomplete_object_property(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "status": {
                "description": "The status of this organization",
                "type": "string",
                "enum": ["Active", "Inactive", "Pending"],
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The unique id of this interface",
                            "$ref": "../../common/schemas/uuid.json",
                            "type": "string",
                        },
                        "uri": {
                            "description": "The URI this interface",
                            "type": "string",
                        },
                        "name": {"description": "The name of this interface", "type": "string"},
                    },
                    "required": ["name"],
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": False,
        "required": ["status"],
    }

    record1 = {
        "id": "id1",
        "status": "Active",
        "int_name": "",
        "int_uri": "this has a uri but no name",
        "int_nameB": "A name",
        "int_uriB": "A uri",
    }

    record2 = {
        "id": "id2",
        "status": "Active",
        "int_name": "This has a name but no uri",
        "int_uri": "",
    }

    record3 = {
        "id": "id3",
        "status": "Active",
        "int_name": "",
        "int_uri": "eb1e8cd4-4bb6-51b0-a2a7-51159d552e13",
    }
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "status",
                "legacy_field": "status",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].uri",
                "legacy_field": "int_uri",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].name",
                "legacy_field": "int_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[1].uri",
                "legacy_field": "int_uriB",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[1].name",
                "legacy_field": "int_nameB",
                "value": "",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record1, record1["id"], FOLIONamespaces.organizations)
    assert len(folio_rec["interfaces"]) == 1

    folio_rec, folio_id = mapper.do_map(record2, record2["id"], FOLIONamespaces.organizations)
    assert "name" in folio_rec["interfaces"][0]
    assert "uri" not in folio_rec["interfaces"][0]

    folio_rec, folio_id = mapper.do_map(record3, record3["id"], FOLIONamespaces.organizations)
    assert "interfaces" not in folio_rec


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
        "id": "38",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 2
    assert folio_id == "38"
    assert folio_rec["id"] == "02e198b2-ebc1-5d46-bf98-eaaba6aa0794"

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
    mocked_folio_client: FolioClient,
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
    record = {"note_1": "my note<delimiter>my second note", "id": "39"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert folio_rec["uber_prop"]["prop1"] == "my note<delimiter>my second note"
    assert folio_rec["uber_prop"]["prop2"] == "Some value"


def test_concatenate_fields_if_mapped_multiple_times(mocked_folio_client: FolioClient):
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
    record = {"note_1": "my note", "note_2": "my second note", "id": "1493"}
    # Loop to make sure the right order occurs the first time.
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    for r in range(8000, 2000):
        record["id"] = str(r)
        folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
        assert folio_rec["uber_prop"]["prop1"] == "my note my second note"


def test_concatenate_fields_if_mapped_multiple_times_and_data_is_in_random_order(
    mocked_folio_client: FolioClient,
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
    record = {"note_2": "my second note", "id": "14", "note_1": "my note"}
    # Loop to make sure the right order occurs the first time.
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    for r in range(11000, 2000):
        record["id"] = str(r)
        folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
        assert folio_rec["uber_prop"]["prop1"] == "my note my second note"


def test_zip3(mocked_folio_client: FolioClient):
    o = {"p1": "a<delimiter>b", "p2": "c<delimiter>d<delimiter>e", "p3": "same for both"}
    d = "<delimiter>"
    l = ["p1", "p2"]
    s = MappingFileMapperBase.split_obj_by_delim(d, o, l)
    assert s[0] == {"p1": "a", "p2": "c", "p3": "same for both"}
    assert s[1] == {"p1": "b", "p2": "d", "p3": "same for both"}
    assert len(s) == 2


def test_do_not_split_string_prop(mocked_folio_client: FolioClient):
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
        "id": "15",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["formerId"] == "id2<delimiter>id3"


def test_split_former_ids(mocked_folio_client: FolioClient):
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
        "id": "16",
    }
    tfm = MyTestableFileMapper(schema, fake_holdings_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["formerIds"]) == 3
    assert "id1" in folio_rec["formerIds"]
    assert "id2" in folio_rec["formerIds"]
    assert "id3" in folio_rec["formerIds"]


def test_validate_no_leakage_between_properties(mocked_folio_client: FolioClient):
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
    record = {"stmt_1": "stmt", "id": "17", "stmt_2": "suppl", "stmt_3": "idx"}
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


def test_map_string_first_level(mocked_folio_client: FolioClient):
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


def test_map_string_array_first_level(mocked_folio_client: FolioClient):
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


def test_map_string_second_level(mocked_folio_client: FolioClient):
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
    record = {"id": "488", "note_1": "my note"}
    # Loop to make sure the right order occurs the first time.
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    for r in range(10000, 2000):
        record["id"] = str(r)
        folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
        assert folio_rec["firstLevel"]["secondLevel"] == "my note"


def test_map_string_array_second_level(mocked_folio_client: FolioClient):
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
    record = {"id": "19", "note_1": "my note"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note"]


def test_map_string_array_second_level_multiple_values(mocked_folio_client: FolioClient):
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
    record = {"id": "20", "note_1": "my note", "note_2": "my note 2"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note", "my note 2"]


def test_map_string_array_second_level_multiple_additional_split_values(
    mocked_folio_client: FolioClient,
):
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
    record = {"id": "21", "note_1": "my note", "note_2": "my note 2<delimiter>my note 3"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note", "my note 2", "my note 3"]


def test_map_string_array_second_level_split_values(mocked_folio_client: FolioClient):
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
    record = {"id": "22", "note_2": "my note 2<delimiter>my note 3"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["stringArray"] == ["my note 2", "my note 3"]


def test_map_array_of_objects_with_string_array(mocked_folio_client: FolioClient):
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
    record = {"id": "23", "note_1": "my note", "note_2": "my note 2"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"][0]["secondLevel"] == ["my note", "my note 2"]


def test_map_array_of_objects_with_string_array_delimiter(mocked_folio_client: FolioClient):
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
    record = {"id": "24", "note_1": "my note", "note_2": "my note 2<delimiter>my note 3"}
    # Loop to make sure the right order occurs the first time.

    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"][0]["secondLevel"] == ["my note", "my note 2", "my note 3"]


def test_map_string_third_level(mocked_folio_client: FolioClient):
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
    record = {"id": "25", "note_1": "my note"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert (
        folio_rec["firstLevel"]["secondLevel"]["thirdLevel"] == "my note"
    )  # No mapping on third level yet...


def test_map_string_and_array_of_strings_fourth_level(mocked_folio_client: FolioClient):
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
    record = {"id": "26", "note_1": "my note", "note_2": "my note 2", "note_3": "my note 3"}
    tfm = MyTestableFileMapper(schema, fake_item_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["firstLevel"]["secondLevel"]["thirdLevel"]["fourthLevel"] == "my note"
    assert folio_rec["firstLevel"]["secondLevel"]["thirdLevel"]["fourthLevelArr"] == [
        "my note 2",
        "my note 3",
    ]  # No mapping on third level yet...


def test_map_object_and_array_of_strings_fourth_level(mocked_folio_client: FolioClient):
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
    record = {"id": "27", "note_1": "my note", "note_2": "my note 2", "note_3": "my note 3"}
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


def test_map_enums(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "status": {
                "description": "The status of this organization",
                "type": "string",
                "enum": ["Active", "Inactive", "Pending"],
            },
            "organizationTypes": {
                "description": "A list of organization types assigned to this organization",
                "type": "array",
                "items": {
                    "description": "UUID of an organization type record",
                    "$ref": "../../common/schemas/uuid.json",
                    "type": "string",
                },
                "uniqueItems": True,
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The unique id of this interface",
                            "$ref": "../../common/schemas/uuid.json",
                            "type": "string",
                        },
                        "name": {"description": "The name of this interface", "type": "string"},
                        "uri": {"description": "The URI of this interface", "type": "string"},
                        "notes": {"description": "The notes for this interface", "type": "string"},
                        "available": {
                            "description": "The availability setting for this interface",
                            "type": "boolean",
                        },
                        "deliveryMethod": {
                            "description": "The delivery method for this interface",
                            "type": "string",
                            "enum": ["Online", "FTP", "Email", "Other"],
                        },
                        "statisticsFormat": {
                            "description": "The format of the statistics for this interface",
                            "type": "string",
                        },
                        "locallyStored": {
                            "description": "The locally stored location of this interface",
                            "type": "string",
                        },
                        "onlineLocation": {
                            "description": "The online location for this interface",
                            "type": "string",
                        },
                        "statisticsNotes": {
                            "description": "The notes regarding the statistics for this interface",
                            "type": "string",
                        },
                        "type": {
                            "description": "Interface types",
                            "type": "array",
                            "items": {
                                "type": "string",
                                "$ref": "interface_type.json",
                                "$schema": "http://json-schema.org/draft-04/schema#",
                                "description": "the type of interface",
                                "enum": [
                                    "Admin",
                                    "End user",
                                    "Reports",
                                    "Orders",
                                    "Invoices",
                                    "Other",
                                ],
                            },
                        },
                        "metadata": {
                            "type": "Deprecated",
                            "$ref": "../../../raml-util/schemas/metadata.schema",
                            "readonly": True,
                        },
                    },
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": False,
        "required": ["status"],
    }
    record = {"id": "id1", "status": "Pending"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {"folio_field": "status", "legacy_field": "status", "value": "", "description": ""},
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert folio_rec["status"] == "Pending"


def test_map_enums_empty_required(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "status": {
                "description": "The status of this organization",
                "type": "string",
                "enum": ["Active", "Inactive", "Pending"],
            },
            "organizationTypes": {
                "description": "A list of organization types assigned to this organization",
                "type": "array",
                "items": {
                    "description": "UUID of an organization type record",
                    "$ref": "../../common/schemas/uuid.json",
                    "type": "string",
                },
                "uniqueItems": True,
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The unique id of this interface",
                            "$ref": "../../common/schemas/uuid.json",
                            "type": "string",
                        },
                        "name": {"description": "The name of this interface", "type": "string"},
                        "uri": {"description": "The URI of this interface", "type": "string"},
                        "notes": {"description": "The notes for this interface", "type": "string"},
                        "available": {
                            "description": "The availability setting for this interface",
                            "type": "boolean",
                        },
                        "deliveryMethod": {
                            "description": "The delivery method for this interface",
                            "type": "string",
                            "enum": ["Online", "FTP", "Email", "Other"],
                        },
                        "statisticsFormat": {
                            "description": "The format of the statistics for this interface",
                            "type": "string",
                        },
                        "locallyStored": {
                            "description": "The locally stored location of this interface",
                            "type": "string",
                        },
                        "onlineLocation": {
                            "description": "The online location for this interface",
                            "type": "string",
                        },
                        "statisticsNotes": {
                            "description": "The notes regarding the statistics for this interface",
                            "type": "string",
                        },
                        "type": {
                            "description": "Interface types",
                            "type": "array",
                            "items": {
                                "type": "string",
                                "$ref": "interface_type.json",
                                "$schema": "http://json-schema.org/draft-04/schema#",
                                "description": "the type of interface",
                                "enum": [
                                    "Admin",
                                    "End user",
                                    "Reports",
                                    "Orders",
                                    "Invoices",
                                    "Other",
                                ],
                            },
                        },
                        "metadata": {
                            "type": "Deprecated",
                            "$ref": "../../../raml-util/schemas/metadata.schema",
                            "readonly": True,
                        },
                    },
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": False,
        "required": ["status"],
    }
    record = {"id": "id1", "status": ""}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {"folio_field": "status", "legacy_field": "status", "value": "", "description": ""},
        ]
    }
    with pytest.raises(TransformationRecordFailedError):
        mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
        folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)


def test_map_empty_not_required_enums(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "status": {
                "description": "The status of this organization",
                "type": "string",
                "enum": ["Active", "Inactive", "Pending"],
            },
            "organizationTypes": {
                "description": "A list of organization types assigned to this organization",
                "type": "array",
                "items": {
                    "description": "UUID of an organization type record",
                    "$ref": "../../common/schemas/uuid.json",
                    "type": "string",
                },
                "uniqueItems": True,
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The unique id of this interface",
                            "$ref": "../../common/schemas/uuid.json",
                            "type": "string",
                        },
                        "name": {"description": "The name of this interface", "type": "string"},
                        "uri": {"description": "The URI of this interface", "type": "string"},
                        "notes": {"description": "The notes for this interface", "type": "string"},
                        "available": {
                            "description": "The availability setting for this interface",
                            "type": "boolean",
                        },
                        "deliveryMethod": {
                            "description": "The delivery method for this interface",
                            "type": "string",
                            "enum": ["Online", "FTP", "Email", "Other"],
                        },
                        "statisticsFormat": {
                            "description": "The format of the statistics for this interface",
                            "type": "string",
                        },
                        "locallyStored": {
                            "description": "The locally stored location of this interface",
                            "type": "string",
                        },
                        "onlineLocation": {
                            "description": "The online location for this interface",
                            "type": "string",
                        },
                        "statisticsNotes": {
                            "description": "The notes regarding the statistics for this interface",
                            "type": "string",
                        },
                        "type": {
                            "description": "Interface types",
                            "type": "array",
                            "items": {
                                "type": "string",
                                "$ref": "interface_type.json",
                                "$schema": "http://json-schema.org/draft-04/schema#",
                                "description": "the type of interface",
                                "enum": [
                                    "Admin",
                                    "End user",
                                    "Reports",
                                    "Orders",
                                    "Invoices",
                                    "Other",
                                ],
                            },
                        },
                        "metadata": {
                            "type": "Deprecated",
                            "$ref": "../../../raml-util/schemas/metadata.schema",
                            "readonly": True,
                        },
                    },
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": False,
        "required": [],
    }
    record = {"id": "id1", "status": ""}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {"folio_field": "status", "legacy_field": "status", "value": "", "description": ""},
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert "status" not in folio_rec


def test_map_enums_invalid_required(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "status": {
                "description": "The status of this organization",
                "type": "string",
                "enum": ["Active", "Inactive", "Pending"],
            },
        },
        "additionalProperties": False,
        "required": ["status"],
    }
    record = {"id": "id1", "status": "Whaaaat?"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {"folio_field": "status", "legacy_field": "status", "value": "", "description": ""},
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    with pytest.raises(
        TransformationRecordFailedError, match=r"Forbidden enum value found"
    ):
        folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)


def test_map_enums_invalid_not_required(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "status": {
                "description": "The status of this organization",
                "type": "string",
                "enum": ["Active", "Inactive", "Pending"],
            },
        },
        "additionalProperties": False,
        "required": [],
    }
    record = {"id": "id1", "status": "Whaaaat?"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {"folio_field": "status", "legacy_field": "status", "value": "", "description": ""},
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    with pytest.raises(
        TransformationRecordFailedError, match=r"Forbidden enum value found"
    ):
        folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)


def test_map_enums_empty_not_required_deeper_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "status": {
                "description": "The status of this organization",
                "type": "string",
                "enum": ["Active", "Inactive", "Pending"],
            },
            "organizationTypes": {
                "description": "A list of organization types assigned to this organization",
                "type": "array",
                "items": {
                    "description": "UUID of an organization type record",
                    "$ref": "../../common/schemas/uuid.json",
                    "type": "string",
                },
                "uniqueItems": True,
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The unique id of this interface",
                            "$ref": "../../common/schemas/uuid.json",
                            "type": "string",
                        },
                        "name": {"description": "The name of this interface", "type": "string"},
                        "deliveryMethod": {
                            "description": "The delivery method for this interface",
                            "type": "string",
                            "enum": ["Online", "FTP", "Email", "Other"],
                        },
                    },
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": False,
        "required": [],
    }
    record = {"id": "id1", "delivery_method": ""}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].deliveryMethod",
                "legacy_field": "delivery_method",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].name",
                "legacy_field": "",
                "value": "apa",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert "deliveryMethod" not in folio_rec["interfaces"][0]


def test_map_enums_invalid_not_required_deeper_level(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The unique id of this interface",
                            "$ref": "../../common/schemas/uuid.json",
                            "type": "string",
                        },
                        "name": {"description": "The name of this interface", "type": "string"},
                        "deliveryMethod": {
                            "description": "The delivery method for this interface",
                            "type": "string",
                            "enum": ["Online", "FTP", "Email", "Other"],
                        },
                    },
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": False,
        "required": [],
    }
    record = {"id": "id1", "delivery_method": "Offline"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].deliveryMethod",
                "legacy_field": "delivery_method",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].name",
                "legacy_field": "",
                "value": "apa",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    with pytest.raises(
        TransformationRecordFailedError, match=r"Forbidden enum value found"
    ):
        folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)


def test_default_false(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "isVendor": {
                "id": "isVendor",
                "description": "Used to indicate that this organization is also a vendor",
                "type": "boolean",
                "default": False,
            },
        },
    }
    record = {"id": "id1", "delivery_method": "Offline"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert folio_rec["isVendor"] is False


def test_default_false_and_mapped_to_true(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "isVendor": {
                "id": "isVendor",
                "description": "Used to indicate that this organization is also a vendor",
                "type": "boolean",
                "default": False,
            },
        },
    }
    record = {"id": "id1", "is_vendor": True}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "isVendor",
                "legacy_field": "is_vendor",
                "value": "",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert folio_rec["isVendor"] is True


def test_default_false_and_value_set_to_true(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "isVendor": {
                "id": "isVendor",
                "description": "Used to indicate that this organization is also a vendor",
                "type": "boolean",
                "default": False,
            },
        },
    }
    record = {"id": "id1"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "isVendor",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert folio_rec["isVendor"] is True


def test_default_no_defaults_on_subprops(mocked_folio_client: FolioClient):
    """Test that verifies that we do not add default values to sub-properties (yet), since this
    could trigger half-baked sub-objects with no actual content.

    Args:
        mocked_folio_client (_type_): _description_
    """
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "isVendor": {
                            "id": "isVendor",
                            "description": "Used to indicate that this organization is also a vendor",
                            "type": "boolean",
                            "default": True,
                        },
                    },
                    "additionalProperties": False,
                },
            },
        },
        "additionalProperties": False,
        "required": [],
    }
    record = {"id": "id1", "delivery_method": "Offline"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert "interfaces" not in folio_rec


def test_default_true(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "isVendor": {
                "id": "isVendor",
                "description": "Used to indicate that this organization is also a vendor",
                "type": "boolean",
                "default": True,
            },
        },
    }
    record = {"id": "id1", "delivery_method": "Offline"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)
    assert folio_rec["isVendor"] is True


def test_map_wrong_not_required_deeper_level_enums(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "id": {
                "description": "The unique UUID for this organization",
                "$ref": "../../common/schemas/uuid.json",
                "type": "string",
            },
            "interfaces": {
                "id": "interfaces",
                "description": "The list of interfaces assigned to this organization",
                "type": "array",
                "items": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "description": "An interface record",
                    "type": "object",
                    "properties": {
                        "id": {
                            "description": "The unique id of this interface",
                            "$ref": "../../common/schemas/uuid.json",
                            "type": "string",
                        },
                        "name": {"description": "The name of this interface", "type": "string"},
                        "deliveryMethod": {
                            "description": "The delivery method for this interface",
                            "type": "string",
                            "enum": ["Online", "FTP", "Email", "Other"],
                        },
                    },
                    "additionalProperties": True,
                },
            },
        },
        "additionalProperties": False,
        "required": [],
    }
    record = {"id": "id1", "delivery_method": "Offline"}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].deliveryMethod",
                "legacy_field": "delivery_method",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].name",
                "legacy_field": "",
                "value": "apa",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    with pytest.raises(
        TransformationRecordFailedError, match=r"Forbidden enum value found"
    ):
        folio_rec, folio_id = mapper.do_map(record, record["id"], FOLIONamespaces.organizations)


def test_map_array_object_array_object_string(mocked_folio_client: FolioClient):
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
                                "properties": {"addressLine1": {"type": "string"}},
                            },
                        }
                    },
                },
            }
        },
    }
    record = {
        "id": "id1",
        "contact_person": "Jane",
        "contact_address_line1": "My Street",
        "contact_address2_line1": "My other street",
        "contact2_address_line1": "Yet another street",
        "contact_address_town": "Gothenburg",
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
                "folio_field": "contacts[0].addresses[1].addressLine1",
                "legacy_field": "contact_address2_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[1].addresses[0].addressLine1",
                "legacy_field": "contact2_address_line1",
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
            },
        ]
    }

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["contacts"][0]["addresses"][0]["addressLine1"] == "My Street"
    assert folio_rec["contacts"][0]["addresses"][1]["addressLine1"] == "My other street"
    assert folio_rec["contacts"][1]["addresses"][0]["addressLine1"] == "Yet another street"


def test_do_not_overwrite_array_object_array_object_string_with_array_object_string(
    mocked_folio_client: FolioClient,
):
    # TODO Make test succeed

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
                        "firstName": {
                            "type": "string",
                        },
                        "lastName": {
                            "type": "string",
                        },
                        "addresses": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "city": {
                                        "type": "string",
                                    },
                                    "addressLine1": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            }
        },
    }
    record = {
        "id": "id1",
        "contact_person_fn": "Jane",
        "contact_person_ln": "Deer",
        "contact_address_line1": "My Street",
        "contact_address_town": "Gothenburg",
        "contact_address_types": "support<delimiter>sales",
    }
    org_map = {
        "data": [
            {
                "folio_field": "contacts[0].firstName",
                "legacy_field": "contact_person_fn",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].lastName",
                "legacy_field": "contact_person_ln",
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
                "folio_field": "contacts[0].addresses[0].city",
                "legacy_field": "contact_address_town",
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
            },
        ]
    }

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["contacts"][0]["firstName"] == "Jane"
    assert folio_rec["contacts"][0]["lastName"] == "Deer"
    assert folio_rec["contacts"][0]["addresses"][0]["addressLine1"] == "My Street"
    assert folio_rec["contacts"][0]["addresses"][0]["city"] == "Gothenburg"


def test_do_not_overwrite_array_object_array_object_string_with_array_object_string2(
    mocked_folio_client: FolioClient,
):
    # TODO Make test succeed

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
                        "firstName": {
                            "type": "string",
                        },
                        "lastName": {
                            "type": "string",
                        },
                        "addresses": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "city": {
                                        "type": "string",
                                    },
                                    "addressLine1": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            }
        },
    }
    record = {
        "id": "id1",
        "contact_person_fn": "Jane",
        "contact_person_ln": "Deer",
        "contact_address_line1": "My Street",
        "contact_address_town": "Gothenburg",
        "contact_address_types": "support<delimiter>sales",
        "contact_person_fn1": "John",
        "contact_person_ln1": "Dear",
        "contact_address2_line1": "My second Street",
        "contact_address2_town": "Fritsla",
        "contact_address2_types": "support<delimiter>sales",
    }
    org_map = {
        "data": [
            {
                "folio_field": "contacts[0].firstName",
                "legacy_field": "contact_person_fn",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].lastName",
                "legacy_field": "contact_person_ln",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[1].firstName",
                "legacy_field": "contact_person_fn1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[1].lastName",
                "legacy_field": "contact_person_ln1",
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
                "folio_field": "contacts[0].addresses[0].city",
                "legacy_field": "contact_address_town",
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
                "folio_field": "contacts[0].addresses[1].addressLine1",
                "legacy_field": "contact_address2_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].addresses[1].city",
                "legacy_field": "contact_address2_town",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].addresses[1].categories",
                "legacy_field": "contact_address2_types",
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

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["contacts"][0]["firstName"] == "Jane"
    assert folio_rec["contacts"][0]["lastName"] == "Deer"
    assert folio_rec["contacts"][1]["firstName"] == "John"
    assert folio_rec["contacts"][1]["lastName"] == "Dear"
    assert folio_rec["contacts"][0]["addresses"][1]["addressLine1"] == "My second Street"
    assert folio_rec["contacts"][0]["addresses"][1]["city"] == "Fritsla"


def test_map_array_object_array_string_on_edge(mocked_folio_client: FolioClient):
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
                        "streets": {
                            "type": "array",
                            "items": {"type": "string"},
                        }
                    },
                },
            }
        },
    }
    record = {
        "id": "id1",
        "contact_person": "Jane",
        "contact_address_line1": "My Street",
        "contact_address2_line1": "My other street",
        "contact2_address_line1": "Yet another street",
        "contact_address_town": "Gothenburg",
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
                "folio_field": "contacts[0].streets[0]",
                "legacy_field": "contact_address_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[0].streets[1]",
                "legacy_field": "contact_address2_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "contacts[1].streets[0]",
                "legacy_field": "contact2_address_line1",
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

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["contacts"][0]["streets"][0] == "My Street"
    assert folio_rec["contacts"][0]["streets"][1] == "My other street"
    assert folio_rec["contacts"][1]["streets"][0] == "Yet another street"


def test_map_array_object_array_string_on_edge_lowest(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "streets": {
                "type": "array",
                "items": {"type": "string"},
            }
        },
    }
    record = {
        "id": "id1",
        "contact_person": "Jane",
        "contact_address_line1": "My Street",
        "contact_address2_line1": "My other street",
        "contact2_address_line1": "Yet another street",
        "contact_address_town": "Gothenburg",
        "contact_address_types": "support<delimiter>sales",
    }
    org_map = {
        "data": [
            {
                "folio_field": "streets[0]",
                "legacy_field": "contact_address_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "streets[1]",
                "legacy_field": "contact_address2_line1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "streets[2]",
                "legacy_field": "contact2_address_line1",
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

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["streets"][0] == "My Street"
    assert folio_rec["streets"][1] == "My other street"
    assert folio_rec["streets"][2] == "Yet another street"


# TODO Make this run successfully.
def test_map_array_object_array_object_array_string(mocked_folio_client: FolioClient):
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
                                    "categories": {"type": "array", "items": {"type": "string"}},
                                    "addressLine1": {"type": "string"},
                                },
                            },
                        }
                    },
                },
            }
        },
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
                "folio_field": "contacts[0].addresses[0].categories[0]",
                "legacy_field": "contact_address_types",
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

    contact = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = contact.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["contacts"][0]["addresses"][0]["categories"] == ["support", "sales"]


def test_set_default(mocked_folio_client: FolioClient):
    d1 = {"level1": {"level2": {}}}
    d1["level1"]["level2"] = {"apa": 1}
    d1["level1"]["level2"].setdefault("papa", 2)
    assert d1["level1"]["level2"] == {"apa": 1, "papa": 2}


def test_get_prop_multiple_legacy_identifiers_only_one(mocked_folio_client: FolioClient):
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


def test_get_prop_multiple_legacy_identifiers(mocked_folio_client: FolioClient):
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
    legacy_record = {"firstLevel": "user_name_1", "id": "44", "id2": "1"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["id"] == "d82d9010-6348-5225-a1e7-c3b52924e3e7"


def test_value_mapped_enum_properties(mocked_folio_client: FolioClient):
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


def test_value_mapped_non_enum_properties(mocked_folio_client: FolioClient):
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
    legacy_record = {"id": "29"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["my_enum"] == "014/EAN"


def test_value_not_mapped_mapped_non_enum_properties(mocked_folio_client: FolioClient):
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
    legacy_record = {"id": "30"}
    tfm = MyTestableFileMapper(schema, record_map, mocked_folio_client)
    folio_rec, folio_id = tfm.do_map(legacy_record, legacy_record["id"], FOLIONamespaces.holdings)
    assert folio_rec["my_enum"] == "014/EAN"


def test_map_array_object_array_string(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "interfaces": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "array",
                            "items": {"type": "string"},
                        }
                    },
                },
            }
        },
    }

    record = {"id": "id7", "interface_name": "FOLIO", "interface_type": "Admin"}
    org_map = {
        "data": [
            {
                "folio_field": "interfaces[0].name",
                "legacy_field": "interface_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].type[0]",
                "legacy_field": "interface_type",
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

    interface = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = interface.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["interfaces"][0]["type"] == ["Admin"]


def test_get_legacy_value_mapped_value():
    legacy_object = {"title": "Leif"}
    mapping_file_entry = {
        "folio_field": "title",
        "legacy_field": "title",
        "value": "",
        "description": "",
    }

    res = MappingFileMapperBase.get_legacy_value(
        legacy_object, mapping_file_entry, MigrationReport(), ""
    )
    assert res == "Leif"


def test_get_legacy_value_value():
    legacy_object = {"title": "Leif"}
    mapping_file_entry = {
        "folio_field": "title",
        "legacy_field": "title",
        "value": "Torsten",
        "description": "",
    }
    res = MappingFileMapperBase.get_legacy_value(
        legacy_object, mapping_file_entry, MigrationReport(), ""
    )
    assert res == "Torsten"


def test_get_legacy_value_replace_value():
    legacy_object = {"title": "0"}
    mapping_file_entry = {
        "folio_field": "title",
        "legacy_field": "title",
        "value": "",
        "description": "",
        "rules": {"replaceValues": {"0": "Graduate", "a": "Alumni"}},
    }
    res = MappingFileMapperBase.get_legacy_value(
        legacy_object, mapping_file_entry, MigrationReport(), ""
    )
    assert res == "Graduate"


def test_get_legacy_value_regex():
    legacy_object = {"title": "leif@leifochbilly.se"}
    mapping_file_entry = {
        "folio_field": "title",
        "legacy_field": "title",
        "value": "",
        "description": "",
        "rules": {"regexGetFirstMatchOrEmpty": "(.*)@.*"},
    }
    res = MappingFileMapperBase.get_legacy_value(
        legacy_object, mapping_file_entry, MigrationReport(), ""
    )
    assert res == "leif"


def test_get_legacy_value_fallback_field():
    legacy_object = {"title": "", "alternative_title": "billy@leifochbilly.se"}
    mapping_file_entry = {
        "folio_field": "title",
        "legacy_field": "title",
        "value": "",
        "description": "",
        "fallback_legacy_field": "alternative_title",
    }
    res = MappingFileMapperBase.get_legacy_value(
        legacy_object, mapping_file_entry, MigrationReport(), ""
    )
    assert res == "billy@leifochbilly.se"


def test_get_legacy_value_fallback_value():
    legacy_object = {"title": "", "alternative_title": ""}
    mapping_file_entry = {
        "folio_field": "title",
        "legacy_field": "title",
        "value": "",
        "description": "",
        "fallback_legacy_field": "alternative_title",
        "fallback_value": "info@leifochbilly.se",
    }
    res = MappingFileMapperBase.get_legacy_value(
        legacy_object, mapping_file_entry, MigrationReport(), ""
    )
    assert res == "info@leifochbilly.se"


def test_get_legacy_value_from_map(mocked_folio_client):
    legacy_object = {"id": "1", "title": "Alpha", "alternative_title": "Omega"}
    schema = {}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {"folio_field": "title", "legacy_field": "title", "value": "", "description": ""},
            {
                "folio_field": "title",
                "legacy_field": "alternative_title",
                "value": "",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    res = mapper.get_value_from_map("title", legacy_object, "")
    assert res == "Alpha Omega"


def test_get_legacy_value_from_map_one_value(mocked_folio_client):
    legacy_object = {"id": "1", "title": "Alpha", "alternative_title": "Omega"}
    schema = {}
    the_map = {
        "data": [
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {"folio_field": "title", "legacy_field": "title", "value": "", "description": ""},
            {
                "folio_field": "title",
                "legacy_field": "alternative_title",
                "value": "Beta",
                "description": "",
            },
        ]
    }
    mapper = MyTestableFileMapper(schema, the_map, mocked_folio_client)
    res = mapper.get_value_from_map("title", legacy_object, "")
    assert res == "Alpha Beta"


def test_map_array_object_object_string(mocked_folio_client):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "interfaces": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "interfaceCredential": {
                            "type": "object",
                            "properties": {
                                "username": {"type": "string"},
                                "password": {"type": "string"},
                            },
                        },
                    },
                },
            }
        },
    }
    record = {
        "id": "ic1",
        "interface_name": "FOLIO",
        "interface_uri": "www",
        "interface_username": "MyUsername",
        "interface_password": "MyPassword",
    }
    org_map = {
        "data": [
            {
                "folio_field": "interfaces[0].interfaceCredential.password",
                "legacy_field": "interface_password",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].name",
                "legacy_field": "interface_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].uri",
                "legacy_field": "interface_uri",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].interfaceCredential.username",
                "legacy_field": "interface_username",
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

    interface = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = interface.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["interfaces"][0]["interfaceCredential"]["username"] == "MyUsername"


def test_map_array_object_subproperty_string(mocked_folio_client: FolioClient):
    schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "The record of an organization",
        "type": "object",
        "properties": {
            "interfaces": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "cost": {
                            "type": "object",
                            "properties": {"currency": {"type": "string"}},
                        },
                    },
                },
            }
        },
    }

    record = {"id": "id7", "interface_name": "FOLIO", "interface_type": "Admin", "curr": "USD"}
    org_map = {
        "data": [
            {
                "folio_field": "interfaces[0].name",
                "legacy_field": "interface_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "interfaces[0].cost.currency",
                "legacy_field": "curr",
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

    interface = MyTestableFileMapper(schema, org_map, mocked_folio_client)
    folio_rec, folio_id = interface.do_map(record, record["id"], FOLIONamespaces.organizations)

    assert folio_rec["interfaces"][0]["name"] == "FOLIO"
    assert folio_rec["interfaces"][0]["cost"]["currency"] == "USD"
