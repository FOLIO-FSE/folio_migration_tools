import csv
import io
from pathlib import Path
from unittest.mock import Mock

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer
from folio_migration_tools.test_infrastructure import mocked_classes


# flake8: noqa
class MyTestableFileMapper(MappingFileMapperBase):
    def __init__(self, schema: dict, record_map: dict):
        mock_conf = Mock(spec=LibraryConfiguration)
        mock_conf.multi_field_delimiter = "<delimiter>"
        mock_folio = mocked_classes.mocked_folio_client()
        super().__init__(
            mock_folio,
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


def test_validate_required_properties_sub_pro_missing_uri():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 1
    assert folio_id == "11"
    assert folio_rec["id"] == "f00d59ac-4cfc-56d6-9c62-dc9084c18003"


def test_validate_required_properties_sub_pro_missing_uri_and_more():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 1


def test_validate_required_properties_item_notes():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_item_notes_unmapped():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_item_notes_unmapped_2():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert len(folio_rec["notes"]) == 1


def test_validate_required_properties_obj():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["electronicAccessObj"]["uri"] == "some_link"


def test_validate_required_properties_item_notes_split_on_delimiter_notes():
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
    tfm = MyTestableFileMapper(schema, fake_item_map)
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


def test_multiple_repeated_split_on_delimiter_electronic_access():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
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


def test_validate_required_properties_item_notes_split_on_delimiter_plain_object():
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
    tfm = MyTestableFileMapper(schema, fake_item_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    ItemsTransformer.handle_notes(folio_rec)
    assert folio_rec["uber_prop"]["prop1"] == "my note<delimiter>my second note"
    assert folio_rec["uber_prop"]["prop2"] == "Some value"


def test_zip3():
    o = {"p1": "a<delimiter>b", "p2": "c<delimiter>d<delimiter>e", "p3": "same for both"}
    d = "<delimiter>"
    l = ["p1", "p2"]
    s = MappingFileMapperBase.split_obj_by_delim(d, o, l)
    assert s[0] == {"p1": "a", "p2": "c", "p3": "same for both"}
    assert s[1] == {"p1": "b", "p2": "d", "p3": "same for both"}
    assert len(s) == 2


def test_do_not_split_string_prop():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert folio_rec["formerId"] == "id2<delimiter>id3"


def test_split_former_ids():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["formerIds"]) == 3
    assert "id1" in folio_rec["formerIds"]
    assert "id2" in folio_rec["formerIds"]
    assert "id3" in folio_rec["formerIds"]


def test_validate_no_leakage_between_properties():
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
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
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
