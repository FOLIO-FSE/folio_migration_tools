from unittest.mock import MagicMock, Mock
from folioclient import FolioClient
from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)


class MyTestableFileMapper(MappingFileMapperBase):
    def __init__(self, schema: dict, record_map: dict):
        mock_conf = Mock(spec=LibraryConfiguration)
        mock_folio = Mock(spec=FolioClient)
        mock_folio.okapi_url = "okapi_url"
        mock_folio.folio_get_single_object = MagicMock(
            return_value={
                "instances": {"prefix": "pref", "startNumber": "1"},
                "holdings": {"prefix": "pref", "startNumber": "1"},
            }
        )
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
            value = self.mapped_from_values.get(folio_prop_name, "")
            return value
        legacy_values = MappingFileMapperBase.get_legacy_vals(
            legacy_item, legacy_item_keys
        )
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
    # with pytest.raises(TransformationRecordFailedError):
    #   MapperBase.validate_required_properties("", record, schema)
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
    # with pytest.raises(TransformationRecordFailedError):
    #   MapperBase.validate_required_properties("", record, schema)
    tfm = MyTestableFileMapper(schema, fake_holdings_map)
    folio_rec, folio_id = tfm.do_map(record, record["id"], FOLIONamespaces.holdings)
    assert len(folio_rec["electronicAccess"]) == 1
