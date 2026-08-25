from pymarc.field import Indicators
import datetime
import io
import json
import logging
import logging.handlers
import types
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient
from pymarc import Leader, Subfield
from pymarc.reader import MARCReader
from pymarc.record import Field, Record

from folio_migration_tools.library_configuration import FolioRelease, HridHandling, LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.marc_reader_wrapper import (
    DEFAULT_MARC_RECORD_PREPROCESSORS,
)
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
    is_array_of_strings,
    is_array_of_objects,
)
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.migration_tasks.migration_task_base import MarcTaskConfigurationBase
from .test_infrastructure import mocked_classes

# flake8: noqa: E501

@pytest.fixture
def folio_client():
    fc = mocked_classes.mocked_folio_client()
    # Set properties through the folio_auth object since they're read-only properties
    fc.folio_auth.gateway_url = "https://folio-snapshot.dev.folio.org"
    fc.folio_auth.tenant_id = "diku"
    fc.folio_username = "diku_admin"
    fc.folio_password = "admin"
    fc.get_holdings_schema = types.MethodType(FolioClient.get_holdings_schema, fc)
    fc.get_instance_json_schema = types.MethodType(FolioClient.get_instance_json_schema, fc)
    reference_data = list(Path(__file__).parent.joinpath("test_data/reference_data").glob("*.json"))
    for ref_data in reference_data:
        with open(ref_data, "r") as f:
            setattr(fc, ref_data.stem, json.load(f))
    return fc


@pytest.fixture
def mapper_base(folio_client):
    mapper_library_configuration = LibraryConfiguration(
        **{
            "gateway_url": "https://folio-snapshot.dev.folio.org",
            "tenant_id": "diku",
            "folio_username": "diku_admin",
            "folio_password": "admin",
            "iteration_identifier": "test",
            "library_name": "Test Library",
            "folio_release": FolioRelease.sunflower,
            "log_level_debug": False,
            "base_folder": "/"
        }
    )
    mapper_task_configuration = MarcTaskConfigurationBase(
        **{
            "name": "test",
            "migration_task_type": "BibsTransformer",
            "hrid_handling": HridHandling.default,
            "files": [],
            # "ils_flavour": "field001"
        }
    )
    mapper = RulesMapperBase(folio_client, mapper_library_configuration, mapper_task_configuration, {}, {})
    mapper.conditions = Conditions(folio_client, mapper, "any", FolioRelease.ramsons, "Library of Congress classification")
    return mapper


def test_dedupe_recs():
    my_dict = {"my_arr": [{"a": "b"}, {"a": "b"}, {"c": "d"}]}
    RulesMapperBase.dedupe_rec(my_dict)
    assert my_dict != {"my_arr": [{"a": "b"}, {"a": "b"}, {"c": "d"}]}
    assert my_dict == {"my_arr": [{"a": "b"}, {"c": "d"}]}


def test_marc_task_configuration_default_preprocessors():
    config = MarcTaskConfigurationBase(
        **{
            "name": "test",
            "migration_task_type": "BibsTransformer",
            "files": [],
        }
    )

    assert config.marc_record_preprocessors == DEFAULT_MARC_RECORD_PREPROCESSORS
    assert config.preprocessors_args == {}


def test_datetime_from_005():
    f005_1 = "19940223151047.0"
    record = Record()
    record.add_field(Field(tag="005", data=f005_1))
    instance = {
        "metadata": {
            "createdDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "updatedDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    }
    RulesMapperBase.set_005_as_updated_date(record, instance, "some_id")
    assert instance["metadata"]["updatedDate"] == "1994-02-23T15:10:47"


def test_date_from_008():
    f008 = "170309s2017\\\\quc\\\\\o\\\\\000\0\fre\d"
    record = Record()
    record.add_field(Field(tag="008", data=f008))
    instance = {
        "title": "some title",
        "metadata": {
            "createdDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "updatedDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        },
    }
    RulesMapperBase.use_008_for_dates(record, instance, "some_id")
    assert instance["catalogedDate"] == "2017-03-09"
    # assert instance["metadata"]["createdDate"] == "2017-03-09T00:00:00"


def test_get_first_subfield_value():
    marc_field = Field(
        tag="100",
        indicators=Indicators(*["0", "1"]),
        subfields=[
            Subfield(code="e", value="puppeteer"),
            Subfield(code="e", value="assistant puppeteer"),
            Subfield(code="e", value="Executive Vice Puppeteer"),
        ],
    )
    assert marc_field.get_subfields("j", "e")[0] == "puppeteer"


def test_get_first_subfield_value_no_subfields():
    with pytest.raises(IndexError):
        marc_field = Field(
            tag="100",
            indicators=Indicators(*["0", "1"]),
            subfields=[],
        )
        assert marc_field.get_subfields("j", "e")[0] == "puppeteer"


def test_remove_subfields():
    marc_field = Field(
        tag="338",
        indicators=Indicators(*["0", "1"]),
        subfields=[
            Subfield(code="b", value="ac"),
            Subfield(code="b", value="ab"),
            Subfield(code="i", value="ba"),
        ],
    )
    new_field = RulesMapperBase.remove_repeated_subfields(marc_field)
    assert len(new_field.subfields_as_dict()) == len(marc_field.subfields_as_dict())
    assert len(marc_field.subfields) == 3
    assert len(new_field.subfields) == 2


def test_date_from_008_holding():
    f008 = "170309s2017\\\\quc\\\\\o\\\\\000\0\fre\d"
    record = Record()
    record.add_field(Field(tag="008", data=f008))
    holding = {
        "metadata": {
            "createdDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "updatedDate": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
    }
    RulesMapperBase.use_008_for_dates(record, holding, "some_id")
    assert "catalogedDate" not in holding
    # assert holding["metadata"]["createdDate"] == "2017-03-09T00:00:00"


def test_add_entity_to_record(folio_client):
    entity = {"id": "id", "type": "type"}
    rec = {}
    latest_schema = folio_client.get_instance_json_schema()
    RulesMapperBase.add_entity_to_record(entity, "identifiers", rec, latest_schema)
    assert rec == {"identifiers": [{"id": "id", "type": "type"}]}


def test_weirdness():
    path = "./tests/test_data/two020a.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = Record()
        for record in reader:
            record1 = record
        f020s = record1.get_fields("020")
        mapping = {"subfield": ["a"]}
        subfields = f020s[1].get_subfields(*mapping["subfield"])
        assert subfields


def test_grouped():
    path = "./tests/test_data/two020a.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = Record()
        for record in reader:
            record1 = record
        f020s = record1.get_fields("020")
        grouped = RulesMapperBase.grouped(f020s[1])
        for tf in grouped:
            assert isinstance(tf, Field)
            assert tf.tag == "020"
            assert tf.subfields in [
                [Subfield(code="a", value="0870990004 (v. 1)"), Subfield(code="c", value="20sek")],
                [Subfield(code="a", value="0870990020 (v. 2)"), Subfield(code="c", value="20sek")],
            ]

        for field in f020s:
            grouped = RulesMapperBase.grouped(field)
            for tf in grouped:
                mapping = {"subfield": ["a"]}
                subfields = tf.get_subfields(*mapping["subfield"])
                assert subfields


def test_get_srs_string_bib():
    path = "./tests/test_data/two020a.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        instance = {"id": str(uuid4()), "hrid": "my hrid"}
        id_holder = {
            "instanceId": instance["id"],
            "instanceHrid": instance["hrid"],
        }
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = None
        for record in reader:
            record1 = record
            srs_record_string = RulesMapperBase.get_srs_string(
                record1,
                instance,
                str(uuid4()),
                True,
                FOLIONamespaces.instances,
            )
            assert '"recordType": "MARC_BIB"' in srs_record_string
            assert json.dumps(id_holder) in srs_record_string
            assert "snapshotId" not in record


def test_get_srs_string_bad_leaders():
    path = "./tests/test_data/corrupt_leader.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = Record()
        record = next(reader)
        l1 = record.leader
        record.leader = Leader(f"{record.leader[:-4]}4500")
        assert l1 != record.leader
        assert str(record.leader).endswith("4500")
        assert len(str(record.leader)) == 24


def test_create_srs_uuid(mapper_base):
    # Set the gateway_url through the folio_auth object since gateway_url is read-only
    mapper_base.folio_client.folio_auth.gateway_url = "some_url"
    created_id = mapper_base.create_srs_id(FOLIONamespaces.holdings, "id_1")
    assert str(created_id) == "06e42308-4555-5bd2-b0b4-4655f7e30e4a"
    created_id_2 = mapper_base.create_srs_id(FOLIONamespaces.instances, "id_1")
    assert str(created_id) != str(created_id_2)


@pytest.fixture
def folio_record():
    return {
        "id": str(uuid4()),
        "title": "Sample Title",
    }


@pytest.fixture
def marc_record():
    record = Record()
    record.add_field(Field(tag="001", data="123456"))
    return record


def test_save_source_record(caplog, folio_record, marc_record, mapper_base):
    record_type = FOLIONamespaces.instances
    folio_client = Mock(spec=FolioClient)
    # Mock the folio_auth object to handle gateway_url property
    folio_client.folio_auth = Mock()
    folio_client.folio_auth.gateway_url = "https://folio-snapshot.dev.folio.org"
    # Make gateway_url a property that returns the value from folio_auth
    type(folio_client).gateway_url = property(lambda self: self.folio_auth.gateway_url)
    legacy_ids = ["legacy_id_1", "legacy_id_2"]
    suppress = False
    srs_records = []

    with io.StringIO() as srs_records_file:
        mapper_base.save_source_record(
            srs_records_file,
            record_type,
            folio_client,
            marc_record,
            folio_record,
            legacy_ids,
            suppress,
        )
        srs_records_file.seek(0)
        srs_records.extend(srs_records_file.readlines())

    log_messages = [call.message for call in caplog.records]
    assert not any(
        message.startswith("Something is wrong with the marc record's leader:")
        for message in log_messages
    )

    assert len(srs_records) == 1
    assert srs_records[0].startswith('{"id": "')
    assert srs_records[0].endswith('"}\n')


schema_ea = {
    "properties":{
        "electronicAccess": {
            "description": "List of electronic access items",
            "type": "array",
            "items": {
                "type": "object",
                "$schema": "http://json-schema.org/draft-04/schema#",
                "description": "Electronic access item",
                "javaType": "org.folio.rest.jaxrs.model.ElectronicAccessItem",
                "additionalProperties": False,
                "properties": {
                    "uri": {
                        "type": "string",
                        "description": "uniform resource identifier (URI) is a string of characters designed for unambiguous identification of resources"
                    },
                    "linkText": {
                        "type": "string",
                        "description": "the value of the MARC tag field 856 2nd indicator, where the values are: no information provided, resource, version of resource, related resource, no display constant generated"
                    },
                    "materialsSpecification": {
                        "type": "string",
                        "description": "materials specified is used to specify to what portion or aspect of the resource the electronic location and access information applies (e.g. a portion or subset of the item is electronic, or a related electronic resource is being linked to the record)"
                    },
                    "publicNote": {
                        "type": "string",
                        "description": "URL public note to be displayed in the discovery"
                    },
                    "relationshipId": {
                        "type": "string",
                        "description": "relationship between the electronic resource at the location identified and the item described in the record as a whole"
                    }
                },
                "required": [
                    "uri"
                ]
            }
        }
    }
}

default_rule_856 = {
    "856": [
        {
            "entity": [
                {
                    "rules": [
                        {
                            "conditions": [
                                {
                                    "type": "set_electronic_access_relations_id"
                                }
                            ]
                        }
                    ],
                    "target": "electronicAccess.relationshipId",
                    "subfield": [
                        "3",
                        "y",
                        "u",
                        "z"
                    ],
                    "description": "Relationship between the electronic resource at the location identified and the item described in the record as a whole",
                    "applyRulesOnConcatenatedData": True
                },
                {
                    "rules": [
                        {
                            "conditions": [
                                {
                                    "type": "remove_ending_punc, trim"
                                }
                            ]
                        }
                    ],
                    "target": "electronicAccess.uri",
                    "subfield": [
                        "u"
                    ],
                    "description": "URI"
                },
                {
                    "rules": [
                        {
                            "conditions": [
                                {
                                    "type": "remove_ending_punc, trim"
                                }
                            ]
                        }
                    ],
                    "target": "electronicAccess.linkText",
                    "subfield": [
                        "y"
                    ],
                    "description": "Link text"
                },
                {
                    "rules": [
                        {
                            "conditions": [
                                {
                                    "type": "remove_ending_punc, trim"
                                }
                            ]
                        }
                    ],
                    "target": "electronicAccess.materialsSpecification",
                    "subfield": [
                        "3"
                    ],
                    "description": "Materials Specified"
                },
                {
                    "rules": [
                        {
                            "conditions": [
                                {
                                    "type": "remove_ending_punc, trim"
                                }
                            ]
                        }
                    ],
                    "target": "electronicAccess.publicNote",
                    "subfield": [
                        "z"
                    ],
                    "description": "URL public note"
                }
            ]
        }
    ],
}


def test_handle_entity_mapping_with_856_uri(mapper_base):
    mapper = mapper_base
    mapper.mapping_rules = default_rule_856
    mapper.schema = schema_ea
    marc_field = Field(
        tag="856",
        indicators=Indicators(*["4", "0"]),
        subfields=[
            Subfield(code="u", value="http://example.com"),
            Subfield(code="y", value="Link Text"),
            Subfield(code="z", value="URL Public Note"),
        ],
    )
    legacy_ids = ["123456"]
    ea_record = {}
    mapper.handle_entity_mapping(marc_field, mapper.mapping_rules['856'][0], ea_record, legacy_ids)
    ea_record_tuples = list(ea_record.items())
    ea_record_tuples.sort(key=lambda x: x[0])
    compare_record = {
        "electronicAccess": [
            {
                "linkText": "Link Text",
                "publicNote": "URL Public Note",
                "relationshipId": "f5d0068e-6272-458e-8a81-b85e7b9a14aa",
                "uri": "http://example.com",
            }
        ]
    }
    compare_record_tuples = list(compare_record.items())
    compare_record_tuples.sort(key=lambda x: x[0])
    assert ea_record_tuples == compare_record_tuples


def test_handle_entity_mapping_with_856_without_uri(mapper_base, caplog):
    mapper = mapper_base
    mapper.mapping_rules = default_rule_856
    mapper.schema = schema_ea
    DATA_ISSUE_LVL_NUM = 26
    logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")
    marc_field = Field(
        tag="856",
        indicators=Indicators(*["4", "0"]),
        subfields=[
            Subfield(code="u", value=""),
            Subfield(code="y", value="Link Text"),
            Subfield(code="z", value="URL Public Note"),
        ],
    )
    folio_record = {}
    legacy_ids = []
    mapper.handle_entity_mapping = RulesMapperBase.handle_entity_mapping
    mapper.handle_entity_mapping(mapper, marc_field, mapper.mapping_rules['856'][0], folio_record, legacy_ids)
    assert "Missing one or more required property in entity" in caplog.text
    assert folio_record.get("electronicAccess", []) == []


def test_handle_entity_mapping_with_856_no_u(mapper_base, caplog):
    mapper = mapper_base
    mapper.mapping_rules = default_rule_856
    mapper.schema = schema_ea
    DATA_ISSUE_LVL_NUM = 26
    logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")
    marc_field = Field(
        tag="856",
        indicators=Indicators(*["4", "0"]),
        subfields=[],
    )
    folio_record = {}
    legacy_ids = []
    mapper.handle_entity_mapping = RulesMapperBase.handle_entity_mapping
    mapper.handle_entity_mapping(mapper, marc_field, mapper.mapping_rules['856'][0], folio_record, legacy_ids)
    assert "Missing one or more required property in entity" in caplog.text
    assert folio_record.get("electronicAccess", []) == []


def test_map_field_according_to_mapping_exception_logging(mapper_base, caplog):
    """Test that map_field_according_to_mapping logs error on exception."""
    from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
    from unittest.mock import MagicMock

    mapper = mapper_base
    marc_field = Field(
        tag="100",
        indicators=Indicators(*["1", " "]),
        subfields=[Subfield(code="a", value="Test Author")],
    )

    # Create a mapping that will trigger entity mapping
    entity_mapping = [{"entity": [], "target": "contributors"}]
    folio_record = {}
    legacy_ids = ["legacy-id-1"]

    # Mock handle_entity_mapping to raise a TransformationFieldMappingError
    original_method = mapper.handle_entity_mapping
    mapping_error = TransformationFieldMappingError("legacy-id-1", "Test error", "test data")
    mapper.handle_entity_mapping = MagicMock(side_effect=mapping_error)

    with caplog.at_level(26):  # DATA_ISSUES level
        mapper.map_field_according_to_mapping(marc_field, entity_mapping, folio_record, legacy_ids)

    # Verify that the error was logged (log_it was called on the exception)
    assert "FIELD MAPPING FAILED" in caplog.text
    assert "legacy-id-1" in caplog.text
    assert "Test error" in caplog.text

    # Restore
    mapper.handle_entity_mapping = original_method


def test_set_005_as_updated_date_missing_005():
    """005 field absent should not modify the record."""
    record = Record()
    instance = {
        "metadata": {
            "createdDate": "2024-01-01T00:00:00",
            "updatedDate": "2024-01-01T00:00:00",
        }
    }
    RulesMapperBase.set_005_as_updated_date(record, instance, "some_id")
    assert instance["metadata"]["updatedDate"] == "2024-01-01T00:00:00"


def test_set_005_as_updated_date_empty_data():
    """005 field with empty data should not modify the record."""
    record = Record()
    record.add_field(Field(tag="005", data=""))
    instance = {
        "metadata": {
            "createdDate": "2024-01-01T00:00:00",
            "updatedDate": "2024-01-01T00:00:00",
        }
    }
    RulesMapperBase.set_005_as_updated_date(record, instance, "some_id")
    assert instance["metadata"]["updatedDate"] == "2024-01-01T00:00:00"


def test_use_008_for_dates_missing_008():
    """008 field absent should not modify the record."""
    record = Record()
    instance = {
        "title": "some title",
        "metadata": {
            "createdDate": "2024-01-01T00:00:00",
            "updatedDate": "2024-01-01T00:00:00",
        },
    }
    RulesMapperBase.use_008_for_dates(record, instance, "some_id")
    assert "catalogedDate" not in instance


def test_use_008_for_dates_empty_data():
    """008 field with empty data should not modify the record."""
    record = Record()
    record.add_field(Field(tag="008", data=""))
    instance = {
        "title": "some title",
        "metadata": {
            "createdDate": "2024-01-01T00:00:00",
            "updatedDate": "2024-01-01T00:00:00",
        },
    }
    RulesMapperBase.use_008_for_dates(record, instance, "some_id")
    assert "catalogedDate" not in instance


def test_apply_rule_raises_when_conditions_none(mapper_base):
    """apply_rule should raise TransformationProcessError when conditions is None."""
    mapper_base.conditions = None
    with pytest.raises(TransformationProcessError, match="conditions not initialized"):
        mapper_base.apply_rule("legacy-1", "value", ["trim"], None, {})


def test_create_srs_id_unknown_record_type(mapper_base):
    """create_srs_id should raise TransformationProcessError for unknown record type."""
    with pytest.raises(TransformationProcessError, match="Unknown SRS record type"):
        mapper_base.create_srs_id(FOLIONamespaces.items, "id_1")


def test_get_schema_property_found(mapper_base):
    """_get_schema_property returns the property when it exists at current level."""
    sc_prop = {"title": {"type": "string"}}
    result = mapper_base._get_schema_property(sc_prop, None, "title", "title")
    assert result == {"type": "string"}


def test_get_schema_property_from_parent(mapper_base):
    """_get_schema_property descends into schema_parent items when target not at top level."""
    sc_prop = {}
    schema_parent = {"items": {"properties": {"uri": {"type": "string"}}}}
    result = mapper_base._get_schema_property(sc_prop, schema_parent, "uri", "ea.uri")
    assert result == {"type": "string"}


def test_get_schema_property_raises_without_parent(mapper_base):
    """_get_schema_property raises when target not found and schema_parent is None."""
    sc_prop = {}
    with pytest.raises(TransformationProcessError, match="Schema parent not set"):
        mapper_base._get_schema_property(sc_prop, None, "missing", "missing")


def test_initialize_nested_target_array_of_strings(mapper_base):
    rec = {}
    sc_prop = {"type": "array", "items": {"type": "string"}}
    mapper_base._initialize_nested_target(rec, "tags", sc_prop, "tags")
    assert rec["tags"] == []


def test_initialize_nested_target_array_of_objects(mapper_base):
    rec = {}
    sc_prop = {"type": "array", "items": {"type": "object", "properties": {}}}
    mapper_base._initialize_nested_target(rec, "identifiers", sc_prop, "identifiers")
    assert rec["identifiers"] == [{}]


def test_initialize_nested_target_raises_on_unexpected_type(mapper_base):
    mapper_base.schema = {"properties": {"foo": {"type": "integer"}}}
    rec = {}
    sc_prop = {"type": "integer"}
    with pytest.raises(TransformationProcessError, match="Edge"):
        mapper_base._initialize_nested_target(rec, "foo", sc_prop, "foo")


def test_should_append_array_object_true(mapper_base):
    rec = {"items": [{"a": "1", "b": "2"}]}
    sc_prop = {"type": "array", "items": {"type": "object", "properties": {"a": {}, "b": {}}}}
    assert mapper_base._should_append_array_object(rec, "items", sc_prop) is True


def test_should_append_array_object_false_incomplete(mapper_base):
    rec = {"items": [{"a": "1"}]}
    sc_prop = {"type": "array", "items": {"type": "object", "properties": {"a": {}, "b": {}}}}
    assert mapper_base._should_append_array_object(rec, "items", sc_prop) is False


def test_is_string_in_array_object(mapper_base):
    schema_parent = {"type": "array", "items": {"type": "object", "properties": {}}}
    sc_prop = {"type": "string"}
    assert mapper_base._is_string_in_array_object(schema_parent, sc_prop) is True


def test_is_string_in_array_object_no_parent(mapper_base):
    sc_prop = {"type": "string"}
    assert not mapper_base._is_string_in_array_object(None, sc_prop)


def test_set_nested_string_value_without_add_parent(mapper_base):
    rec = {"ea": [{}]}
    mapper_base._set_nested_string_value(rec, "ea", "uri", "http://example.com", add_parent=False)
    assert rec["ea"][-1]["uri"] == "http://example.com"


def test_set_nested_string_value_with_add_parent(mapper_base):
    rec = {"ea": [{"uri": "http://old.com"}]}
    mapper_base._set_nested_string_value(rec, "ea", "uri", "http://new.com", add_parent=True)
    assert len(rec["ea"]) == 2
    assert rec["ea"][-1]["uri"] == "http://new.com"


def test_add_value_to_target_nested(mapper_base):
    """Test the refactored add_value_to_target with a nested schema target."""
    mapper_base.schema = schema_ea
    rec = {}
    mapper_base.add_value_to_target(rec, "electronicAccess.uri", ["http://example.com"])
    assert rec == {"electronicAccess": [{"uri": "http://example.com"}]}


def test_add_value_to_target_nested_appends_second_property(mapper_base):
    """Second property on existing nested object is added in-place."""
    mapper_base.schema = schema_ea
    rec = {"electronicAccess": [{"uri": "http://example.com"}]}
    mapper_base.add_value_to_target(rec, "electronicAccess.linkText", ["Click here"])
    assert rec["electronicAccess"][-1]["linkText"] == "Click here"


def test_map_statistical_codes_with_non_record(mapper_base):
    """map_statistical_codes should skip MARC field mapping when legacy_record is not a Record."""
    mapper_base.task_configuration.statistical_code_mapping_fields = ["099$a"]
    folio_record = {}
    from folio_migration_tools.library_configuration import FileDefinition
    file_def = FileDefinition(file_name="test.mrc", suppressed=False)
    # Passing a dict instead of Record should not attempt MARC field mapping
    mapper_base.map_statistical_codes(folio_record, file_def, legacy_record={"not": "a record"})
    assert "statisticalCodeIds" not in folio_record


def test_map_statistical_codes_with_none(mapper_base):
    """map_statistical_codes should skip MARC field mapping when legacy_record is None."""
    mapper_base.task_configuration.statistical_code_mapping_fields = ["099$a"]
    folio_record = {}
    from folio_migration_tools.library_configuration import FileDefinition
    file_def = FileDefinition(file_name="test.mrc", suppressed=False)
    mapper_base.map_statistical_codes(folio_record, file_def, legacy_record=None)
    assert "statisticalCodeIds" not in folio_record
