import datetime
import io
import json
import logging
import logging.handlers
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
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.migration_tasks.migration_task_base import MarcTaskConfigurationBase
from folio_migration_tools.test_infrastructure import mocked_classes

# flake8: noqa: E501

@pytest.fixture
def folio_client():
    fc = mocked_classes.mocked_folio_client()
    fc.gateway_url = "https://folio-snapshot.dev.folio.org"
    fc.tenant_id = "diku"
    fc.folio_username = "diku_admin"
    fc.folio_password = "admin"
    fc.get_from_github = FolioClient.get_from_github
    fc.get_latest_from_github = FolioClient.get_latest_from_github
    fc.get_module_version = FolioClient.get_module_version
    fc.get_holdings_schema = FolioClient.get_holdings_schema
    fc.get_instance_json_schema = FolioClient.get_instance_json_schema
    reference_data = list(Path(__file__).parent.joinpath("test_data/reference_data").glob("*.json"))
    for ref_data in reference_data:
        with open(ref_data, "r") as f:
            setattr(fc, ref_data.stem, json.load(f))
    return fc


@pytest.fixture
def mapper_base(folio_client):
    mapper_library_configuration = LibraryConfiguration(
        **{
            "okapi_url": "https://folio-snapshot.dev.folio.org",
            "tenant_id": "diku",
            "okapi_username": "diku_admin",
            "okapi_password": "admin",
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
    mapper = RulesMapperBase(folio_client, mapper_library_configuration, mapper_task_configuration, [], {})
    mapper.conditions = Conditions(folio_client, mapper, "any", FolioRelease.ramsons, "Library of Congress classification")
    return mapper


def test_dedupe_recs():
    my_dict = {"my_arr": [{"a": "b"}, {"a": "b"}, {"c": "d"}]}
    RulesMapperBase.dedupe_rec(my_dict)
    assert my_dict != {"my_arr": [{"a": "b"}, {"a": "b"}, {"c": "d"}]}
    assert my_dict == {"my_arr": [{"a": "b"}, {"c": "d"}]}


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
        indicators=["0", "1"],
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
            indicators=["0", "1"],
            subfields=[],
        )
        assert marc_field.get_subfields("j", "e")[0] == "puppeteer"


def test_remove_subfields():
    marc_field = Field(
        tag="338",
        indicators=["0", "1"],
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


def test_add_entity_to_record():
    entity = {"id": "id", "type": "type"}
    rec = {}
    latest_schema = FolioClient.get_latest_from_github(
        "folio-org", "mod-inventory-storage", "ramls/instance.json"
    )
    RulesMapperBase.add_entity_to_record(entity, "identifiers", rec, latest_schema)
    assert rec == {"identifiers": [{"id": "id", "type": "type"}]}


def test_weirdness():
    path = "./tests/test_data/two020a.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record1 = None
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
        record1 = None
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
        record: Record = None
        record = next(reader)
        l1 = record.leader
        record.leader = Leader(f"{record.leader[:-4]}4500")
        assert l1 != record.leader
        assert str(record.leader).endswith("4500")
        assert len(str(record.leader)) == 24


def test_create_srs_uuid(mapper_base):
    mapper_base.folio_client.gateway_url = "some_url"
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
    folio_client.gateway_url = "https://folio-snapshot.dev.folio.org"
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
        indicators=["4", "0"],
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
        indicators=["4", "0"],
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
        indicators=["4", "0"],
        subfields=[],
    )
    folio_record = {}
    legacy_ids = []
    mapper.handle_entity_mapping = RulesMapperBase.handle_entity_mapping
    mapper.handle_entity_mapping(mapper, marc_field, mapper.mapping_rules['856'][0], folio_record, legacy_ids)
    assert "Missing one or more required property in entity" in caplog.text
    assert folio_record.get("electronicAccess", []) == []
