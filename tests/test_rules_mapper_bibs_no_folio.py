import logging
from datetime import datetime
from unittest.mock import Mock

import pymarc
import pytest
from dateutil.parser import parse
from pymarc import Field
from pymarc import MARCReader
from pymarc import Record

from folio_migration_tools.report_blurbs import Blurbs

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from folio_migration_tools.test_infrastructure import mocked_classes

xpath_245 = "//marc:datafield[@tag='245']"
# flake8: noqa


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> BibsRulesMapper:
    folio = mocked_classes.mocked_folio_client()
    lib = LibraryConfiguration(
        okapi_url=folio.okapi_url,
        tenant_id=folio.tenant_id,
        okapi_username=folio.username,
        okapi_password=folio.password,
        folio_release=FolioRelease.morning_glory,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    conf = BibsTransformer.TaskConfiguration(
        name="test",
        migration_task_type="BibsTransformer",
        hrid_handling=HridHandling.default,
        files=[],
        ils_flavour=IlsFlavour.sierra,
        reset_hrid_settings=False,
    )
    mapper = BibsRulesMapper(folio, lib, conf)
    mapper.folio_client = folio
    mapper.migration_report = MigrationReport()
    return mapper


def test_handle_suppression_set_false(mapper):
    folio_instance = {}
    file_def = FileDefinition(file_name="", staff_suppressed=False, suppressed=False)
    mapper.handle_suppression(folio_instance, file_def)
    assert folio_instance.get("staffSuppress") is False
    assert folio_instance.get("discoverySuppress") is False
    assert (
        mapper.migration_report.report[Blurbs.Suppression[0]]["Suppressed from discovery = False"]
        == 1
    )
    assert mapper.migration_report.report[Blurbs.Suppression[0]]["Staff suppressed = False "] == 1


def test_handle_suppression_set_true(mapper):
    folio_instance = {}
    file_def = FileDefinition(file_name="", staff_suppressed=True, suppressed=True)
    mapper.handle_suppression(folio_instance, file_def)
    assert folio_instance.get("staffSuppress") is True
    assert folio_instance.get("discoverySuppress") is True
    assert (
        mapper.migration_report.report[Blurbs.Suppression[0]]["Suppressed from discovery = True"]
        == 1
    )
    assert mapper.migration_report.report[Blurbs.Suppression[0]]["Staff suppressed = True "] == 1


def test_get_folio_id_by_code_except(mapper, caplog):
    caplog.set_level(26)
    res = mapper.get_instance_format_id_by_code("legacy_id_99", "test_code_999")
    assert "Instance format Code not found in FOLIO" in caplog.text
    assert "test_code_999" in caplog.text
    assert "legacy_id_99" in caplog.text
    assert res == ""


def test_create_entity_empty_props(mapper: BibsRulesMapper):
    entity_mappings = [
        {
            "target": "contributors.authorityId",
            "subfield": ["9"],
            "description": "Authority ID that controlling the contributor",
            "applyRulesOnConcatenatedData": True,
        },
        {
            "rules": [
                {
                    "conditions": [
                        {
                            "type": "set_contributor_name_type_id",
                            "parameter": {"name": "Personal name"},
                        }
                    ]
                }
            ],
            "target": "contributors.contributorNameTypeId",
            "subfield": [],
            "description": "Type for Personal Name",
            "applyRulesOnConcatenatedData": True,
        },
        {
            "rules": [{"conditions": [{"type": "set_contributor_type_id"}]}],
            "target": "contributors.contributorTypeId",
            "subfield": ["4"],
            "description": "Type of contributor",
            "applyRulesOnConcatenatedData": True,
        },
        {
            "rules": [{"conditions": [{"type": "set_contributor_type_text"}]}],
            "target": "contributors.contributorTypeText",
            "subfield": ["e"],
            "description": "Contributor type free text",
            "applyRulesOnConcatenatedData": True,
        },
        {
            "rules": [{"value": "true", "conditions": []}],
            "target": "contributors.primary",
            "subfield": [],
            "description": "Primary contributor",
            "applyRulesOnConcatenatedData": True,
        },
        {
            "rules": [{"conditions": [{"type": "trim_period, trim"}]}],
            "target": "contributors.name",
            "subfield": [
                "a",
                "b",
                "c",
                "d",
                "f",
                "g",
                "j",
                "k",
                "l",
                "n",
                "p",
                "q",
                "t",
                "u",
            ],
            "description": "Personal Name",
            "applyRulesOnConcatenatedData": True,
        },
    ]
    marc_field = Field(
        tag="100",
        indicators=["1", " "],
        subfields=["a", "De Geer, Jan,", "d", "1918-2007", "0", "280552"],
    )
    entity = mapper.create_entity(entity_mappings, marc_field, "contributors", "apa")
    assert "authorityId" not in entity


def test_get_instance_format_ids_no_rda(mapper, caplog):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "", "b", ""]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "", "b", ""]))
    res = mapper.get_instance_format_ids(record, "legacy_id_99")
    assert not any(res)


def test_get_instance_format_ids_empty_values_are_ignored(mapper, caplog):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "", "b", "", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "", "b", "", "2", "rdacarrier"]))
    res = mapper.get_instance_format_ids(record, "legacy_id_99")
    assert not any(res)


def test_get_instance_format_ids_three_digit_values_are_ignored(mapper, caplog):
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(tag="337", subfields=["a", "aaa", "b", "aaa", "2", "rdacarrier"])
    )
    record.add_field(
        pymarc.Field(tag="338", subfields=["a", "aaa", "b", "aaa", "2", "rdacarrier"])
    )
    res = BibsRulesMapper.get_instance_format_ids(mapper, record, "legacy_id_99")
    assert not any(res)


def test_get_instance_format_ids_338b_is_mapped(mapper: BibsRulesMapper, caplog):
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(tag="337", subfields=["a", "ignored", "b", "ignored", "2", "rdacarrier"])
    )
    record.add_field(
        pymarc.Field(tag="338", subfields=["a", "ignored", "b", "sb", "2", "rdacarrier"])
    )
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert any(res)


def test_get_instance_format_ids_one_338_two_337(mapper, caplog):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "audio", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="337", subfields=["a", "audio", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "audio belt", "2", "rdacarrier"]))
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert len(res) == 1


def test_get_instance_format_ids_two_338_two_337(mapper):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "audio", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="337", subfields=["a", "audio", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "audio belt", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "audio belt", "2", "rdacarrier"]))
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert len(res) == 2


def test_get_instance_format_ids_two_338a_one_337(mapper: BibsRulesMapper):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "audio", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "audio belt", "2", "rdacarrier"]))
    record.add_field(
        pymarc.Field(tag="338", subfields=["a", "audio belt", "b", "ab", "2", "rdacarrier"])
    )
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert len(res) == 2


def test_get_instance_format_ids_338a_is_mapped(mapper):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "audio", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "audio belt", "2", "rdacarrier"]))
    mocked_mapper = Mock(spec=BibsRulesMapper)
    mocked_mapper.migration_report = MigrationReport()
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert any(iter(res))


def test_f338_source_is_rda_carrier(mapper):
    field = pymarc.Field(tag="338", subfields=["a", "", "b", ""])
    res = mapper.f338_source_is_rda_carrier(field)
    assert not res


def test_f338_source_is_rda_carrier_2(mapper):
    field = pymarc.Field(tag="338", subfields=["a", "", "b", "", "2", " "])
    res = mapper.f338_source_is_rda_carrier(field)
    assert not res


def test_f338_source_is_rda_carrier_3(mapper):
    field = pymarc.Field(tag="338", subfields=["a", "", "b", "", "2", "rdacarrier"])
    res = mapper.f338_source_is_rda_carrier(field)
    assert res


def test_f338_source_is_rda_carrier_4(mapper):
    field = pymarc.Field(tag="338", subfields=["a", "", "b", "", "2", " rdacarrier"])
    res = mapper.f338_source_is_rda_carrier(field)
    assert res


def test_get_folio_id_by_name_except(mapper, caplog):
    caplog.set_level(26)
    res = mapper.get_instance_format_id_by_name("test_failed", "name", "legacy_id_99")
    assert "Unsuccessful matching on 337" in caplog.text
    assert "test_failed -- name" in caplog.text
    assert "legacy_id_99" in caplog.text
    assert res == ""


def test_get_folio_id_by_name(mapper, caplog):
    caplog.set_level(26)
    res = mapper.get_instance_format_id_by_name("audio", "audio belt", "legacy_id_99")
    assert not caplog.text
    assert (
        "Successful matching on 337$a & 338$a - audio -- audio belt->audio -- audio belt"
        in mapper.migration_report.report[Blurbs.InstanceFormat[0]]
    )
    assert res == "0d9b1c3d-2d13-4f18-9472-cc1b91bf1752"


def test_get_get_langs(mapper: BibsRulesMapper, caplog):
    langs = BibsRulesMapper.fetch_language_codes(mapper)
    assert any(langs)


def test_handle_leader_05(mapper, caplog):
    path = "./tests/test_data/with_control_caracther_and_corrupt_ldr05.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        assert record.leader[5] == "s"
        BibsRulesMapper.handle_leader_05(mapper, record, ["legacy id"])
        assert record.leader[5] == "c"
        assert (
            "Original value: s" in mapper.migration_report.report["Record status (leader pos 5)"]
        )
        assert "Changed s to c" in mapper.migration_report.report["Record status (leader pos 5)"]


def test_fieldReplacementBy3Digits(mapper: BibsRulesMapper, caplog):
    path = "./tests/test_data/diacritics/test-880.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        res = mapper.parse_record(
            record, FileDefinition(file_name="", suppressed=False, staff_suppressed=False), ["ii"]
        )
        assert "宝塚歌劇団" in [s["value"] for s in res["subjects"]]
        assert "[東京宝塚劇場公演パンフレット. ]" in res["alternativeTitles"][0]["alternativeTitle"]
        assert "1. 7月星組公演.  淀君, シャンソン・ダムール (1959)" in res["notes"][2]["note"]
        assert "宝塚" in res["publication"][1]["place"]
        assert "Records without $6" in mapper.migration_report.report["880 mappings"]


def test_parse_cataloged_date():
    cat_dates_and_expected_results = [
        ["06-23-93", "1993-06-23"],
        ["23/6-93", "1993-06-23"],
        ["06-06-06", "2006-06-06"],
    ]
    for cd in cat_dates_and_expected_results:
        parsed_date = parse(cd[0], fuzzy=True)
        assert str(parsed_date.date()) == cd[1]


@pytest.mark.skip(reason="Need to validate the entity before creating it")
def test_required_properties_electronic_access_missing_u(mapper: BibsRulesMapper, caplog):
    with pytest.raises(TransformationFieldMappingError):
        bad_856 = Field(
            tag="856", indicators=["0", "0"], subfields=["y", "URL to some fancy place"]
        )
        mapping_856 = mapper.mappings["856"][0]
        folio_record: dict = {}
        mapper.handle_entity_mapping(bad_856, mapping_856, folio_record, ["reqprop_entity_1"])


@pytest.mark.skip(reason="Need to validate the entity before creating it")
def test_required_properties_electronic_access_empty_u(mapper: BibsRulesMapper, caplog):
    with pytest.raises(TransformationFieldMappingError):
        bad_856 = Field(
            tag="856", indicators=["0", "0"], subfields=["u", "", "y", "URL to some fancy place"]
        )
        mapping_856 = mapper.mappings["856"][0]
        folio_record: dict = {}
        mapper.handle_entity_mapping(bad_856, mapping_856, folio_record, ["reqprop_entity_1"])


@pytest.mark.skip(reason="Need to validate the entity before creating it")
def test_required_properties_classification_empty_a(mapper: BibsRulesMapper, caplog):
    with pytest.raises(TransformationFieldMappingError):
        bad_082 = Field(tag="082", indicators=["0", "0"], subfields=["a", ""])
        mapping_082 = mapper.mappings["082"][0]
        folio_record: dict = {}
        mapper.handle_entity_mapping(bad_082, mapping_082, folio_record, ["reqprop_entity_1"])


def test_required_properties_classification_empty_a(mapper: BibsRulesMapper, caplog):
    """Temporary test for entity testing"""
    bad_082 = Field(tag="082", indicators=["0", "0"], subfields=["a", ""])
    mapping_082 = mapper.mappings["082"][0]
    folio_record: dict = {}
    mapper.handle_entity_mapping(bad_082, mapping_082, folio_record, ["reqprop_entity_1"])
    assert folio_record == {}


@pytest.mark.skip(reason="Need to validate the entity before creating it")
def test_required_properties_classification_missing_a(mapper: BibsRulesMapper, caplog):
    with pytest.raises(TransformationFieldMappingError):
        bad_082 = Field(tag="082", indicators=["0", "0"], subfields=["z", "garbage field"])
        mapping_082 = mapper.mappings["082"][0]
        folio_record: dict = {}
        mapper.handle_entity_mapping(bad_082, mapping_082, folio_record, ["reqprop_entity_1"])
