import json
import logging
import re
import unittest
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch
from uuid import uuid4

import pymarc
import pytest
from folioclient import FolioClient
from lxml import etree
from pymarc import MARCReader
from pymarc import Record

from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.test_infrastructure.mocked_classes import mocked_folio_client

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapper_base import MapperBase
from folio_migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from folio_migration_tools.test_infrastructure import mocked_classes

xpath_245 = "//marc:datafield[@tag='245']"
# flake8: noqa


@pytest.fixture(scope="module")
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
    BibsRulesMapper.__init__ = MagicMock(name="__init__", return_value=None)
    BibsRulesMapper.get_instance_schema = MagicMock(name="get_instance_schema")
    Conditions.setup_reference_data_for_all = MagicMock(name="setup_reference_data_for_all")
    Conditions.setup_reference_data_for_bibs = MagicMock(name="setup_reference_data_for_bibs")
    mapper = BibsRulesMapper(folio, lib, conf)
    mapper.folio = folio
    mapper.migration_report = MigrationReport()
    return mapper


def test_handle_suppression_set_false(mapper):
    folio_instance = {}
    file_def = FileDefinition(file_name="", staff_suppressed=False, suppressed=False)
    mapper.handle_suppression(folio_instance, file_def)
    assert folio_instance.get("staffSuppress") is False
    assert folio_instance.get("discoverySuppress") is False
    assert (
        mapper.migration_report.report[Blurbs.GeneralStatistics[0]][
            "Suppressed from discovery = False"
        ]
        == 1
    )
    assert (
        mapper.migration_report.report[Blurbs.GeneralStatistics[0]]["Staff suppressed = False "]
        == 1
    )


def test_handle_suppression_set_true(mapper):
    folio_instance = {}
    file_def = FileDefinition(file_name="", staff_suppressed=True, suppressed=True)
    mapper.handle_suppression(folio_instance, file_def)
    assert folio_instance.get("staffSuppress") is True
    assert folio_instance.get("discoverySuppress") is True
    assert (
        mapper.migration_report.report[Blurbs.GeneralStatistics[0]][
            "Suppressed from discovery = True"
        ]
        == 1
    )
    assert (
        mapper.migration_report.report[Blurbs.GeneralStatistics[0]]["Staff suppressed = True "]
        == 1
    )


def test_get_folio_id_by_code_except(mapper, caplog):
    caplog.set_level(26)
    res = mapper.get_instance_format_id_by_code("legacy_id_99", "test_code_999")
    assert "Instance format Code not found in FOLIO" in caplog.text
    assert "test_code_999" in caplog.text
    assert "legacy_id_99" in caplog.text
    assert res == ""


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


def test_get_instance_format_ids_338b_is_mapped(mapper, caplog):
    record = pymarc.Record()
    record.add_field(
        pymarc.Field(tag="337", subfields=["a", "ignored", "b", "ignored", "2", "rdacarrier"])
    )
    record.add_field(
        pymarc.Field(tag="338", subfields=["a", "ignored", "b", "ab", "2", "rdacarrier"])
    )
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert any(res)


def test_get_instance_format_ids_one_338_two_337(mapper, caplog):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "test", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="337", subfields=["a", "test", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "name 2", "2", "rdacarrier"]))
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert len(res) == 1


def test_get_instance_format_ids_two_338_two_337(mapper):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "test", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="337", subfields=["a", "test", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "name 2", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "name 2", "2", "rdacarrier"]))
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert len(res) == 2


def test_get_instance_format_ids_two_338a_one_337(mapper: BibsRulesMapper):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "test", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "name", "2", "rdacarrier"]))
    record.add_field(
        pymarc.Field(tag="338", subfields=["a", "name 2", "b", "ab", "2", "rdacarrier"])
    )
    res = list(mapper.get_instance_format_ids(record, "legacy_id_99"))
    assert len(res) == 2


def test_get_instance_format_ids_338a_is_mapped(mapper):
    record = pymarc.Record()
    record.add_field(pymarc.Field(tag="337", subfields=["a", "test", "2", "rdacarrier"]))
    record.add_field(pymarc.Field(tag="338", subfields=["a", "name 2", "2", "rdacarrier"]))
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
    res = mapper.get_instance_format_id_by_name("test", "name", "legacy_id_99")
    assert not caplog.text
    assert (
        "Successful matching on 337$a & 338$a - test -- name->test -- name"
        in mapper.migration_report.report[Blurbs.InstanceFormat[0]]
    )
    assert res == "605e9527-4008-45e2-a78a-f6bfb027c43a"


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
