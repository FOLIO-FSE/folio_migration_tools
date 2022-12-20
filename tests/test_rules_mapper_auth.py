import logging

import pytest
from pymarc import MARCReader
from pymarc import Record

from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.rules_mapper_authorities import (
    AuthorityMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.authority_transformer import (
    AuthorityTransformer,
)
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> AuthorityMapper:
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
    conf = AuthorityTransformer.TaskConfiguration(
        name="test",
        migration_task_type="AuthorityTransformer",
        hrid_handling=HridHandling.default,
        files=[],
        ils_flavour=IlsFlavour.tag001,
    )
    # BibsRulesMapper.__init__ = MagicMock(name="__init__", return_value=None)
    # BibsRulesMapper.get_instance_schema = MagicMock(name="get_instance_schema")
    # Conditions.setup_reference_data_for_all = MagicMock(name="setup_reference_data_for_all")
    # Conditions.setup_reference_data_for_bibs = MagicMock(name="setup_reference_data_for_bibs")
    mapper = AuthorityMapper(folio, lib, conf)
    mapper.folio_client = folio
    mapper.migration_report = MigrationReport()
    return mapper


def test_basic(mapper: AuthorityMapper, caplog):
    path = "./tests/test_data/auth_363723.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        auth = mapper.parse_record(record, FileDefinition(file_name=""), ["ids"])
        assert auth["personalName"] == "Ericsson, Leif KE, 1964-"
        assert auth["personalNameTitle"] == "Ericsson, Leif KE, 1964-"
        assert auth["id"] == "54ac1b25-aa36-566b-a688-030a745ae080"
        assert all(id["identifierTypeId"] and id["value"] for id in auth["identifiers"])
        assert len(auth["identifiers"]) == 2
        assert auth["source"] == "MARC"
        assert mapper.mapped_folio_fields["personalNameTitle"] == [1]
        assert mapper.mapped_folio_fields["personalName"] == [1]
        assert mapper.mapped_folio_fields["source"] == [1]
        assert mapper.mapped_folio_fields["identifiers.value"] == [1]
        assert mapper.mapped_folio_fields["identifiers.identifierTypeId"] == [1]


def test_saft(mapper: AuthorityMapper, caplog):
    path = "./tests/test_data/auth_918643.mrc"
    with open(path, "rb") as marc_file:
        mapper.mapped_folio_fields["sftPersonalName"] = [0]
        mapper.mapped_folio_fields["personalNameTitle"] = [0]
        mapper.mapped_folio_fields["personalName"] = [0]
        mapper.mapped_folio_fields["identifiers"] = [0]
        mapper.mapped_folio_fields["identifiers.identifierTypeId"] = [0]

        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        auth = mapper.parse_record(record, FileDefinition(file_name=""), ["ids"])
        assert "Yu, Tanling" in auth["sftPersonalName"]
        assert "于丹翎" in auth["sftPersonalName"]
        assert mapper.mapped_folio_fields["personalNameTitle"] == [1]
        assert mapper.mapped_folio_fields["identifiers"] == [1]
        assert mapper.mapped_folio_fields["identifiers.identifierTypeId"] == [1]
        assert mapper.mapped_folio_fields["personalName"] == [1]
        assert mapper.mapped_folio_fields["sftPersonalName"] == [1]