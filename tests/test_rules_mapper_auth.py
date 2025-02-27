import logging

import pytest
from pymarc import MARCReader, Record

from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
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
        folio_release=FolioRelease.ramsons,
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
        auth = mapper.parse_record(record, FileDefinition(file_name=""), ["ids"])[0]
        assert auth["personalName"] == "Ericsson, Leif KE, 1964-"
        assert auth["personalNameTitle"] == "Ericsson, Leif KE, 1964-"
        assert auth["id"] == "54ac1b25-aa36-566b-a688-030a745ae080"
        assert auth["naturalId"] == "363723"
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
        auth = mapper.parse_record(record, FileDefinition(file_name=""), ["ids"])[0]
        assert auth["naturalId"] == "n2008028538"
        assert auth["sourceFileId"] == "af045f2f-e851-4613-984c-4bc13430454a"
        assert "Yu, Tanling" in auth["sftPersonalName"]
        assert "于丹翎" in auth["sftPersonalName"]
        assert mapper.mapped_folio_fields["personalNameTitle"] == [1]
        assert mapper.mapped_folio_fields["identifiers"] == [1]
        assert mapper.mapped_folio_fields["identifiers.identifierTypeId"] == [1]
        assert mapper.mapped_folio_fields["personalName"] == [1]
        assert mapper.mapped_folio_fields["sftPersonalName"] == [1]


def test_invalid_ldr_17(mapper: AuthorityMapper, caplog):
    path = "./tests/test_data/auth_918643_invalid_ldr17.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        record: Record = None
        record = next(reader)
        assert record.leader[17] == " "
        _ = mapper.parse_record(record, FileDefinition(file_name=""), ["ids"])[0]
        assert record.leader[17] == "n"
