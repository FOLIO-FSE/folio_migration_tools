from unittest.mock import Mock

import pytest

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from folio_migration_tools.custom_exceptions import TransformationProcessError
from pathlib import Path

from .test_infrastructure import mocked_classes


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> HoldingsMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    print("init")
    mock_folio = mocked_classes.mocked_folio_client()

    lib = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.ramsons,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    holdings_map = {
        "data": [
            {
                "folio_field": "_version",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "acquisitionFormat",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "acquisitionMethod",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "callNumber",
                "legacy_field": "CALLNUMBER",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "callNumberPrefix",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "Z30_REC_KEY",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "callNumberSuffix",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "callNumberTypeId",
                "legacy_field": "CALLNUMBERTYPE",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "copyNumber",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "digitizationPolicy",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "discoverySuppress",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].linkText",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].materialsSpecification",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].publicNote",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].relationshipId",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "electronicAccess[0].uri",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "formerIds[0]",
                "legacy_field": "Z30_REC_KEY",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatements[0].note",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatements[0].staffNote",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatements[0].statement",
                "legacy_field": "holdings_stmt",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForIndexes[0].note",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForIndexes[0].staffNote",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForIndexes[0].statement",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForSupplements[0].note",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForSupplements[0].staffNote",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsStatementsForSupplements[0].statement",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "holdingsTypeId",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {"folio_field": "hrid", "legacy_field": "Not mapped", "value": "", "description": ""},
            {"folio_field": "id", "legacy_field": "Not mapped", "value": "", "description": ""},
            {
                "folio_field": "illPolicyId",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "instanceId",
                "legacy_field": "bibnumber",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].holdingsNoteTypeId",
                "legacy_field": "holdings_note",
                "value": "f453de0f-8b54-4e99-9180-52932529e3a6",
                "description": "",
            },
            {
                "folio_field": "notes[0].note",
                "legacy_field": "holdings_note",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].staffOnly",
                "legacy_field": "holdings_note",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "numberOfItems",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "permanentLocationId",
                "legacy_field": "PERM_LOCATION",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "receiptStatus",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "receivingHistory.displayType",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "receivingHistory.entries[0].chronology",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "receivingHistory.entries[0].enumeration",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "receivingHistory.entries[0].publicDisplay",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "retentionPolicy",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "shelvingTitle",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "sourceId",
                "legacy_field": "Not mapped",
                "value": "",
                "description": "",
            },
            {"folio_field": "tags", "legacy_field": "", "value": "", "description": ""},
            {
                "folio_field": "temporaryLocationId",
                "legacy_field": "TEMP_LOCATION",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "statisticalCodeIds[0]",
                "legacy_field": "STATCODE",
                "value": "",
                "description": "",
            },
        ]
    }
    location_map = [
        {"folio_code": "KU/CC/DI/P", "PERM_LOCATION": "infoOff"},
        {"folio_code": "E", "PERM_LOCATION": "*"},
    ]
    call_number_type_map = [
        {"folio_name": "LC Modified", "CALLNUMBERTYPE": "LCM"},
        {"folio_name": "Library of Congress classification", "CALLNUMBERTYPE": "*"},
    ]
    statistical_codes_map = [
        {"folio_code": "arch", "legacy_stat_code": "Codered"},
        {"folio_code": "arch", "legacy_stat_code": "arch"},
        {"folio_code": "audstream", "legacy_stat_code": "audstream"},
        {"folio_code": "audstream", "legacy_stat_code": "*"},
    ]
    instance_id_map = {"b1": ["b1", "88009a08-5a2e-49e1-a3dd-d44c44d21b76"]}
    mocked_config = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mocked_config.look_up_instructor = False
    return HoldingsMapper(
        mock_folio,
        holdings_map,
        location_map,
        call_number_type_map,
        instance_id_map,
        lib,
        mocked_config,
        statistical_codes_map,
    )


def test_simple_get_prop_instance_id(mapper: HoldingsMapper):
    legacy_holding = {"bibnumber": "b1"}
    res = mapper.get_prop(legacy_holding, "instanceId", "id1", "")
    assert res == ["88009a08-5a2e-49e1-a3dd-d44c44d21b76"]


def test_simple_get_prop_stat_codes_empty_if_empty(mapper: HoldingsMapper):
    legacy_holding = {"STATCODE": ""}
    res = mapper.get_prop(legacy_holding, "statisticalCodeIds[0]", "id1", "")
    assert res == ""


def test_simple_get_prop_stat_codes_empty_if_not_mapped(mapper: HoldingsMapper):
    legacy_holding = {"STATCODE": "CodeBlue"}  # CodeBlue is not in map
    res = mapper.get_prop(legacy_holding, "statisticalCodeIds[0]", "id1", "")
    assert res == "CodeBlue"


def test_simple_get_prop_stat_codes(mapper: HoldingsMapper):
    legacy_holding = {"STATCODE": "Codered"}
    res = mapper.get_prop(legacy_holding, "statisticalCodeIds[0]", "id1", "")
    assert res == "Codered"
    # assert res == "b6b46869-f3c1-4370-b603-29774a1e42b1"


def test_simple_get_prop_call_number_type(mapper: HoldingsMapper):
    legacy_holding = {"CALLNUMBERTYPE": "LCM"}
    res = mapper.get_prop(legacy_holding, "callNumberTypeId", "id1", "")
    assert res == "512173a7-bd09-490e-b773-17d83f2b63fe"


def test_simple_get_prop_call_number(mapper: HoldingsMapper):
    legacy_holding = {"CALLNUMBER": "V.01"}
    res = mapper.get_prop(legacy_holding, "callNumber", "id1", "")
    assert res == "V.01"


def test_simple_get_prop_perm_location(mapper: HoldingsMapper):
    legacy_holding = {"PERM_LOCATION": "electronic"}
    res = mapper.get_prop(legacy_holding, "permanentLocationId", "id1", "")
    assert res == "184aae84-a5bf-4c6a-85ba-4a7c73026cd5"


def test_simple_get_prop_regular_value(mapper: HoldingsMapper):
    legacy_holding = {"Z30_REC_KEY": "old_id"}
    res = mapper.get_prop(legacy_holding, "formerIds[0]", "old_id", "")
    assert res == "old_id"


def test_get_legacy_bib_ids_one_id():
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.migration_report = MigrationReport()
    res = HoldingsMapper.get_legacy_bib_ids(mocked_mapper, "id 1", "LegacyId")
    assert res == ["id 1"]


def test_get_legacy_bib_ids_two_ids():
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.migration_report = MigrationReport()
    res = HoldingsMapper.get_legacy_bib_ids(mocked_mapper, '["id 1","id 2"]', "LegacyId")
    assert res == ["id 1", "id 2"]


def test_get_legacy_bib_ids_two_exception():
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.migration_report = MigrationReport()
    with pytest.raises(TransformationRecordFailedError):
        HoldingsMapper.get_legacy_bib_ids(mocked_mapper, '[id 1,id 2"]', "LegacyId")


def test_get_call_number_1():
    call_number = "['Merrymount']"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == "Merrymount"


def test_get_call_number_2():
    call_number = "[English Poetry]WELB"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_get_call_number_3():
    call_number = "PR2754 .F4 [13]"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_get_call_number_4():
    call_number = "PR2754 .Y8 [13]"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_get_call_number_5():
    call_number = "[English Poetry]WELB"
    mocked_mapper = Mock(spec=HoldingsMapper)
    res = HoldingsMapper.get_call_number(mocked_mapper, call_number)
    assert res == call_number


def test_perform_additional_mappings(mapper: HoldingsMapper):
    file_config = Mock(spec=FileDefinition)
    file_config_2 = Mock(spec=FileDefinition)
    file_config.discovery_suppressed = True
    file_config_2.discovery_suppressed = False
    file_config.statistical_code = ""
    file_config_2.statistical_code = ""
    suppressed_holdings = {"id": "12345", "instanceId": "12345", "permanentLocationId": "12345"}
    unsuppressed_holdings = {"id": "54321", "instanceId": "12345", "permanentLocationId": "12345"}
    mapper.perform_additional_mappings("1", suppressed_holdings, file_config)
    mapper.perform_additional_mappings("2", unsuppressed_holdings, file_config_2)
    assert suppressed_holdings["discoverySuppress"] is True
    assert unsuppressed_holdings["discoverySuppress"] is False


def test_perform_additional_mappings_with_stat_codes(mapper: HoldingsMapper, caplog):
    file_config = Mock(spec=FileDefinition)
    file_config.statistical_code = "arch<delimiter>audstream"
    file_config.discovery_suppressed = True
    file_config_2 = Mock(spec=FileDefinition)
    file_config_2.statistical_code = "arch"
    file_config_2.discovery_suppressed = False
    suppressed_holdings = {
        "id": "12345",
        "instanceId": "12345",
        "permanentLocationId": "12345",
    }
    unsuppressed_holdings = {
        "id": "54321",
        "instanceId": "12345",
        "permanentLocationId": "12345",
    }
    caplog.set_level("DEBUG")
    mapper.perform_additional_mappings("1", suppressed_holdings, file_config)
    mapper.perform_additional_mappings("2", unsuppressed_holdings, file_config_2)
    # mapper.map_statistical_codes(suppressed_holdings, file_config)
    # mapper.map_statistical_codes(unsuppressed_holdings, file_config_2)
    # assert suppressed_holdings["statisticalCodeIds"] == ["arch", "audstream"]
    # assert unsuppressed_holdings["statisticalCodeIds"] == ["arch"]
    # mapper.map_statistical_code_ids("1", suppressed_holdings)
    # mapper.map_statistical_code_ids("2", unsuppressed_holdings)
    assert "b6b46869-f3c1-4370-b603-29774a1e42b1" in suppressed_holdings["statisticalCodeIds"]
    assert "e10796e0-a594-47b7-b748-3a81b69b3d9b" in suppressed_holdings["statisticalCodeIds"]
    assert "b6b46869-f3c1-4370-b603-29774a1e42b1" in unsuppressed_holdings["statisticalCodeIds"]
    assert "e10796e0-a594-47b7-b748-3a81b69b3d9b" not in unsuppressed_holdings["statisticalCodeIds"]


def test_apply_default_call_number_type_when_call_number_present_and_no_type():
    """Test that default call number type is applied when call number exists but type doesn't."""
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Library of Congress classification"
    mocked_mapper.has_call_number_parts = HoldingsMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {"callNumber": "QA76.73", "instanceId": "inst-123", "permanentLocationId": "loc-1"}

    HoldingsMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert folio_rec["callNumberTypeId"] == "test-uuid-1234"
    assert "CallNumberTypeMapping" in mocked_mapper.migration_report.report


def test_apply_default_call_number_type_not_applied_when_type_already_exists():
    """Test that default call number type is NOT applied when callNumberTypeId already exists."""
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Library of Congress classification"
    mocked_mapper.has_call_number_parts = HoldingsMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {
        "callNumber": "QA76.73",
        "callNumberTypeId": "existing-type-uuid",
        "instanceId": "inst-123",
        "permanentLocationId": "loc-1",
    }

    HoldingsMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert folio_rec["callNumberTypeId"] == "existing-type-uuid"


def test_apply_default_call_number_type_not_applied_when_no_call_number():
    """Test that default call number type is NOT applied when no call number parts exist."""
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Library of Congress classification"
    mocked_mapper.has_call_number_parts = HoldingsMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {"instanceId": "inst-123", "permanentLocationId": "loc-1"}

    HoldingsMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert "callNumberTypeId" not in folio_rec


def test_apply_default_call_number_type_not_applied_when_no_default_configured():
    """Test that nothing happens when no default call number type is configured."""
    mocked_mapper = Mock(spec=HoldingsMapper)
    # Simulate no default_call_number_type_id attribute
    del mocked_mapper.default_call_number_type_id
    mocked_mapper.has_call_number_parts = HoldingsMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {"callNumber": "QA76.73", "instanceId": "inst-123", "permanentLocationId": "loc-1"}

    HoldingsMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert "callNumberTypeId" not in folio_rec


def test_apply_default_call_number_type_with_call_number_prefix():
    """Test that default is applied when callNumberPrefix exists (not just callNumber)."""
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Dewey Decimal classification"
    mocked_mapper.has_call_number_parts = HoldingsMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {
        "callNumberPrefix": "REF",
        "instanceId": "inst-123",
        "permanentLocationId": "loc-1",
    }

    HoldingsMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert folio_rec["callNumberTypeId"] == "test-uuid-1234"


def test_apply_default_call_number_type_not_applied_for_additional_call_numbers():
    """Test that default is NOT applied when only additionalCallNumbers exists.

    The has_call_number_parts method should exclude fields containing 'additional'
    to avoid applying call number type to additional call numbers which have their
    own type embedded in the structure.
    """
    mocked_mapper = Mock(spec=HoldingsMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Library of Congress classification"
    mocked_mapper.has_call_number_parts = HoldingsMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    # Record has only additionalCallNumbers, not a primary call number
    folio_rec = {
        "additionalCallNumbers": ["ABC123", "DEF456"],
        "instanceId": "inst-123",
        "permanentLocationId": "loc-1",
    }

    HoldingsMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert "callNumberTypeId" not in folio_rec


def test_mapper_init_with_default_call_number_type_no_map():
    """Test HoldingsMapper initialization with default call number type but no mapping file."""

    mock_folio = mocked_classes.mocked_folio_client()

    lib = LibraryConfiguration(
        okapi_url="okapi_url",
        tenant_id="tenant_id",
        okapi_username="username",
        okapi_password="password",
        folio_release=FolioRelease.ramsons,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="test",
        base_folder=Path("/"),
    )

    holdings_map = {
        "data": [
            {"folio_field": "legacyIdentifier", "legacy_field": "ID", "value": "", "description": ""},
            {"folio_field": "callNumber", "legacy_field": "CN", "value": "", "description": ""},
            {"folio_field": "permanentLocationId", "legacy_field": "LOC", "value": "", "description": ""},
            {"folio_field": "instanceId", "legacy_field": "BIB", "value": "", "description": ""},
        ]
    }
    location_map = [{"folio_code": "E", "LOC": "*"}]
    instance_id_map = {"b1": ["b1", "88009a08-5a2e-49e1-a3dd-d44c44d21b76"]}

    mocked_config = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mocked_config.look_up_instructor = False
    mocked_config.default_call_number_type_name = "Library of Congress classification"

    mapper = HoldingsMapper(
        mock_folio,
        holdings_map,
        location_map,
        None,  # No call number type map
        instance_id_map,
        lib,
        mocked_config,
        None,
    )

    # Should have resolved the default call number type ID
    assert hasattr(mapper, "default_call_number_type_id")
    assert mapper.default_call_number_type_id == "95467209-6d7b-468b-94df-0f5d7ad2747d"


def test_mapper_init_with_invalid_default_call_number_type():
    """Test that HoldingsMapper raises error when default call number type name is invalid."""

    mock_folio = mocked_classes.mocked_folio_client()

    lib = LibraryConfiguration(
        okapi_url="okapi_url",
        tenant_id="tenant_id",
        okapi_username="username",
        okapi_password="password",
        folio_release=FolioRelease.ramsons,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="test",
        base_folder=Path("/"),
    )

    holdings_map = {
        "data": [
            {"folio_field": "legacyIdentifier", "legacy_field": "ID", "value": "", "description": ""},
            {"folio_field": "permanentLocationId", "legacy_field": "LOC", "value": "", "description": ""},
            {"folio_field": "instanceId", "legacy_field": "BIB", "value": "", "description": ""},
        ]
    }
    location_map = [{"folio_code": "E", "LOC": "*"}]
    instance_id_map = {"b1": ["b1", "88009a08-5a2e-49e1-a3dd-d44c44d21b76"]}

    mocked_config = Mock(spec=HoldingsCsvTransformer.TaskConfiguration)
    mocked_config.look_up_instructor = False
    mocked_config.default_call_number_type_name = "Nonexistent Call Number Type"

    with pytest.raises(TransformationProcessError) as exc_info:
        HoldingsMapper(
            mock_folio,
            holdings_map,
            location_map,
            None,  # No call number type map
            instance_id_map,
            lib,
            mocked_config,
            None,
        )

    assert "Nonexistent Call Number Type" in str(exc_info.value)
    assert "not found in tenant" in str(exc_info.value)
