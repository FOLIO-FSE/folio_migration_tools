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
from folio_migration_tools.test_infrastructure import mocked_classes


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
        {"folio_code": "arch", "STATCODE": "Codered"},
        {"folio_code": "audstream", "STATCODE": "*"},
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
    assert res == ""


def test_simple_get_prop_stat_codes(mapper: HoldingsMapper):
    legacy_holding = {"STATCODE": "Codered"}
    res = mapper.get_prop(legacy_holding, "statisticalCodeIds[0]", "id1", "")
    assert res == "b6b46869-f3c1-4370-b603-29774a1e42b1"


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
    suppressed_holdings = {"id": "12345", "instanceId": "12345", "permanentLocationId": "12345"}
    unsuppressed_holdings = {"id": "54321", "instanceId": "12345", "permanentLocationId": "12345"}
    mapper.perform_additional_mappings(suppressed_holdings, file_config)
    mapper.perform_additional_mappings(unsuppressed_holdings, file_config_2)
    assert suppressed_holdings["discoverySuppress"] is True
    assert unsuppressed_holdings["discoverySuppress"] is False
