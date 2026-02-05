from unittest.mock import Mock, create_autospec

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.item_mapper import ItemMapper
from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer
from .test_infrastructure import mocked_classes


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> ItemMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    # print("init")
    mock_folio_client = mocked_classes.mocked_folio_client()

    lib_config = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.ramsons,
        library_name="Item Tester Library",
        log_level_debug=False,
        iteration_identifier="Test!",
        base_folder="/",
        multi_field_delimiter="^-^",
    )

    task_config = create_autospec(ItemsTransformer.TaskConfiguration)
    task_config.name = "Test Task"
    task_config.migration_task_type = "Test Task"
    task_config.files = [
        FileDefinition(file_type="items", file_path="items.json", discovery_suppressed=False)
    ]
    task_config.prevent_permanent_location_map_default = False
    task_config.default_call_number_type_name = ""

    loan_type_map = [
        {"lt": "cst", "folio_name": "Can circulate"},
        {"lt": "*", "folio_name": "Can circulate"},
    ]
    material_type_map = [
        {"mat": "cst", "folio_name": "book"},
        {"mat": "*", "folio_name": "book"},
    ]

    location_map = [
        {"folio_code": "KU/CC/DI/P", "PERM_LOCATION": "infoOff"},
        {"folio_code": "E", "PERM_LOCATION": "*"},
    ]
    statistical_codes_map = [
        {"folio_code": "arch", "legacy_stat_code": "Codered"},
        {"folio_code": "arch", "legacy_stat_code": "arch"},
        {"folio_code": "audstream", "legacy_stat_code": "audstream"},
        {"folio_code": "audstream", "legacy_stat_code": "*"},
    ]
    return ItemMapper(
        mock_folio_client,
        item_map,
        material_type_map,
        loan_type_map,
        location_map,
        {},
        {
            "000000950000010": ["000000950000010", "9e840586-a641-5932-92ef-cfde8e84f9a1"],
            "ABC000950000010": ["ABC000950000010", "9e840586-a641-5932-92ef-cfde8e84f9a2"],
            "abc000950000010": ["abc000950000010", "9e840586-a641-5932-92ef-cfde8e84f9a3"],
        },
        statistical_codes_map,
        {},
        {},
        {},
        lib_config,
        task_config,
    )


def test_item_mapping(mapper):
    data = {"barcode": "000000950000010", "note": "Check it out!", "lt": "ah", "mat": "oh"}

    item, idx = mapper.do_map(data, data["barcode"], FOLIONamespaces.items)
    assert item["barcode"] == "000000950000010"

    assert "mat" in mapper.mapped_legacy_fields


def test_item_mapper_duplicate_barcode(mapper):
    data = {"barcode": "ABC000950000010", "note": "Check it out!", "lt": "ah", "mat": "oh"}
    data2 = {"barcode": "abc000950000010", "note": "Check it out!", "lt": "ah", "mat": "oh"}

    item1, idx1 = mapper.do_map(data, data["barcode"], FOLIONamespaces.items)
    item2, idx2 = mapper.do_map(data2, data2["barcode"], FOLIONamespaces.items)

    assert item1["barcode"] == "ABC000950000010"
    assert item2["barcode"].startswith("abc000950000010-")


def test_perform_additional_mappings(mapper: ItemMapper):
    file_config = Mock(spec=FileDefinition)
    file_config_2 = Mock(spec=FileDefinition)
    file_config.discovery_suppressed = True
    file_config_2.discovery_suppressed = False
    file_config.statistical_code = ""
    file_config_2.statistical_code = ""
    suppressed_holdings = {"id": "12345", "holdingsId": "12345", "permanentLoanTypeId": "12345"}
    unsuppressed_holdings = {"id": "54321", "holdingsId": "12345", "permanentLoanTypeId": "12345"}
    mapper.perform_additional_mappings("1", suppressed_holdings, file_config)
    mapper.perform_additional_mappings("2", unsuppressed_holdings, file_config_2)
    assert suppressed_holdings["discoverySuppress"] is True
    assert unsuppressed_holdings["discoverySuppress"] is False

def test_get_prop_permanent_location(mapper: ItemMapper):
    item_data = {"barcode": "000000950000010", "note": "Check it out!", "lt": "ah", "mat": "oh", "PERM_LOCATION": "infoOff"}
    prop = mapper.get_prop(item_data, "permanentLocationId", item_data['barcode'], "")
    assert prop == 'b241764c-1466-4e1d-a028-1a3684a5da87'

def test_get_prop_permanent_location_no_default(mapper: ItemMapper):
    item_data = {"barcode": "000000950000010", "note": "Check it out!", "lt": "ah", "mat": "oh", "PERM_LOCATION": "invalidLoc"}
    mapper.task_configuration.prevent_permanent_location_map_default = True
    prop = mapper.get_prop(item_data, "permanentLocationId", item_data['barcode'], "")
    assert prop == ""

# Shared map

item_map = {
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "barcode",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "permanentLoanTypeId",
            "legacy_field": "lt",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "materialTypeId",
            "legacy_field": "mat",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "holdingsRecordId",
            "legacy_field": "barcode",
            "value": "",
            "description": "",
        },
        {"folio_field": "barcode", "legacy_field": "barcode", "value": "", "description": ""},
        {"folio_field": "status", "legacy_field": "code", "value": "Available", "description": ""},
        {
            "folio_field": "notes[0].itemNoteTypeId",
            "legacy_field": "Not mapped",
            "value": "7eaa4906-1aa8-46dc-b15d-fe5d96746929",
            "description": "WMS Item Acquired Date",
        },
        {
            "folio_field": "notes[0].note",
            "legacy_field": "note",
            "value": "",
            "description": "WMS Item Acquired Date",
        },
        {
            "folio_field": "notes[0].staffOnly",
            "legacy_field": "",
            "value": True,
            "description": "",
        },
    ]
}


def test_perform_additional_mappings_with_stat_codes(mapper: ItemMapper, caplog):
    file_config = Mock(spec=FileDefinition)
    file_config.statistical_code = "arch^-^audstream"
    file_config.discovery_suppressed = True
    file_config_2 = Mock(spec=FileDefinition)
    file_config_2.statistical_code = "arch"
    file_config_2.discovery_suppressed = False
    suppressed_item = {
        "id": "12345",
        "holdingsId": "12345",
        "permanentLocationId": "12345",
    }
    unsuppressed_item = {
        "id": "54321",
        "holdingsId": "12345",
        "permanentLocationId": "12345",
    }
    caplog.set_level("DEBUG")
    mapper.perform_additional_mappings("1", suppressed_item, file_config)
    mapper.perform_additional_mappings("2", unsuppressed_item, file_config_2)
    # mapper.map_statistical_codes(suppressed_holdings, file_config)
    # mapper.map_statistical_codes(unsuppressed_holdings, file_config_2)
    # assert suppressed_holdings["statisticalCodeIds"] == ["arch", "audstream"]
    # assert unsuppressed_holdings["statisticalCodeIds"] == ["arch"]
    # mapper.map_statistical_code_ids("1", suppressed_holdings)
    # mapper.map_statistical_code_ids("2", unsuppressed_holdings)
    assert "b6b46869-f3c1-4370-b603-29774a1e42b1" in suppressed_item["statisticalCodeIds"]
    assert "e10796e0-a594-47b7-b748-3a81b69b3d9b" in suppressed_item["statisticalCodeIds"]
    assert "b6b46869-f3c1-4370-b603-29774a1e42b1" in unsuppressed_item["statisticalCodeIds"]
    assert "e10796e0-a594-47b7-b748-3a81b69b3d9b" not in unsuppressed_item["statisticalCodeIds"]


from folio_migration_tools.migration_report import MigrationReport


def test_apply_default_call_number_type_when_call_number_present_and_no_type():
    """Test that default call number type is applied when item call number exists but type doesn't."""
    mocked_mapper = Mock(spec=ItemMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Library of Congress classification"
    mocked_mapper.has_call_number_parts = ItemMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {
        "itemLevelCallNumber": "QA76.73",
        "holdingsRecordId": "hold-123",
        "permanentLoanTypeId": "loan-1",
    }

    ItemMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert folio_rec["itemLevelCallNumberTypeId"] == "test-uuid-1234"
    assert "CallNumberTypeMapping" in mocked_mapper.migration_report.report


def test_apply_default_call_number_type_not_applied_when_type_already_exists():
    """Test that default call number type is NOT applied when itemLevelCallNumberTypeId already exists."""
    mocked_mapper = Mock(spec=ItemMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Library of Congress classification"
    mocked_mapper.has_call_number_parts = ItemMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {
        "itemLevelCallNumber": "QA76.73",
        "itemLevelCallNumberTypeId": "existing-type-uuid",
        "holdingsRecordId": "hold-123",
        "permanentLoanTypeId": "loan-1",
    }

    ItemMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert folio_rec["itemLevelCallNumberTypeId"] == "existing-type-uuid"


def test_apply_default_call_number_type_not_applied_when_no_call_number():
    """Test that default call number type is NOT applied when no item call number parts exist."""
    mocked_mapper = Mock(spec=ItemMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Library of Congress classification"
    mocked_mapper.has_call_number_parts = ItemMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {"holdingsRecordId": "hold-123", "permanentLoanTypeId": "loan-1"}

    ItemMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert "itemLevelCallNumberTypeId" not in folio_rec


def test_apply_default_call_number_type_not_applied_when_no_default_configured():
    """Test that nothing happens when no default call number type is configured."""
    mocked_mapper = Mock(spec=ItemMapper)
    # Simulate no default_call_number_type_id attribute
    del mocked_mapper.default_call_number_type_id
    mocked_mapper.has_call_number_parts = ItemMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {
        "itemLevelCallNumber": "QA76.73",
        "holdingsRecordId": "hold-123",
        "permanentLoanTypeId": "loan-1",
    }

    ItemMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert "itemLevelCallNumberTypeId" not in folio_rec


def test_apply_default_call_number_type_with_call_number_prefix():
    """Test that default is applied when itemLevelCallNumberPrefix exists."""
    mocked_mapper = Mock(spec=ItemMapper)
    mocked_mapper.default_call_number_type_id = "test-uuid-1234"
    mocked_mapper.task_configuration = Mock()
    mocked_mapper.task_configuration.default_call_number_type_name = "Dewey Decimal classification"
    mocked_mapper.has_call_number_parts = ItemMapper.has_call_number_parts
    mocked_mapper.migration_report = MigrationReport()

    folio_rec = {
        "itemLevelCallNumberPrefix": "REF",
        "holdingsRecordId": "hold-123",
        "permanentLoanTypeId": "loan-1",
    }

    ItemMapper.apply_default_call_number_type(mocked_mapper, folio_rec)

    assert folio_rec["itemLevelCallNumberTypeId"] == "test-uuid-1234"


def test_mapper_init_with_default_call_number_type_no_map():
    """Test ItemMapper initialization with default call number type but no mapping file."""
    from folio_migration_tools.custom_exceptions import TransformationProcessError
    from pathlib import Path

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

    mocked_config = create_autospec(ItemsTransformer.TaskConfiguration)
    mocked_config.default_call_number_type_name = "Library of Congress classification"

    loan_type_map = [{"lt": "*", "folio_name": "Can circulate"}]
    material_type_map = [{"mat": "*", "folio_name": "book"}]
    location_map = [{"folio_code": "E", "LOC": "*"}]

    mapper = ItemMapper(
        mock_folio,
        item_map,
        material_type_map,
        loan_type_map,
        location_map,
        None,  # No call number type map
        {"h1": ["h1", "holdings-uuid"]},
        None,
        None,
        None,
        None,
        lib,
        mocked_config,
    )

    # Should have resolved the default call number type ID
    assert hasattr(mapper, "default_call_number_type_id")
    assert mapper.default_call_number_type_id == "95467209-6d7b-468b-94df-0f5d7ad2747d"


def test_mapper_init_with_invalid_default_call_number_type():
    """Test that ItemMapper raises error when default call number type name is invalid."""
    from folio_migration_tools.custom_exceptions import TransformationProcessError
    from pathlib import Path

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

    mocked_config = create_autospec(ItemsTransformer.TaskConfiguration)
    mocked_config.default_call_number_type_name = "Nonexistent Call Number Type"

    loan_type_map = [{"lt": "*", "folio_name": "Can circulate"}]
    material_type_map = [{"mat": "*", "folio_name": "book"}]
    location_map = [{"folio_code": "E", "LOC": "*"}]

    with pytest.raises(TransformationProcessError) as exc_info:
        ItemMapper(
            mock_folio,
            item_map,
            material_type_map,
            loan_type_map,
            location_map,
            None,  # No call number type map
            {"h1": ["h1", "holdings-uuid"]},
            None,
            None,
            None,
            None,
            lib,
            mocked_config,
        )

    assert "Nonexistent Call Number Type" in str(exc_info.value)
    assert "not found in tenant" in str(exc_info.value)
