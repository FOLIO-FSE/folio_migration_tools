import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.item_mapper import ItemMapper
from folio_migration_tools.test_infrastructure import mocked_classes


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> ItemMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    print("init")
    mock_folio_client = mocked_classes.mocked_folio_client()

    lib_config = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.morning_glory,
        library_name="Item Tester Library",
        log_level_debug=False,
        iteration_identifier="Test!",
        base_folder="/",
        multi_field_delimiter="^-^",
    )

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

    return ItemMapper(
        mock_folio_client,
        item_map,
        material_type_map,
        loan_type_map,
        location_map,
        {},
        {"000000950000010": ["000000950000010", "9e840586-a641-5932-92ef-cfde8e84f9a1"]},
        {},
        {},
        {},
        {},
        lib_config,
    )


def test_item_mapping(mapper):
    data = {"barcode": "000000950000010", "note": "Check it out!", "lt": "ah", "mat": "oh"}

    item, idx = mapper.do_map(data, data["barcode"], FOLIONamespaces.items)
    assert item["barcode"] == "000000950000010"

    assert "mat" in mapper.mapped_legacy_fields


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
