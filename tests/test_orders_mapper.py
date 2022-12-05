import logging

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.order_mapper import (
    CompositeOrderMapper,
)
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True

# Mock mapper object


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> CompositeOrderMapper:
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
        library_name="Order tester Library",
        log_level_debug=False,
        iteration_identifier="Test!",
        base_folder="/",
        multi_field_delimiter="^-^",
    )

    return CompositeOrderMapper(
        mock_folio_client,
        composite_order_map,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        lib_config,
        ""
    )


# Tests
def test_fetch_acq_schemas_from_github_happy_path():
    composite_order_schema = CompositeOrderMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-orders", "mod-orders", "composite_purchase_order"
    )

    assert composite_order_schema["$schema"]


def test_parse_record_mapping_file(mapper):

    folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(composite_order_map)

    assert folio_keys


def test_composite_order_mapping(mapper):

    composite_order, idx = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)

    assert composite_order["poNumber"] == "o123"


# Shared data and maps

data = {
    "order_number": "o123",
    "vendor": "EBSCO",
    "type": "one-time"
}

composite_order_map = {
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "order_number",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "poNumber",
            "legacy_field": "order_number",
            "value": "",
            "description": ""
        },
        {
            "folio_field": "vendor",
            "legacy_field": "vendor",
            "value": "",
            "description": ""
        },
        {
            "folio_field": "orderType",
            "legacy_field": "type",
            "value": "",
            "description": ""
        }
    ]
}
