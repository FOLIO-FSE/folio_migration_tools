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
    instance_id_map = {"1": ["1", "ae1daef2-ddea-4d87-a434-3aa98ed3e687", "1"]}
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
                "description": "",
            },
            {"folio_field": "vendor", "legacy_field": "vendor", "value": "", "description": ""},
            {"folio_field": "orderType", "legacy_field": "type", "value": "", "description": ""},
            {
                "folio_field": "compositePoLines[0].id",
                "legacy_field": "order_number",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].titleOrPackage",
                "legacy_field": "TITLE",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].instanceId",
                "legacy_field": "bibnumber",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].acquisitionMethod",
                "legacy_field": "vendor",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].orderFormat",
                "legacy_field": "vendor",
                "value": "",
                "description": "",
                "rules": {"replaceValues": {"EBSCO": "Electronic Resource"}},
            },
            {
                "folio_field": "compositePoLines[0].cost.currency",
                "legacy_field": "",
                "value": "USD",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].source",
                "legacy_field": "",
                "value": "API",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].source",
                "legacy_field": "",
                "value": "API",
                "description": "",
            },
            {
                "folio_field": "notes[0].domain",
                "legacy_field": "Not mapped",
                "value": "orders",
                "description": "",
            },
            {
                "folio_field": "notes[0].typeId",
                "legacy_field": "",
                "value": "f5bba0d2-7732-4687-8311-a2cb0eaa12e5",
                "description": "",
            },
            {
                "folio_field": "notes[0].title",
                "legacy_field": "",
                "value": "A migrated note",
                "description": "",
            },
            {
                "folio_field": "notes[0].content",
                "legacy_field": "note1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[1].domain",
                "legacy_field": "Not mapped",
                "value": "orders",
                "description": "",
            },
            {
                "folio_field": "notes[1].typeId",
                "legacy_field": "",
                "value": "f5bba0d2-7732-4687-8311-a2cb0eaa12e5",
                "description": "",
            },
            {
                "folio_field": "notes[1].title",
                "legacy_field": "",
                "value": "A migrated note",
                "description": "",
            },
            {
                "folio_field": "notes[1].content",
                "legacy_field": "note2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0]",
                "legacy_field": "order_note",
                "value": "",
                "description": "",
            },
        ]
    }
    acg_method_map = [
        {"vendor": "EBSCO", "folio_value": "Purchase"},
        {"vendor": "*", "folio_value": "Purchase"},
    ]
    return CompositeOrderMapper(
        mock_folio_client,
        lib_config,
        composite_order_map,
        instance_id_map,
        acg_method_map,
        "",
        "",
        "",
        "",
        "",
        "",
    )


# Tests
@pytest.mark.slow
def test_fetch_acq_schemas_from_github_happy_path():
    composite_order_schema = CompositeOrderMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-orders", "mod-orders", "composite_purchase_order"
    )

    assert composite_order_schema["$schema"]
    assert composite_order_schema["properties"]["orderType"]["enum"] == ["One-Time", "Ongoing"]
    assert composite_order_schema["properties"]["compositePoLines"]["items"]["properties"][
        "receiptStatus"
    ]["enum"] == [
        "Awaiting Receipt",
        "Cancelled",
        "Fully Received",
        "Partially Received",
        "Pending",
        "Receipt Not Required",
        "Ongoing",
    ]
    assert composite_order_schema["properties"]["compositePoLines"]["items"]["properties"][
        "paymentStatus"
    ]["enum"] == [
        "Awaiting Payment",
        "Cancelled",
        "Fully Paid",
        "Partially Paid",
        "Payment Not Required",
        "Pending",
        "Ongoing",
    ]


def test_parse_record_mapping_file(mapper):
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
                "description": "",
            },
            {"folio_field": "vendor", "legacy_field": "vendor", "value": "", "description": ""},
            {"folio_field": "orderType", "legacy_field": "type", "value": "", "description": ""},
            {
                "folio_field": "compositePoLines[0].titleOrPackage",
                "legacy_field": "TITLE",
                "value": "",
                "description": "",
            },
        ]
    }
    folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(composite_order_map)

    assert folio_keys


def test_composite_order_mapping(mapper):
    data = {"order_number": "o123", "vendor": "EBSCO", "type": "One-Time"}

    composite_order, idx = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)
    assert composite_order["id"] == "6bf8d907-054d-53ad-9031-7a45887fcafa"
    assert composite_order["poNumber"] == "o123"
    assert composite_order["vendor"] == "fc54327d-fd60-4f6a-ba37-a4375511b91b"
    assert composite_order["orderType"] == "One-Time"


def test_composite_order_with_one_pol_mapping(mapper):

    data = {
        "order_number": "o124",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
    }
    composite_order_with_pol, idx = mapper.do_map(
        data, data["order_number"], FOLIONamespaces.orders
    )

    assert composite_order_with_pol["poNumber"] == "o124"

    assert (
        composite_order_with_pol["compositePoLines"][0]["titleOrPackage"] == "Once upon a time..."
    )
    assert (
        composite_order_with_pol["compositePoLines"][0]["instanceId"]
        == "ae1daef2-ddea-4d87-a434-3aa98ed3e687"
    )
    assert composite_order_with_pol["compositePoLines"][0]["cost"]["currency"] == "USD"


def test_one_order_one_pol_multiple_notes(mapper):

    mapper.composite_order_map = {
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
                "description": "",
            },
            {"folio_field": "vendor", "legacy_field": "vendor", "value": "", "description": ""},
            {"folio_field": "orderType", "legacy_field": "type", "value": "", "description": ""},
            {
                "folio_field": "compositePoLines[0].id",
                "legacy_field": "order_number",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].titleOrPackage",
                "legacy_field": "TITLE",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].instanceId",
                "legacy_field": "bibnumber",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].acquisitionMethod",
                "legacy_field": "vendor",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].orderFormat",
                "legacy_field": "vendor",
                "value": "",
                "description": "",
                "rules": {"replaceValues": {"EBSCO": "Electronic Resource"}},
            },
            {
                "folio_field": "compositePoLines[0].cost.currency",
                "legacy_field": "",
                "value": "USD",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].source",
                "legacy_field": "",
                "value": "API",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].source",
                "legacy_field": "",
                "value": "API",
                "description": "",
            },
            {
                "folio_field": "notes[0].domain",
                "legacy_field": "Not mapped",
                "value": "orders",
                "description": "",
            },
            {
                "folio_field": "notes[0].typeId",
                "legacy_field": "",
                "value": "f5bba0d2-7732-4687-8311-a2cb0eaa12e5",
                "description": "",
            },
            {
                "folio_field": "notes[0].title",
                "legacy_field": "",
                "value": "A migrated note",
                "description": "",
            },
            {
                "folio_field": "notes[0].content",
                "legacy_field": "note1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[1].domain",
                "legacy_field": "Not mapped",
                "value": "orders",
                "description": "",
            },
            {
                "folio_field": "notes[1].typeId",
                "legacy_field": "",
                "value": "f5bba0d2-7732-4687-8311-a2cb0eaa12e5",
                "description": "",
            },
            {
                "folio_field": "notes[1].title",
                "legacy_field": "",
                "value": "Another migrated note",
                "description": "",
            },
            {
                "folio_field": "notes[1].content",
                "legacy_field": "note2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0]",
                "legacy_field": "order_note",
                "value": "",
                "description": "",
            },
        ]
    }

    data = {
        "row_number": "o124-1",
        "order_number": "o124",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
        "note1": "Hello, hello, hello!",
        "note2": "Make it work!",
        "order_note": "Buy only important stuff."
    }

    composite_order, idx = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders, True)

    mapper.notes_mapper.map_notes(
        data,
        data["order_number"],
        composite_order["compositePoLines"][0]["id"],
        FOLIONamespaces.orders,
    )

    # All in all there shoulkd be three order notes
    assert str(mapper.extradata_writer.cache).count('"domain": "orders"') == 2

    # There should be two notes linked to the POL
    assert (
        str(mapper.extradata_writer.cache).count(composite_order["compositePoLines"][0]["id"]) == 2
    )


def test_multiple_pols_with_one_or_more_notes(mapper):

    data_pols = [
        {
            "row_number": "o124-1",
            "order_number": "o124",
            "vendor": "EBSCO",
            "type": "One-Time",
            "TITLE": "Once upon a time...",
            "bibnumber": "1",
            "note1": "Hello, hello, hello!",
            "note2": "Make it work!",
        },
        {
            "row_number": "o124-2",
            "order_number": "o124",
            "vendor": "EBSCO",
            "type": "One-Time",
            "TITLE": "Sunset Beach: the comic",
            "bibnumber": "2",
            "note1": "Purchased at local yard sale.",
        },
    ]

    composite_orders = []

    for row in data_pols:
        composite_order, idx = mapper.do_map(row, row["order_number"], FOLIONamespaces.orders, True)

        composite_orders.append(composite_order)

        mapper.notes_mapper.map_notes(
            row,
            row["order_number"],
            composite_order["compositePoLines"][0]["id"],
            FOLIONamespaces.orders,
        )

    # All in all there shoulkd be three order notes
    assert str(mapper.extradata_writer.cache).count('"domain": "orders"') == 3

    # There should be two notes linked to the first POL
    assert (
        str(mapper.extradata_writer.cache).count(composite_orders[0]["compositePoLines"][0]["id"])
        == 2
    )

    # There should be one notes linked to the first POL
    assert (
        str(mapper.extradata_writer.cache).count(composite_orders[1]["compositePoLines"][0]["id"])
        == 1
    )
