import copy
import json
import logging
from copy import deepcopy

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.order_mapper import (
    CompositeOrderMapper,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.migration_tasks.orders_transformer import OrdersTransformer
from .test_infrastructure import mocked_classes

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
        folio_release=FolioRelease.ramsons,
        library_name="Order tester Library",
        log_level_debug=False,
        iteration_identifier="Test!",
        base_folder="/",
        multi_field_delimiter="^-^",
    )
    task_config = OrdersTransformer.TaskConfiguration(
        name="Test",
        migration_task_type="OrdersTransformer",
        files=[
            FileDefinition(
                file_name="orders.csv",
            )
        ],
        orders_mapping_file_name="orders_mapping_file.json",
        organizations_code_map_file_name="organizations_code_map.json",
        acquisition_method_map_file_name="acquisition_method_map.json",
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
                "legacy_field": "acqmethod",
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
                "folio_field": "compositePoLines[0].cost.quantityPhysical",
                "legacy_field": "quantity_physical",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].cost.quantityElectronic",
                "legacy_field": "",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].cost.poLineEstimatedPrice",
                "legacy_field": "price",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].cost.currency",
                "legacy_field": "",
                "value": "USD",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].details.subscriptionFrom",
                "legacy_field": "SUBSCRIPTION FROM",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].details.subscriptionTo",
                "legacy_field": "SUBSCRIPTION TO",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].source",
                "legacy_field": "",
                "value": "API",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].locations[0].quantity",
                "legacy_field": "copies",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].locations[0].locationId",
                "legacy_field": "location",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[0].domain",
                "legacy_field": "Not mapped",
                "value": "orders",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[0].typeId",
                "legacy_field": "",
                "value": "f5bba0d2-7732-4687-8311-a2cb0eaa12e5",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[0].title",
                "legacy_field": "",
                "value": "A migrated note",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[0].content",
                "legacy_field": "note1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[1].domain",
                "legacy_field": "Not mapped",
                "value": "orders",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[1].typeId",
                "legacy_field": "",
                "value": "f5bba0d2-7732-4687-8311-a2cb0eaa12e5",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[1].title",
                "legacy_field": "",
                "value": "A migrated note",
                "description": "",
            },
            {
                "folio_field": "compositePoLines[0].notes[1].content",
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
        {"acqmethod": "p", "folio_value": "Purchase"},
        {"acqmethod": "*", "folio_value": "Other"},
    ]
    location_map = [
        {"location": "order", "folio_code": "E"},
        {"location": "*", "folio_code": "KU/CC/DI/O"},
    ]
    return CompositeOrderMapper(
        mock_folio_client,
        lib_config,
        task_config,
        composite_order_map,
        None,
        instance_id_map,
        acg_method_map,
        "",
        "",
        "",
        location_map,
        "",
        "",
    )

from folio_migration_tools.mapping_file_transformation.acq_schema_resolver import (  # noqa: E402
    resolve_acq_schema,
)


# Tests
@pytest.mark.slow
def test_fetch_acq_schemas_from_github_happy_path():
    composite_order_schema = resolve_acq_schema("mod-orders", "composite_purchase_order")

    assert composite_order_schema["$schema"]
    assert composite_order_schema["properties"]["orderType"]["enum"] == ["One-Time", "Ongoing"]
    # Detect the PO lines key (compositePoLines or poLines depending on version)
    po_lines_key = next(
        (p for p in composite_order_schema["properties"]
         if p in ("compositePoLines", "poLines")),
        None,
    )
    assert po_lines_key is not None
    assert composite_order_schema["properties"][po_lines_key]["items"]["properties"][
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
    assert composite_order_schema["properties"][po_lines_key]["items"]["properties"][
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


@pytest.mark.slow
def test_fetch_acq_schemas_for_specific_release():
    # Ramsons-era release should use compositePoLines
    old_schema = resolve_acq_schema(
        "mod-orders", "composite_purchase_order", "v12.9.14",
    )
    assert old_schema["$schema"]
    assert "compositePoLines" in old_schema["properties"]

    # Verify $ref resolution: PO line items should have resolved properties
    old_pol_props = old_schema["properties"]["compositePoLines"]["items"]["properties"]
    assert "titleOrPackage" in old_pol_props
    assert "cost" in old_pol_props
    assert "properties" in old_pol_props["cost"], "cost $ref was not resolved"
    assert "currency" in old_pol_props["cost"]["properties"]

    # Trillium release should use poLines
    new_schema = resolve_acq_schema(
        "mod-orders", "composite_purchase_order", "v13.1.0",
    )
    assert new_schema["$schema"]
    assert "poLines" in new_schema["properties"]

    # Verify $ref resolution: PO line items should have resolved properties
    new_pol_props = new_schema["properties"]["poLines"]["items"]["properties"]
    assert "titleOrPackage" in new_pol_props
    assert "cost" in new_pol_props
    assert "properties" in new_pol_props["cost"], "cost $ref was not resolved"
    assert "currency" in new_pol_props["cost"]["properties"]

    # Typeless $ref properties must be resolved, not discarded (referenceNumbers regression)
    ref_numbers = new_pol_props["vendorDetail"]["properties"]["referenceNumbers"]
    assert ref_numbers["type"] == "array"
    assert "refNumber" in ref_numbers["items"]["properties"]


@pytest.mark.slow
def test_acq_schema_resolution_is_deterministic():
    results = {
        json.dumps(
            resolve_acq_schema("mod-orders", "composite_purchase_order", "v13.1.0"),
            sort_keys=True,
        )
        for _ in range(3)
    }
    assert len(results) == 1


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
    data = {
        "order_number": "o123",
        "vendor": "EBSCO",
        "type": "One-Time",
        "copies": "",
        "location": "",
        "acqmethod": "p",
    }

    # Order without a title: poLine item has legacy-sourced content but is missing
    # required titleOrPackage. This should be logged as a field mapping issue,
    # the item discarded, and the record should still map.
    composite_order, idx = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)
    assert composite_order["poNumber"] == "o123"
    assert mapper.po_lines_key not in composite_order or len(composite_order[mapper.po_lines_key]) == 0


def test_composite_order_with_one_pol_mapping(mapper):
    data = {
        "order_number": "o124",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
        "quantity_physical": "1",
        "price": "125.00",
        "copies": "2",
        "location": "order",
        "acqmethod": "p",
        "SUBSCRIPTION FROM": "2026-01-01",
        "SUBSCRIPTION TO": "2026-12-31",
    }
    composite_order_with_pol, idx = mapper.do_map(
        data, data["order_number"], FOLIONamespaces.orders
    )

    assert composite_order_with_pol["poNumber"] == "o124"

    pol_key = mapper.po_lines_key
    assert (
        composite_order_with_pol[pol_key][0]["titleOrPackage"] == "Once upon a time..."
    )
    assert composite_order_with_pol[pol_key][0]["instanceId"] == "1"
    assert composite_order_with_pol[pol_key][0]["cost"]["currency"] == "USD"
    assert composite_order_with_pol[pol_key][0]["cost"]["quantityPhysical"] == "1"
    assert (
        composite_order_with_pol[pol_key][0]["cost"]["poLineEstimatedPrice"] == "125.00"
    )
    assert (
        composite_order_with_pol[pol_key][0]["locations"][0]["locationId"]
        == "184aae84-a5bf-4c6a-85ba-4a7c73026cd5"
    )
    assert composite_order_with_pol[pol_key][0]["locations"][0]["quantity"] == "2"
    assert composite_order_with_pol[pol_key][0]["details"]["subscriptionFrom"] == "2026-01-01"
    assert composite_order_with_pol[pol_key][0]["details"]["subscriptionTo"] == "2026-12-31"


def test_delimited_locations_are_mapped_to_separate_location_objects(mapper):
    data = {
        "order_number": "o199",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
        "quantity_physical": "1",
        "price": "125.00",
        "copies": "2^-^3",
        "location": "order^-^elsewhere",
        "acqmethod": "p",
        "SUBSCRIPTION FROM": "2026-01-01",
        "SUBSCRIPTION TO": "2026-12-31",
    }
    order, _ = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)

    locations = order[mapper.po_lines_key][0]["locations"]
    assert len(locations) == 2
    assert locations[0]["locationId"] == "184aae84-a5bf-4c6a-85ba-4a7c73026cd5"
    assert locations[1]["locationId"] != locations[0]["locationId"]
    assert "^-^" not in locations[1]["locationId"]
    assert [loc["quantity"] for loc in locations] == ["2", "3"]


def test_legacy_field_counted_once_per_mapping_not_once_per_array_prefix(mapper):
    """A column mapped to one FOLIO field must be reported once per row.

    The PO line array has several distinct sub-paths (cost, locations, notes...). Mapping
    the array must not repeat once per sub-path, which inflated the report past 100%.
    """
    data = {
        "order_number": "o125",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
        "quantity_physical": "1",
        "price": "125.00",
        "copies": "2",
        "location": "order",
        "acqmethod": "p",
    }
    saved = mapper.mapped_legacy_fields
    mapper.mapped_legacy_fields = {}
    try:
        order, _ = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)
        counts = dict(mapper.mapped_legacy_fields)
    finally:
        mapper.mapped_legacy_fields = saved

    pol = order[mapper.po_lines_key][0]
    assert pol["titleOrPackage"] == "Once upon a time..."
    assert pol["cost"]["quantityPhysical"] == "1"
    assert len(order[mapper.po_lines_key]) == 1
    # Each of these is mapped to exactly one FOLIO field
    for legacy_field in ["TITLE", "bibnumber", "acqmethod", "copies", "location"]:
        assert counts[legacy_field] == [1, 1], legacy_field
    # order_number is reported for poNumber and compositePoLines[0].id (legacyIdentifier is
    # not reported as a mapped legacy field)
    assert counts["order_number"] == [2, 2]


def test_array_of_objects_inside_nested_object_of_pol_is_mapped(mapper):
    """vendorDetail.referenceNumbers is an array of objects inside an object inside the POL.

    It must be written relative to the PO line being built, not relative to the order.
    """
    pol_key = mapper.po_lines_key
    extra = [
        (f"{pol_key}[0].vendorDetail.referenceNumbers[0].refNumber", "refnum"),
        (f"{pol_key}[0].vendorDetail.referenceNumbers[0].refNumberType", "reftype"),
        (f"{pol_key}[0].vendorDetail.vendorAccount", "acct"),
    ]
    saved_data, saved_keys = mapper.record_map["data"], mapper.folio_keys
    mapper.record_map["data"] = saved_data + [
        {"folio_field": f, "legacy_field": lf, "value": "", "description": ""} for f, lf in extra
    ]
    mapper.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
        mapper.record_map
    )
    data = {
        "order_number": "o126",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
        "quantity_physical": "1",
        "price": "125.00",
        "copies": "2",
        "location": "order",
        "acqmethod": "p",
        "refnum": "R1",
        "reftype": "Vendor continuation reference number",
        "acct": "A1",
    }
    try:
        order, _ = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)
    finally:
        mapper.record_map["data"], mapper.folio_keys = saved_data, saved_keys

    assert pol_key not in order[pol_key][0]
    assert order[pol_key][0]["vendorDetail"] == {
        "vendorAccount": "A1",
        "referenceNumbers": [
            {"refNumber": "R1", "refNumberType": "Vendor continuation reference number"}
        ],
    }


def test_one_order_one_pol_multiple_notes(mapper):
    data = {
        "row_number": "o124-1",
        "order_number": "o124",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
        "copies": "",
        "location": "",
        "note1": "Hello, hello, hello!",
        "note2": "Make it work!",
        "order_note": "Buy only important stuff.",
        "acqmethod": "p",
    }
    mapper.extradata_writer.cache = []
    composite_order, idx = mapper.do_map(data, data["order_number"], FOLIONamespaces.orders, True)
    pol_key = mapper.po_lines_key
    # PO-level notes are the order's own array of strings, not mod-notes records
    assert composite_order["notes"] == ["Buy only important stuff."]
    mapper.notes_mapper.map_notes(
        data,
        data["order_number"],
        composite_order[pol_key][0]["id"],
        FOLIONamespaces.orders,
    )

    # All in all there should be three order notes
    assert str(mapper.extradata_writer.cache).count('"domain": "orders"') == 2

    # There should be two notes linked to the POL
    assert (
        str(mapper.extradata_writer.cache).count(composite_order[pol_key][0]["id"]) == 2
    )


def test_pol_level_notes_are_extracted_and_linked_to_pol(mapper):
    pol_key = mapper.po_lines_key
    pol_map = {
        "data": mapper.record_map["data"]
        + [
            {"folio_field": f"{pol_key}[0].notes[0].typeId", "legacy_field": "",
             "value": "44cf3bf5-6e57-4541-8f8c-63b67c81e581", "description": ""},
            {"folio_field": f"{pol_key}[0].notes[0].domain", "legacy_field": "Not mapped",
             "value": "orders", "description": ""},
            {"folio_field": f"{pol_key}[0].notes[0].title", "legacy_field": "",
             "value": "POL note", "description": ""},
            {"folio_field": f"{pol_key}[0].notes[0].content", "legacy_field": "note1",
             "value": "", "description": ""},
        ]
    }
    from folio_migration_tools.mapping_file_transformation.notes_mapper import NotesMapper

    notes_mapper = NotesMapper(
        mapper.library_configuration,
        None,
        mapper.folio_client,
        mapper._build_pol_notes_map(pol_map),
        FOLIONamespaces.note,
        True,
    )
    notes_mapper.migration_report = mapper.migration_report
    data = {"order_number": "o9", "note1": "POL level note"}
    notes_mapper.extradata_writer.cache = []
    notes_mapper.map_notes(data, "o9", "pol-uuid", FOLIONamespaces.orders)
    cache = str(notes_mapper.extradata_writer.cache)
    assert "POL level note" in cache
    assert '"type": "poLine"' in cache and "pol-uuid" in cache


def test_multiple_pols_with_one_or_more_notes(mapper):
    data_pols = [
        {
            "row_number": "o124-1",
            "order_number": "o124",
            "vendor": "EBSCO",
            "type": "One-Time",
            "TITLE": "Once upon a time...",
            "bibnumber": "1",
            "copies": "",
            "location": "",
            "note1": "Hello, hello, hello!",
            "note2": "Make it work!",
            "acqmethod": "p",
        },
        {
            "row_number": "o124-2",
            "order_number": "o124",
            "vendor": "EBSCO",
            "type": "One-Time",
            "TITLE": "Sunset Beach: the comic",
            "bibnumber": "2",
            "copies": "",
            "location": "",
            "note1": "Purchased at local yard sale.",
            "acqmethod": "g",
        },
    ]

    composite_orders = []
    mapper.extradata_writer.cache = []
    pol_key = mapper.po_lines_key

    for row in data_pols:
        composite_order, idx = mapper.do_map(
            row, row["order_number"], FOLIONamespaces.orders, True
        )

        composite_orders.append(composite_order)

        mapper.notes_mapper.map_notes(
            row,
            row["order_number"],
            composite_order[pol_key][0]["id"],
            FOLIONamespaces.orders,
        )

    # All in all there shoulkd be three order notes
    assert str(mapper.extradata_writer.cache).count('"domain": "orders"') == 3

    # There should be two notes linked to the first POL
    assert (
        str(mapper.extradata_writer.cache).count(composite_orders[0][pol_key][0]["id"])
        == 2
    )

    # There should be one notes linked to the first POL
    assert (
        str(mapper.extradata_writer.cache).count(composite_orders[1][pol_key][0]["id"])
        == 1
    )


def test_composite_order_mapping_with_custom_fields(mapper):
    custom_map = deepcopy(mapper.record_map)
    pol_key = mapper.po_lines_key
    custom_map["data"].extend(
        [
            {
                "folio_field": "customFields.orderSource",
                "legacy_field": "order_custom",
                "value": "",
                "description": "",
            },
            {
                "folio_field": f"{pol_key}[0].customFields.lineTag",
                "legacy_field": "line_custom",
                "value": "",
                "description": "",
            },
        ]
    )

    custom_mapper = CompositeOrderMapper(
        mapper.folio_client,
        mapper.library_configuration,
        mapper.task_configuration,
        custom_map,
        mapper.organizations_code_map,
        mapper.instance_id_map,
        mapper.acquisitions_methods_mapping.map,
        "",
        "",
        "",
        mapper.location_mapping.map,
        "",
        "",
    )

    data = {
        "order_number": "o125",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Mapped with custom fields",
        "bibnumber": "1",
        "copies": "1",
        "location": "order",
        "acqmethod": "p",
        "order_custom": "donation",
        "line_custom": "rush",
    }

    mapped_order, _ = custom_mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)

    assert mapped_order["customFields"]["orderSource"] == "donation"
    assert mapped_order[pol_key][0]["customFields"]["lineTag"] == "rush"


def test_composite_order_mapping_with_named_billto_shipto_addresses(mapper):
    custom_map = deepcopy(mapper.record_map)
    custom_map["data"].extend(
        [
            {
                "folio_field": "billTo",
                "legacy_field": "bill_to",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "shipTo",
                "legacy_field": "ship_to",
                "value": "",
                "description": "",
            },
        ]
    )

    custom_mapper = CompositeOrderMapper(
        mapper.folio_client,
        mapper.library_configuration,
        mapper.task_configuration,
        custom_map,
        mapper.organizations_code_map,
        mapper.instance_id_map,
        mapper.acquisitions_methods_mapping.map,
        "",
        "",
        "",
        mapper.location_mapping.map,
        "",
        "",
    )

    data = {
        "order_number": "o126",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Address mapping",
        "bibnumber": "1",
        "acqmethod": "p",
        "location": "order",
        "quantity_physical": "1",
        "copies": "1",
        "bill_to": "Main Billing Address",
        "ship_to": "Main Shipping Address",
    }
    mapped_order, _ = custom_mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)
    mapped_order = custom_mapper.perform_additional_mapping(data["order_number"], mapped_order)

    assert mapped_order["billTo"] == "70d6f0a4-19ac-4f37-b568-4f7af7af767a"
    assert mapped_order["shipTo"] == "1b1f66b5-feb5-49af-ae88-f78656ec622f"


def test_composite_order_mapping_with_normalized_named_addresses(mapper):
    custom_map = deepcopy(mapper.record_map)
    custom_map["data"].extend(
        [
            {
                "folio_field": "billTo",
                "legacy_field": "bill_to",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "shipTo",
                "legacy_field": "ship_to",
                "value": "",
                "description": "",
            },
        ]
    )

    custom_mapper = CompositeOrderMapper(
        mapper.folio_client,
        mapper.library_configuration,
        mapper.task_configuration,
        custom_map,
        mapper.organizations_code_map,
        mapper.instance_id_map,
        mapper.acquisitions_methods_mapping.map,
        "",
        "",
        "",
        mapper.location_mapping.map,
        "",
        "",
    )

    data = {
        "order_number": "o126b",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Address mapping normalized",
        "bibnumber": "1",
        "acqmethod": "p",
        "location": "order",
        "quantity_physical": "1",
        "copies": "1",
        "bill_to": "Main  Billing\x1dAddress",
        "ship_to": "Main\tShipping Address",
    }
    mapped_order, _ = custom_mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)
    mapped_order = custom_mapper.perform_additional_mapping(data["order_number"], mapped_order)

    assert mapped_order["billTo"] == "70d6f0a4-19ac-4f37-b568-4f7af7af767a"
    assert mapped_order["shipTo"] == "1b1f66b5-feb5-49af-ae88-f78656ec622f"


def test_composite_order_mapping_with_unknown_billto_address_fails(mapper):
    custom_map = deepcopy(mapper.record_map)
    custom_map["data"].append(
        {
            "folio_field": "billTo",
            "legacy_field": "bill_to",
            "value": "",
            "description": "",
        }
    )

    custom_mapper = CompositeOrderMapper(
        mapper.folio_client,
        mapper.library_configuration,
        mapper.task_configuration,
        custom_map,
        mapper.organizations_code_map,
        mapper.instance_id_map,
        mapper.acquisitions_methods_mapping.map,
        "",
        "",
        "",
        mapper.location_mapping.map,
        "",
        "",
    )

    data = {
        "order_number": "o127",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Unknown address mapping",
        "bibnumber": "1",
        "acqmethod": "p",
        "location": "order",
        "copies": "1",
        "bill_to": "Unknown Billing Address",
    }

    with pytest.raises(TransformationRecordFailedError):
        custom_mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)


def test_order_address_prevalidation_rejects_unknown_hardcoded_uuid(mapper):
    custom_map = deepcopy(mapper.record_map)
    custom_map["data"].append(
        {
            "folio_field": "billTo",
            "legacy_field": "",
            "value": "99999999-9999-9999-9999-999999999999",
            "description": "",
        }
    )

    with pytest.raises(TransformationProcessError) as exc_info:
        CompositeOrderMapper(
            mapper.folio_client,
            mapper.library_configuration,
            mapper.task_configuration,
            custom_map,
            mapper.organizations_code_map,
            mapper.instance_id_map,
            mapper.acquisitions_methods_mapping.map,
            "",
            "",
            "",
            mapper.location_mapping.map,
            "",
            "",
        )

    assert exc_info.value.message == "Invalid order address values found in field mapping:"
    assert "UUID not found in tenant settings" in str(exc_info.value.data_value)
    assert "99999999-9999-9999-9999-999999999999" in str(exc_info.value.data_value)


def test_order_address_prevalidation_accepts_known_hardcoded_uuid(mapper):
    custom_map = deepcopy(mapper.record_map)
    custom_map["data"].append(
        {
            "folio_field": "billTo",
            "legacy_field": "",
            "value": "70d6f0a4-19ac-4f37-b568-4f7af7af767a",
            "description": "",
        }
    )

    custom_mapper = CompositeOrderMapper(
        mapper.folio_client,
        mapper.library_configuration,
        mapper.task_configuration,
        custom_map,
        mapper.organizations_code_map,
        mapper.instance_id_map,
        mapper.acquisitions_methods_mapping.map,
        "",
        "",
        "",
        mapper.location_mapping.map,
        "",
        "",
    )

    data = {
        "order_number": "o128",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Hardcoded address uuid",
        "bibnumber": "1",
        "acqmethod": "p",
        "location": "order",
        "copies": "1",
    }

    mapped_order, _ = custom_mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)

    assert mapped_order["billTo"] == "70d6f0a4-19ac-4f37-b568-4f7af7af767a"


def test_order_address_prevalidation_accepts_normalized_hardcoded_name(mapper):
    custom_map = deepcopy(mapper.record_map)
    custom_map["data"].append(
        {
            "folio_field": "billTo",
            "legacy_field": "",
            "value": "Main\x1d Billing  Address",
            "description": "",
        }
    )

    custom_mapper = CompositeOrderMapper(
        mapper.folio_client,
        mapper.library_configuration,
        mapper.task_configuration,
        custom_map,
        mapper.organizations_code_map,
        mapper.instance_id_map,
        mapper.acquisitions_methods_mapping.map,
        "",
        "",
        "",
        mapper.location_mapping.map,
        "",
        "",
    )

    data = {
        "order_number": "o128b",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Hardcoded address normalized name",
        "bibnumber": "1",
        "acqmethod": "p",
        "location": "order",
        "copies": "1",
    }

    mapped_order, _ = custom_mapper.do_map(data, data["order_number"], FOLIONamespaces.orders)

    assert mapped_order["billTo"] == "70d6f0a4-19ac-4f37-b568-4f7af7af767a"


def test_perform_additional_mapping_get_org_from_folio(mapper):
    pol_key = mapper.po_lines_key
    folio_po = {
        "id": "b90e41f3-8987-58fd-99be-b91068509aa0",
        "poNumber": "o124",
        "orderType": "One-Time",
        "vendor": "EBSCO",
        pol_key: [
            {
                "locations": [
                    {"locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "quantity": "1"}
                ],
                "id": "a10af88f-100c-4c5e-8ef3-c95fc85590c2",
                "instanceId": "1",
                "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                "cost": {
                    "currency": "USD",
                    "quantityPhysical": "1",
                    "poLineEstimatedPrice": "125.00",
                },
                "orderFormat": "Electronic Resource",
                "source": "API",
                "titleOrPackage": "Once upon a time...",
            }
        ],
    }

    folio_po = mapper.perform_additional_mapping("1", folio_po)
    assert folio_po["vendor"] == "fc54327d-fd60-4f6a-ba37-a4375511b91b"


def test_perform_additional_mapping_org_and_instance_uuids_from_folio_and_id_map(mapper):
    pol_key = mapper.po_lines_key
    folio_po = {
        "id": "b90e41f3-8987-58fd-99be-b91068509aa0",
        "poNumber": "o124",
        "orderType": "One-Time",
        "vendor": "GOBI",
        pol_key: [
            {
                "locations": [
                    {"locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "quantity": "1"}
                ],
                "id": "a10af88f-100c-4c5e-8ef3-c95fc85590c2",
                "instanceId": "1",
                "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                "cost": {
                    "currency": "USD",
                    "quantityPhysical": "1",
                    "poLineEstimatedPrice": "125.00",
                },
                "orderFormat": "Electronic Resource",
                "source": "API",
                "titleOrPackage": "Once upon a time...",
            }
        ],
    }

    folio_po = mapper.perform_additional_mapping("1", folio_po)
    assert folio_po["vendor"] == "837d04b6-d81c-4c49-9efd-2f62515999b3"
    assert folio_po[pol_key][0]["instanceId"] == "ae1daef2-ddea-4d87-a434-3aa98ed3e687"


def test_perform_additional_mapping_invalid_po_number(mapper):
    pol_key = mapper.po_lines_key
    folio_po = {
        "id": "b90e41f3-8987-58fd-99be-b91068509aa0",
        "poNumber": "o-124",
        "orderType": "One-Time",
        "vendor": "HamAwesomeStartup",
        pol_key: [
            {
                "locations": [
                    {
                        "locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5",
                        "quantity": "1"
                    }
                ],
                "id": "a10af88f-100c-4c5e-8ef3-c95fc85590c2",
                "instanceId": "1",
                "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                "cost": {
                    "currency": "USD",
                    "quantityPhysical": "1",
                    "poLineEstimatedPrice": "125.00",
                },
                "orderFormat": "Electronic Resource",
                "source": "API",
                "titleOrPackage": "Once upon a time...",
            }
        ],
    }
    with pytest.raises(TransformationRecordFailedError):
        folio_po = mapper.perform_additional_mapping("1", folio_po)
        assert not folio_po


def test_perform_additional_mapping_org_uuid_no_match(mapper):
    pol_key = mapper.po_lines_key
    folio_po = {
        "id": "b90e41f3-8987-58fd-99be-b91068509aa0",
        "poNumber": "o124",
        "orderType": "One-Time",
        "vendor": "LisasAwesomeStartup",
        pol_key: [
            {
                "locations": [
                    {"locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "quantity": "1"}
                ],
                "id": "a10af88f-100c-4c5e-8ef3-c95fc85590c2",
                "instanceId": "1",
                "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                "cost": {
                    "currency": "USD",
                    "quantityPhysical": "1",
                    "poLineEstimatedPrice": "125.00",
                },
                "orderFormat": "Electronic Resource",
                "source": "API",
                "titleOrPackage": "Once upon a time...",
            }
        ],
    }
    with pytest.raises(TransformationRecordFailedError):
        folio_po = mapper.perform_additional_mapping("1", folio_po)
        assert not folio_po


def test_perform_additional_mapping_instance_uuid_no_match(mapper):
    pol_key = mapper.po_lines_key
    folio_po = {
        "id": "b90e41f3-8987-58fd-99be-b91068509aa0",
        "poNumber": "o124",
        "orderType": "One-Time",
        "vendor": "GOBI",
        pol_key: [
            {
                "locations": [
                    {"locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5", "quantity": "1"}
                ],
                "id": "a10af88f-100c-4c5e-8ef3-c95fc85590c2",
                "instanceId": "myunpublishedbook",
                "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                "cost": {
                    "currency": "USD",
                    "quantityPhysical": "1",
                    "poLineEstimatedPrice": "125.00",
                },
                "orderFormat": "Electronic Resource",
                "source": "API",
                "titleOrPackage": "Once upon a time...",
            }
        ],
    }

    folio_po = mapper.perform_additional_mapping("1", folio_po)

    assert "instanceId" not in folio_po[pol_key][0]


def test_is_valid_po_number():
    assert CompositeOrderMapper.is_valid_po_number("Spam123")
    assert not CompositeOrderMapper.is_valid_po_number("o 124")
    assert not CompositeOrderMapper.is_valid_po_number("o-124")
    assert not CompositeOrderMapper.is_valid_po_number("o_124")
    assert not CompositeOrderMapper.is_valid_po_number("o.124")
    assert not CompositeOrderMapper.is_valid_po_number("o/124")


def test_map_order_with_notes_links_notes_to_the_mapped_pol_id(mapper):
    pol_key = mapper.po_lines_key
    data = {
        "row_number": "o125-1",
        "order_number": "o125",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Once upon a time...",
        "bibnumber": "1",
        "copies": "",
        "location": "",
        "note1": "First",
        "note2": "Second",
        "order_note": "PO level",
        "acqmethod": "p",
    }
    mapper.extradata_writer.cache = []
    composite_order, _ = mapper.map_order_with_notes(data, "row 0")
    pol_id = composite_order[pol_key][0]["id"]

    assert composite_order["notes"] == ["PO level"]
    notes = [
        json.loads(entry.split("\t", 1)[1])
        for entry in mapper.extradata_writer.cache
        if entry.startswith("notes\t")
    ]
    assert len(notes) == 2
    assert all(n["links"] == [{"id": pol_id, "type": "poLine"}] for n in notes)
    assert composite_order["id"] != pol_id


def test_po_level_mod_notes_mapping_halts_the_transformation(mapper):
    bad_map = deepcopy(mapper.record_map)
    bad_map["data"].append(
        {"folio_field": "notes[0].content", "legacy_field": "note1", "value": "",
         "description": ""}
    )
    with pytest.raises(TransformationProcessError) as exc_info:
        CompositeOrderMapper._validate_no_po_level_mod_notes_mappings(bad_map)
    assert "notes[0].content" in str(exc_info.value.data_value)

    # The PO's string-array notes and POL-level notes are fine
    CompositeOrderMapper._validate_no_po_level_mod_notes_mappings(mapper.record_map)


def test_organizations_code_map_maps_legacy_value_to_folio_code(mapper):
    code_map = CompositeOrderMapper._build_organizations_code_map(
        [{"legacy_vendor": "GB1", "folio_code": "GOBI"}], mapper.folio_organizations_by_code
    )
    mapper_with_map = copy.copy(mapper)
    mapper_with_map.organizations_code_map = code_map

    assert (
        mapper_with_map.get_folio_organization_uuid("1", " gb1 ")
        == "837d04b6-d81c-4c49-9efd-2f62515999b3"
    )
    # The map is authoritative: a value that is a FOLIO code but not in the map fails
    with pytest.raises(TransformationRecordFailedError):
        mapper_with_map.get_folio_organization_uuid("1", "EBSCO")


def test_organizations_code_map_star_row_is_ignored(mapper):
    code_map = CompositeOrderMapper._build_organizations_code_map(
        [
            {"legacy_vendor": "GB1", "folio_code": "GOBI"},
            {"legacy_vendor": "*", "folio_code": "EBSCO"},
        ],
        mapper.folio_organizations_by_code,
    )
    mapper_with_map = copy.copy(mapper)
    mapper_with_map.organizations_code_map = code_map

    assert (
        mapper_with_map.get_folio_organization_uuid("1", "GB1")
        == "837d04b6-d81c-4c49-9efd-2f62515999b3"
    )
    with pytest.raises(TransformationRecordFailedError):
        mapper_with_map.get_folio_organization_uuid("1", "unknown")


def test_organizations_code_map_rejects_unknown_folio_code(mapper):
    with pytest.raises(TransformationProcessError):
        CompositeOrderMapper._build_organizations_code_map(
            [{"legacy_vendor": "GB1", "folio_code": "NOPE"}], mapper.folio_organizations_by_code
        )


def test_organizations_code_map_requires_one_legacy_column(mapper):
    with pytest.raises(TransformationProcessError):
        CompositeOrderMapper._build_organizations_code_map(
            [{"a": "1", "b": "2", "folio_code": "GOBI"}], mapper.folio_organizations_by_code
        )


def test_organizations_mapping_is_reported_per_value(mapper):
    code_map = CompositeOrderMapper._build_organizations_code_map(
        [{"legacy_vendor": "GB1", "folio_code": "GOBI"}], mapper.folio_organizations_by_code
    )
    mapper_with_map = copy.copy(mapper)
    mapper_with_map.organizations_code_map = code_map

    mapper_with_map.get_folio_organization_uuid("1", "GB1")
    with pytest.raises(TransformationRecordFailedError):
        mapper_with_map.get_folio_organization_uuid("1", "unknown")

    report = mapper.migration_report.report["OrganizationsMapping"]
    assert report["GB1 -> GOBI"] >= 1
    assert any(k.endswith("-- unknown -> ") for k in report)


def test_expense_class_map_is_used_for_expense_class_id(mapper):
    class StubClient:
        def folio_get_all(self, *args, **kwargs):
            return [{"id": "ec-1", "code": "Elec", "name": "Electronic"}]

    expense_mapping = RefDataMapping(
        StubClient(),
        "/finance/expense-classes",
        "expenseClasses",
        [
            {"legacy_ec": "E", "folio_code": "Elec"},
            {"legacy_ec": "*", "folio_code": "Elec"},
        ],
        "code",
        "ExpenseClassMapping",
    )
    mapper_with_map = copy.copy(mapper)
    mapper_with_map.expense_class_mapping = expense_mapping

    result = mapper_with_map.get_prop(
        {"legacy_ec": "E"},
        "compositePoLines[0].fundDistribution[0].expenseClassId",
        "1",
        "",
    )
    assert result == "ec-1"
    assert "E -> Elec" in mapper.migration_report.report["ExpenseClassMapping"]
    # An unmatched value is left unmapped rather than defaulted to the * row
    assert (
        mapper_with_map.get_prop(
            {"legacy_ec": "unknown"},
            "compositePoLines[0].fundDistribution[0].expenseClassId",
            "1",
            "",
        )
        == ""
    )


def test_material_type_map_is_used_for_pol_material_types(mapper):
    class StubClient:
        def folio_get_all(self, *args, **kwargs):
            return [{"id": "mt-1", "name": "book"}, {"id": "mt-2", "name": "electronic resource"}]

    material_mapping = RefDataMapping(
        StubClient(),
        "/material-types",
        "mtypes",
        [
            {"legacy_mt": "B", "folio_name": "book"},
            {"legacy_mt": "E", "folio_name": "electronic resource"},
            {"legacy_mt": "*", "folio_name": "book"},
        ],
        "name",
        "MaterialTypeMapping",
    )
    mapper_with_map = copy.copy(mapper)
    mapper_with_map.material_type_mapping = material_mapping

    physical = "compositePoLines[0].physical.materialType"
    eresource = "compositePoLines[0].eresource.materialType"
    assert mapper_with_map.get_prop({"legacy_mt": "B"}, physical, "1", "") == "mt-1"
    assert mapper_with_map.get_prop({"legacy_mt": "E"}, eresource, "1", "") == "mt-2"
    assert "B -> book" in mapper.migration_report.report["MaterialTypeMapping"]


def test_material_type_passes_through_without_map(mapper):
    mapper_copy = copy.copy(mapper)
    mapper_copy.material_type_mapping = None
    uuid_value = "5b2e0e6e-0000-4000-8000-000000000001"
    mapper_copy.record_map = {
        "data": [
            {
                "folio_field": "compositePoLines[0].physical.materialType",
                "legacy_field": "legacy_mt",
                "value": "",
                "description": "",
            }
        ]
    }
    mapper_copy.mapped_from_legacy_data = {
        "compositePoLines[0].physical.materialType": ["legacy_mt"]
    }

    result = mapper_copy.get_prop(
        {"legacy_mt": uuid_value},
        "compositePoLines[0].physical.materialType",
        "1",
        "",
    )
    assert result == uuid_value

    # A name is resolved against FOLIO material types without a map
    mapper_copy._material_types_by_name = {"book": "mt-1"}
    name_result = mapper_copy.get_prop(
        {"legacy_mt": " Book "}, "compositePoLines[0].physical.materialType", "1", ""
    )
    assert name_result == "mt-1"
    assert any(
        key.endswith("-> mt-1") for key in mapper.migration_report.report["MaterialTypeMapping"]
    )

    # An unmatched name fails the record and is reported
    with pytest.raises(TransformationRecordFailedError) as error_info:
        mapper_copy.get_prop(
            {"legacy_mt": "nonsense"}, "compositePoLines[0].physical.materialType", "7", ""
        )
    assert error_info.value.index_or_id == "7"
    assert error_info.value.data_value == "nonsense"
    assert any(
        "nonsense" in key
        for key in mapper.migration_report.report["MaterialTypeMapping"]
    )


def test_hardcoded_material_type_is_validated(mapper):
    mapper_copy = copy.copy(mapper)
    mapper_copy.material_type_mapping = None
    mapper_copy._material_types_by_name = {"book": "mt-1"}

    def validate_with(value):
        mapper_copy.record_map = {
            "data": [
                {
                    "folio_field": "compositePoLines[0].physical.materialType",
                    "legacy_field": "",
                    "value": value,
                    "description": "",
                }
            ]
        }
        mapper_copy._validate_hardcoded_material_types()

    validate_with("book")
    validate_with("")
    with pytest.raises(TransformationProcessError):
        validate_with("not a type")
    with pytest.raises(TransformationProcessError):
        validate_with("5b2e0e6e-0000-4000-8000-000000000009")

    # Hardcoded values are FOLIO values, so they are validated even with a map configured
    mapper_copy.material_type_mapping = object()
    with pytest.raises(TransformationProcessError):
        validate_with("not a type")


def test_hardcoded_material_type_bypasses_the_map(mapper):
    class StubClient:
        def folio_get_all(self, *args, **kwargs):
            return [{"id": "mt-1", "name": "book"}, {"id": "mt-2", "name": "serial"}]

    mapper_copy = copy.copy(mapper)
    mapper_copy.material_type_mapping = RefDataMapping(
        StubClient(),
        "/material-types",
        "mtypes",
        [{"legacy_mt": "B", "folio_name": "book"}, {"legacy_mt": "*", "folio_name": "book"}],
        "name",
        "MaterialTypeMapping",
    )
    mapper_copy._material_types_by_name = {"book": "mt-1", "serial": "mt-2"}
    prop = "compositePoLines[0].physical.materialType"
    mapper_copy.mapped_from_legacy_data = {prop: ["legacy_mt"]}

    def entry(**kwargs):
        return {
            "data": [
                {"folio_field": prop, "legacy_field": "", "value": "", "description": "", **kwargs}
            ]
        }

    # Hardcoded name is resolved directly; the map has no row for it and would default to book
    mapper_copy.record_map = entry(value="serial")
    assert mapper_copy.get_prop({}, prop, "1", "") == "mt-2"

    # A legacy value still goes through the map
    mapper_copy.record_map = entry(legacy_field="legacy_mt")
    assert mapper_copy.get_prop({"legacy_mt": "B"}, prop, "1", "") == "mt-1"

    # fallback_value is a literal only when the legacy field is empty
    mapper_copy.record_map = entry(legacy_field="legacy_mt", fallback_value="serial")
    assert mapper_copy.get_prop({"legacy_mt": ""}, prop, "1", "") == "mt-2"
    assert mapper_copy.get_prop({"legacy_mt": "B"}, prop, "1", "") == "mt-1"


def test_product_id_type_resolves_by_name_or_uuid(mapper):
    mapper_copy = copy.copy(mapper)
    mapper_copy._identifier_types_by_name = {"isbn": "8261054f-be78-422d-bd51-4ed9f33c3422"}
    prop = "compositePoLines[0].details.productIds[0].productIdType"
    resolve = mapper_copy._resolve_product_id_type
    assert resolve("ISBN", prop, "1") == "8261054f-be78-422d-bd51-4ed9f33c3422"
    assert resolve(" isbn ", prop, "1") == "8261054f-be78-422d-bd51-4ed9f33c3422"
    explicit = "913300b2-03ed-469a-8179-c1092c991227"
    assert resolve(explicit, prop, "1") == explicit
    # A bad legacy value does not fail the record
    assert resolve("Not a type", prop, "1") == ""


def test_incomplete_product_ids_are_dropped_but_record_is_kept(mapper):
    order = {
        mapper.po_lines_key: [
            {
                "details": {
                    "productIds": [
                        {"productId": "111", "productIdType": "uuid-1"},
                        {"productId": "222", "productIdType": ""},
                        {"productId": "", "productIdType": "uuid-2"},
                        {"productId": "", "productIdType": ""},
                    ]
                }
            },
            {"details": {"productIds": [{"productId": "333", "productIdType": ""}]}},
        ]
    }
    mapper._drop_incomplete_product_ids("1", order)
    assert order[mapper.po_lines_key][0]["details"]["productIds"] == [
        {"productId": "111", "productIdType": "uuid-1"}
    ]
    assert "productIds" not in order[mapper.po_lines_key][1]["details"]


def test_hardcoded_product_id_type_is_validated(mapper):
    mapper_copy = copy.copy(mapper)
    mapper_copy._identifier_types_by_name = {"isbn": "8261054f-be78-422d-bd51-4ed9f33c3422"}

    def check(value):
        mapper_copy.record_map = {
            "data": [
                {
                    "folio_field": "compositePoLines[0].details.productIds[0].productIdType",
                    "legacy_field": "",
                    "value": value,
                }
            ]
        }
        mapper_copy._validate_hardcoded_product_id_types()

    check("ISBN")
    check("8261054f-be78-422d-bd51-4ed9f33c3422")
    check("")
    with pytest.raises(TransformationProcessError):
        check("Nope")
    with pytest.raises(TransformationProcessError):
        check("913300b2-03ed-469a-8179-c1092c991227")


def test_locations_with_the_same_id_are_merged_and_counted(mapper):
    order = {
        mapper.po_lines_key: [
            {
                "locations": [
                    {"locationId": "a", "quantity": 1, "quantityPhysical": 1},
                    {"locationId": "b", "quantity": 1},
                    {"locationId": "a", "quantity": 1, "quantityPhysical": 1},
                    {"locationId": "a", "quantity": 1, "quantityPhysical": 1},
                    {"quantity": 1},
                ]
            }
        ]
    }
    mapper._merge_duplicate_locations(order)
    assert order[mapper.po_lines_key][0]["locations"] == [
        {"locationId": "a", "quantity": 3, "quantityPhysical": 3},
        {"locationId": "b", "quantity": 1},
        {"quantity": 1},
    ]


def test_unique_locations_are_left_untouched(mapper):
    locations = [{"locationId": "a", "quantity": "2"}, {"locationId": "b", "quantity": "3"}]
    order = {mapper.po_lines_key: [{"locations": [dict(loc) for loc in locations]}]}
    mapper._merge_duplicate_locations(order)
    assert order[mapper.po_lines_key][0]["locations"] == locations
    mapper._merge_duplicate_locations({mapper.po_lines_key: [{}]})


def test_location_quantities_are_validated_against_cost(mapper):
    def order(locations, cost):
        return {mapper.po_lines_key: [{"locations": locations, "cost": cost}]}

    ok = order(
        [{"locationId": "a", "quantityPhysical": 2}, {"locationId": "b", "quantityPhysical": 1}],
        {"quantityPhysical": 3},
    )
    bad = order([{"locationId": "a", "quantity": 1}], {"quantityPhysical": 2})
    mapper._validate_location_quantities("1", ok)
    with pytest.raises(TransformationRecordFailedError, match="quantity total"):
        mapper._validate_location_quantities("1", bad)


def test_issues_are_logged_with_the_legacy_id_and_the_row(mapper):
    mapper = copy.copy(mapper)
    data = {
        "order_number": "o300",
        "vendor": "EBSCO",
        "type": "One-Time",
        "TITLE": "Mismatch",
        "bibnumber": "1",
        "quantity_physical": "1",
        "price": "125.00",
        "copies": "2",
        "location": "order",
        "acqmethod": "p",
    }
    with pytest.raises(TransformationRecordFailedError) as raised:
        mapper.map_order_with_notes(data, "row 7")
    assert raised.value.index_or_id == "o300 (row 7)"
    assert mapper._with_row("o300 (row 7)") == "o300 (row 7)"


def test_field_mapping_errors_are_logged_with_the_legacy_id_and_the_row(mapper):
    mapper = copy.copy(mapper)
    mapper._current_row = "row 3"
    error = TransformationFieldMappingError("o301", "Bad value", "x")
    mapper.handle_transformation_field_mapping_error("o301", error)
    assert error.index_or_id == "o301 (row 3)"
