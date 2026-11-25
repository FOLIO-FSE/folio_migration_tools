import copy
import json
import logging
import types
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
                "rules": {"replaceValues": {"EBSCO": "Physical Resource"}},
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
                "folio_field": "compositePoLines[0].locations[0].quantityPhysical",
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
        (p for p in composite_order_schema["properties"] if p in ("compositePoLines", "poLines")),
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
        "mod-orders",
        "composite_purchase_order",
        "v12.9.14",
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
        "mod-orders",
        "composite_purchase_order",
        "v13.1.0",
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
    assert (
        mapper.po_lines_key not in composite_order
        or len(composite_order[mapper.po_lines_key]) == 0
    )


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
    assert composite_order_with_pol[pol_key][0]["titleOrPackage"] == "Once upon a time..."
    assert composite_order_with_pol[pol_key][0]["instanceId"] == "1"
    assert composite_order_with_pol[pol_key][0]["cost"]["currency"] == "USD"
    assert composite_order_with_pol[pol_key][0]["cost"]["quantityPhysical"] == "1"
    assert composite_order_with_pol[pol_key][0]["cost"]["poLineEstimatedPrice"] == "125.00"
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
    for legacy_field in ["TITLE", "bibnumber", "acqmethod", "location"]:
        assert counts[legacy_field] == [1, 1], legacy_field
    # "copies" is mapped to two FOLIO fields (location quantity and quantityPhysical)
    assert counts["copies"] == [2, 2]
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
        "quantity_physical": "1",
        "copies": "1",
        "location": "order",
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
    assert str(mapper.extradata_writer.cache).count(composite_order[pol_key][0]["id"]) == 2


def test_pol_level_notes_are_extracted_and_linked_to_pol(mapper):
    pol_key = mapper.po_lines_key
    pol_map = {
        "data": mapper.record_map["data"]
        + [
            {
                "folio_field": f"{pol_key}[0].notes[0].typeId",
                "legacy_field": "",
                "value": "44cf3bf5-6e57-4541-8f8c-63b67c81e581",
                "description": "",
            },
            {
                "folio_field": f"{pol_key}[0].notes[0].domain",
                "legacy_field": "Not mapped",
                "value": "orders",
                "description": "",
            },
            {
                "folio_field": f"{pol_key}[0].notes[0].title",
                "legacy_field": "",
                "value": "POL note",
                "description": "",
            },
            {
                "folio_field": f"{pol_key}[0].notes[0].content",
                "legacy_field": "note1",
                "value": "",
                "description": "",
            },
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
    assert str(mapper.extradata_writer.cache).count(composite_orders[0][pol_key][0]["id"]) == 2

    # There should be one notes linked to the first POL
    assert str(mapper.extradata_writer.cache).count(composite_orders[1][pol_key][0]["id"]) == 1


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
                    {
                        "locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5",
                        "quantity": "1",
                        "quantityPhysical": "1",
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
                "orderFormat": "Physical Resource",
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
                    {
                        "locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5",
                        "quantity": "1",
                        "quantityPhysical": "1",
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
                "orderFormat": "Physical Resource",
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
                        "quantity": "1",
                        "quantityPhysical": "1",
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
                "orderFormat": "Physical Resource",
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
                    {
                        "locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5",
                        "quantity": "1",
                        "quantityPhysical": "1",
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
                "orderFormat": "Physical Resource",
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
                    {
                        "locationId": "184aae84-a5bf-4c6a-85ba-4a7c73026cd5",
                        "quantity": "1",
                        "quantityPhysical": "1",
                    }
                ],
                "id": "a10af88f-100c-4c5e-8ef3-c95fc85590c2",
                "instanceId": "myunpublishedbook",
                "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                "cost": {
                    "currency": "USD",
                    "quantityPhysical": "1",
                    "poLineEstimatedPrice": "125.00",
                },
                "orderFormat": "Physical Resource",
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
        "quantity_physical": "1",
        "copies": "1",
        "location": "order",
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
        {
            "folio_field": "notes[0].content",
            "legacy_field": "note1",
            "value": "",
            "description": "",
        }
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
    mapper_with_map.record_map = {
        "data": [
            {
                "folio_field": f"compositePoLines[0].{kind}.materialType",
                "legacy_field": "legacy_mt",
                "value": "",
                "description": "",
            }
            for kind in ("physical", "eresource")
        ]
    }
    mapper_with_map.mapped_from_legacy_data = {
        f"compositePoLines[0].{kind}.materialType": ["legacy_mt"]
        for kind in ("physical", "eresource")
    }

    physical = "compositePoLines[0].physical.materialType"
    eresource = "compositePoLines[0].eresource.materialType"
    assert mapper_with_map.get_prop({"legacy_mt": "B"}, physical, "1", "") == "mt-1"
    assert mapper_with_map.get_prop({"legacy_mt": "E"}, eresource, "1", "") == "mt-2"
    assert "B -> book" in mapper.migration_report.report["MaterialTypeMapping"]
    # An empty value stays empty rather than taking the map's default
    assert mapper_with_map.get_prop({"legacy_mt": ""}, physical, "1", "") == ""


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
    assert any("nonsense" in key for key in mapper.migration_report.report["MaterialTypeMapping"])


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
    resolve = mapper_copy._resolve_product_id_type
    assert resolve("ISBN") == "8261054f-be78-422d-bd51-4ed9f33c3422"
    assert resolve(" isbn ") == "8261054f-be78-422d-bd51-4ed9f33c3422"
    explicit = "913300b2-03ed-469a-8179-c1092c991227"
    assert resolve(explicit) == explicit
    # A bad legacy value does not fail the record
    assert resolve("Not a type") == ""


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


def test_unique_locations_are_counted_not_summed(mapper):
    # One qty column feeds every location object; each object stands for one piece.
    locations = [
        {"locationId": "a", "quantityPhysical": "2"},
        {"locationId": "b", "quantityPhysical": "2"},
    ]
    order = {mapper.po_lines_key: [{"locations": [dict(loc) for loc in locations]}]}
    mapper._merge_duplicate_locations(order)
    assert order[mapper.po_lines_key][0]["locations"] == [
        {"locationId": "a", "quantityPhysical": 1},
        {"locationId": "b", "quantityPhysical": 1},
    ]
    mapper._merge_duplicate_locations({mapper.po_lines_key: [{}]})


def test_single_location_keeps_its_mapped_quantity(mapper):
    locations = [{"locationId": "a", "quantity": "5", "quantityPhysical": "5"}]
    order = {mapper.po_lines_key: [{"locations": [dict(loc) for loc in locations]}]}
    mapper._merge_duplicate_locations(order)
    assert order[mapper.po_lines_key][0]["locations"] == locations


def _pol(**kw):
    pol = {
        "orderFormat": "Physical Resource",
        "cost": {"quantityPhysical": 3, "listUnitPrice": 10},
        "locations": [
            {"locationId": "a", "quantityPhysical": 2},
            {"locationId": "b", "quantityPhysical": 1},
        ],
    }
    pol.update(kw)
    return pol


def _validate(mapper, pol, status="Pending"):
    mapper._validate_composite_order("1", {"workflowStatus": status, mapper.po_lines_key: [pol]})


def test_valid_order_passes_validation(mapper):
    _validate(mapper, _pol())


def test_location_quantities_are_validated_against_cost(mapper):
    bad = _pol(locations=[{"locationId": "a", "quantityPhysical": 1}])
    with pytest.raises(TransformationRecordFailedError, match="quantityPhysical total 1"):
        _validate(mapper, bad)


def test_location_quantity_field_is_not_compared_with_cost(mapper):
    # mod-orders recomputes location.quantity itself
    _validate(mapper, _pol(locations=[{"locationId": "a", "quantity": 9, "quantityPhysical": 3}]))


@pytest.mark.parametrize(
    "pol, message",
    [
        (_pol(cost={"quantityPhysical": 0}), "physical cost quantity must be above 0"),
        (_pol(cost={"quantityPhysical": 3, "quantityElectronic": 1}), "electronic cost"),
        (
            _pol(orderFormat="Electronic Resource", cost={"quantityPhysical": 1}),
            "electronic cost quantity must be above 0",
        ),
        (
            _pol(orderFormat="P/E Mix", cost={"quantityPhysical": 3}),
            "electronic cost quantity must be above 0",
        ),
        (_pol(locations=[{"quantityPhysical": 3}]), "exactly one of locationId"),
        (_pol(cost={"quantityPhysical": 3, "listUnitPrice": -1}), "list unit price"),
        (_pol(cost={"quantityPhysical": 3, "discount": -2}), "discount"),
        (_pol(isPackage=True, instanceId="x"), "package"),
        (
            _pol(
                orderFormat="Electronic Resource",
                cost={"quantityElectronic": 1},
                locations=[],
                details={"isBinderyActive": True},
            ),
            "binder",
        ),
    ],
)
def test_validation_rejects_what_folio_rejects(mapper, pol, message):
    with pytest.raises(TransformationRecordFailedError, match=message):
        _validate(mapper, pol)


def test_line_without_quantities_skips_validation(mapper):
    _validate(mapper, _pol(cost={}, locations=[]))


def test_material_type_is_only_required_for_open_orders(mapper):
    pol = _pol(physical={"createInventory": "Instance, Holding, Item"})
    _validate(mapper, pol, "Pending")
    with pytest.raises(TransformationRecordFailedError, match="material type"):
        _validate(mapper, pol, "Open")
    pol["physical"]["materialType"] = "mt"
    _validate(mapper, pol, "Open")


def test_tenant_inventory_defaults_decide_material_type_requirement(mapper):
    mapper = copy.copy(mapper)
    mapper.create_inventory_defaults = {"physical": "Instance, Holding"}
    _validate(mapper, _pol(), "Open")
    mapper.create_inventory_defaults = {"physical": "Instance, Holding, Item"}
    with pytest.raises(TransformationRecordFailedError, match="material type"):
        _validate(mapper, _pol(), "Open")
    # Without a tenant setting the fallback is "None", so no material type is required
    mapper.create_inventory_defaults = {}
    _validate(mapper, _pol(), "Open")


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


def test_delimited_quantities_are_summed_when_locations_merge(mapper):
    locations = [
        {"locationId": "a", "quantityPhysical": "2"},
        {"locationId": "a", "quantityPhysical": "3"},
    ]
    order = {mapper.po_lines_key: [{"locations": [dict(loc) for loc in locations]}]}
    mapper._merge_duplicate_locations(order, {"quantityPhysical"}, "1")
    assert order[mapper.po_lines_key][0]["locations"] == [
        {"locationId": "a", "quantityPhysical": 5}
    ]


def test_delimited_quantity_fields_are_detected_from_the_legacy_row(mapper):
    delimited = {"copies": "2^-^3", "quantity_physical": "1", "location": "x^-^y"}
    assert "quantity" in mapper._delimited_location_quantity_fields(delimited)
    plain = {"copies": "2", "quantity_physical": "1", "location": "x^-^y"}
    assert mapper._delimited_location_quantity_fields(plain) == set()


def test_non_integer_quantity_fails_the_record(mapper):
    assert mapper._as_int("1.0", "1", "x") == 1
    assert mapper._as_int("", "1", "x") == 0
    with pytest.raises(TransformationRecordFailedError, match="whole number"):
        mapper._as_int("1.5", "1", "x")
    with pytest.raises(TransformationRecordFailedError, match="whole number"):
        mapper._as_int("abc", "1", "x")


def test_location_quantity_is_copied_to_the_format_field(mapper):
    def order(order_format, location):
        return {mapper.po_lines_key: [{"orderFormat": order_format, "locations": [location]}]}

    physical = order("Physical Resource", {"locationId": "a", "quantity": "2"})
    electronic = order("Electronic Resource", {"locationId": "a", "quantity": "2"})
    mixed = order("P/E Mix", {"locationId": "a", "quantity": "2"})
    explicit = order(
        "Physical Resource", {"locationId": "a", "quantity": "2", "quantityElectronic": "1"}
    )
    for composite in (physical, electronic, mixed, explicit):
        mapper._copy_quantity_to_format_field(composite)
    loc = lambda composite: composite[mapper.po_lines_key][0]["locations"][0]  # noqa: E731
    assert loc(physical)["quantityPhysical"] == "2"
    assert loc(electronic)["quantityElectronic"] == "2"
    assert (loc(mixed)["quantityElectronic"], loc(mixed)["quantityPhysical"]) == (1, 1)
    single = order("P/E Mix", {"locationId": "a", "quantity": "1"})
    mapper._copy_quantity_to_format_field(single)
    assert loc(single)["quantityElectronic"] == 1 and "quantityPhysical" not in loc(single)
    many = order("P/E Mix", {"locationId": "a", "quantity": "4"})
    mapper._copy_quantity_to_format_field(many)
    assert (loc(many)["quantityElectronic"], loc(many)["quantityPhysical"]) == (1, 3)
    assert "quantityPhysical" not in loc(explicit)


def test_mix_split_is_done_for_the_whole_line(mapper):
    def run(*locations):
        pol = {"orderFormat": "P/E Mix", "locations": list(locations)}
        mapper._copy_quantity_to_format_field({mapper.po_lines_key: [pol]})
        return [(loc.get("quantityPhysical"), loc.get("quantityElectronic")) for loc in locations]

    # One electronic piece on the first location, everything else physical
    assert run({"quantity": 1}, {"quantity": 3}) == [(None, 1), (3, None)]
    # An electronic quantity set on another location already covers the electronic side
    assert run({"quantity": 2}, {"quantity": 1, "quantityElectronic": 1}) == [(2, None), (None, 1)]
    # A single piece can't be split
    assert run({"quantity": 1}) == [(None, 1)]


def test_explicit_mix_quantity_is_respected_when_copying(mapper):
    def run(location):
        composite = {mapper.po_lines_key: [{"orderFormat": "P/E Mix", "locations": [location]}]}
        mapper._copy_quantity_to_format_field(composite)
        return composite[mapper.po_lines_key][0]["locations"][0]

    electronic = run({"locationId": "a", "quantity": "5", "quantityElectronic": "2"})
    assert (electronic["quantityElectronic"], electronic["quantityPhysical"]) == ("2", 3)
    physical = run({"locationId": "a", "quantity": "5", "quantityPhysical": "4"})
    assert (physical["quantityPhysical"], physical["quantityElectronic"]) == ("4", 1)
    both = run(
        {"locationId": "a", "quantity": "5", "quantityPhysical": 1, "quantityElectronic": 1}
    )
    assert (both["quantityPhysical"], both["quantityElectronic"]) == (1, 1)
    covered = run({"locationId": "a", "quantity": "2", "quantityElectronic": "2"})
    assert "quantityPhysical" not in covered


def test_cost_mirrors_the_location_split_when_the_totals_match(mapper):
    def run(order_format, cost, quantities):
        locations = [{"locationId": str(i), "quantity": q} for i, q in enumerate(quantities)]
        pol = {"orderFormat": order_format, "cost": cost, "locations": locations}
        mapper._copy_quantity_to_format_field({mapper.po_lines_key: [pol]})
        return pol["cost"]

    mixed = run("P/E Mix", {"quantityPhysical": "5"}, ["2", "3"])
    assert (mixed["quantityPhysical"], mixed["quantityElectronic"]) == (4, 1)
    electronic = run("Electronic Resource", {"quantityPhysical": "4"}, ["4"])
    assert (electronic["quantityPhysical"], electronic["quantityElectronic"]) == (0, 4)
    mismatch = run("P/E Mix", {"quantityPhysical": "6"}, ["2", "3"])
    assert mismatch == {"quantityPhysical": "6"}
    both = run("P/E Mix", {"quantityPhysical": "5", "quantityElectronic": "5"}, ["2", "3"])
    assert (both["quantityPhysical"], both["quantityElectronic"]) == (4, 1)
    explicit = run("P/E Mix", {"quantityPhysical": 3, "quantityElectronic": 2}, ["2", "3"])
    assert (explicit["quantityPhysical"], explicit["quantityElectronic"]) == (3, 2)


def test_merged_location_counts_are_mirrored_in_the_cost(mapper):
    def run(order_format, location_ids, total):
        pol = {
            "orderFormat": order_format,
            "cost": {"quantityPhysical": str(total), "quantityElectronic": str(total)},
            "locations": [{"locationId": i, "quantity": str(total)} for i in location_ids],
        }
        composite = {mapper.po_lines_key: [pol]}
        mapper._merge_duplicate_locations(composite)
        mapper._copy_quantity_to_format_field(composite)
        return pol

    physical = run("Physical Resource", "AAB", 3)
    assert [loc["quantity"] for loc in physical["locations"]] == [2, 1]
    assert (physical["cost"]["quantityPhysical"], physical["cost"]["quantityElectronic"]) == (3, 0)
    mixed = run("P/E Mix", "AAABB", 5)
    assert [(loc["quantityPhysical"], loc.get("quantityElectronic")) for loc in mixed["locations"]] == [
        (2, 1),
        (2, None),
    ]
    assert (mixed["cost"]["quantityPhysical"], mixed["cost"]["quantityElectronic"]) == (4, 1)
    # The electronic piece is put on the first location, so a location count of 1 is fine
    single = run("P/E Mix", "AAB", 3)
    assert [(loc["quantityPhysical"], loc.get("quantityElectronic")) for loc in single["locations"]] == [
        (1, 1),
        (1, None),
    ]
    assert (single["cost"]["quantityPhysical"], single["cost"]["quantityElectronic"]) == (2, 1)


@pytest.mark.parametrize(
    "pol, message",
    [
        (_pol(claimingActive=True), "claiming interval"),
        (_pol(claimingActive="true", claimingInterval="0"), "claiming interval"),
        (_pol(receiptStatus="Receipt Not Required"), "receipt not required"),
        (
            _pol(receiptStatus="Receipt Not Required", checkinItems=False),
            "receipt not required",
        ),
        (
            _pol(
                details={"isBinderyActive": True},
                physical={"createInventory": "Instance, Holding, Item"},
            ),
            "binder active requires the Independent",
        ),
        (
            _pol(multiYearPayment=True, paymentTerms={"prepaymentTerm": 2}),
            "prepayment term 2",
        ),
        (
            _pol(
                multiYearPayment=True,
                paymentTerms={"prepaymentTerm": 2, "fiscalYearDistributions": [{}, {}, {}]},
            ),
            "prepayment term 2",
        ),
        (
            _pol(cost={"quantityPhysical": 3, "listUnitPrice": 10, "discount": 50}),
            "estimated price negative",
        ),
    ],
)
def test_validation_rejects_more_of_what_folio_rejects(mapper, pol, message):
    with pytest.raises(TransformationRecordFailedError, match=message):
        _validate(mapper, pol)


def test_independent_workflow_satisfies_receipt_and_binder_rules(mapper):
    mapper = copy.copy(mapper)
    mapper.create_inventory_defaults = {"physical": "Instance, Holding, Item"}
    _validate(
        mapper,
        _pol(
            checkinItems="true",
            receiptStatus="Receipt Not Required",
            details={"isBinderyActive": True},
        ),
    )


def test_valid_claiming_prepayment_and_percentage_discount_pass(mapper):
    _validate(
        mapper,
        _pol(
            claimingActive=True,
            claimingInterval=30,
            multiYearPayment=True,
            paymentTerms={"prepaymentTerm": 2, "fiscalYearDistributions": [{}, {}]},
            cost={
                "quantityPhysical": 3,
                "listUnitPrice": 10,
                "discount": 50,
                "discountType": "percentage",
            },
        ),
    )


def test_ongoing_order_gets_a_minimal_ongoing_object_only_when_none_is_mapped(mapper):
    order = {"orderType": "Ongoing"}
    mapper._ensure_ongoing_info(order)
    assert order["ongoing"] == {"isSubscription": False, "manualRenewal": False}
    mapped = {"orderType": "Ongoing", "ongoing": {"interval": 90}}
    mapper._ensure_ongoing_info(mapped)
    assert mapped["ongoing"] == {"interval": 90}
    one_time = {"orderType": "One-Time"}
    mapper._ensure_ongoing_info(one_time)
    assert "ongoing" not in one_time
    mapped_one_time = {"orderType": "One-Time", "ongoing": {"interval": 365}}
    mapper._ensure_ongoing_info(mapped_one_time)
    assert "ongoing" not in mapped_one_time


def test_fund_distribution_total_is_only_checked_for_open_orders(mapper):
    pol = _pol(fundDistribution=[{"distributionType": "percentage", "value": 60}])
    _validate(mapper, pol, "Pending")
    with pytest.raises(TransformationRecordFailedError, match="do not add up"):
        _validate(mapper, pol, "Open")
    pol["fundDistribution"].append({"distributionType": "percentage", "value": 40})
    _validate(mapper, pol, "Open")


def test_fund_distribution_amounts_are_compared_with_the_estimated_price(mapper):
    # 3 x 10 = 30 estimated
    amounts = [
        {"distributionType": "amount", "value": 10},
        {"distributionType": "amount", "value": 20},
    ]
    _validate(mapper, _pol(fundDistribution=amounts), "Open")
    amounts[1]["value"] = 15
    with pytest.raises(TransformationRecordFailedError, match="do not add up"):
        _validate(mapper, _pol(fundDistribution=amounts), "Open")


def test_zero_price_fund_distributions(mapper):
    free = {"quantityPhysical": 3, "listUnitPrice": 0}
    mixed = [
        {"distributionType": "amount", "value": 0},
        {"distributionType": "percentage", "value": 100},
    ]
    with pytest.raises(TransformationRecordFailedError, match="cannot be mixed"):
        _validate(mapper, _pol(cost=free, fundDistribution=mixed), "Open")
    _validate(mapper, _pol(cost=free, fundDistribution=mixed[:1]), "Open")
    _validate(mapper, _pol(cost=free, fundDistribution=mixed[1:]), "Open")


def _fund_info(restrict=False, locations=(), expense_classes=None):
    return {
        "restrict_by_locations": restrict,
        "locations": set(locations),
        "expense_classes": expense_classes or {},
    }


def test_expense_class_must_be_in_the_funds_active_budget(mapper):
    mapper._fund_info_cache["f1"] = _fund_info(expense_classes={"ec1": "Active"})
    ok = _pol(fundDistribution=[{"fundId": "f1", "expenseClassId": "ec1"}])
    _validate(mapper, ok)
    bad = _pol(fundDistribution=[{"fundId": "f1", "expenseClassId": "ec2", "code": "F1"}])
    with pytest.raises(
        TransformationRecordFailedError, match="not in an Active budget of fund F1"
    ):
        _validate(mapper, bad)


def _with_expense_class_check(mapper, mode):
    variant = copy.copy(mapper)
    variant.task_configuration = types.SimpleNamespace(expense_class_check=mode)
    variant._fund_info_cache = {"f1": _fund_info(expense_classes={"ec1": "Active"})}
    variant.migration_report = copy.copy(mapper.migration_report)
    variant.migration_report.report = {}
    return variant


def test_expense_class_check_warn_keeps_the_record_and_counts_it(mapper):
    variant = _with_expense_class_check(mapper, "warn")
    bad = _pol(fundDistribution=[{"fundId": "f1", "expenseClassId": "ec2", "code": "F1"}])
    _validate(variant, bad)
    assert "OrderValidationWarnings" in variant.migration_report.report
    assert "OrderValidationFailures" not in variant.migration_report.report


def test_expense_class_check_off_skips_the_check(mapper):
    variant = _with_expense_class_check(mapper, "off")
    bad = _pol(fundDistribution=[{"fundId": "f1", "expenseClassId": "ec2", "code": "F1"}])
    _validate(variant, bad)
    assert "OrderValidationWarnings" not in variant.migration_report.report


def test_expense_class_check_fail_is_the_explicit_default(mapper):
    variant = _with_expense_class_check(mapper, "fail")
    bad = _pol(fundDistribution=[{"fundId": "f1", "expenseClassId": "ec2", "code": "F1"}])
    with pytest.raises(TransformationRecordFailedError, match="not in an Active budget"):
        _validate(variant, bad)


def test_inactive_expense_class_only_fails_open_orders(mapper):
    mapper._fund_info_cache["f1"] = _fund_info(expense_classes={"ec1": "Inactive"})
    pol = _pol(
        fundDistribution=[
            {
                "fundId": "f1",
                "expenseClassId": "ec1",
                "distributionType": "percentage",
                "value": 100,
            }
        ]
    )
    _validate(mapper, pol)
    with pytest.raises(TransformationRecordFailedError, match="inactive"):
        _validate(mapper, pol, status="Open")


def test_fund_location_restrictions_only_fail_open_orders_with_other_locations(mapper):
    mapper._fund_info_cache["f1"] = _fund_info(restrict=True, locations=["x"])
    pol = _pol(fundDistribution=[{"fundId": "f1", "distributionType": "percentage", "value": 100}])
    _validate(mapper, pol)  # Pending
    with pytest.raises(TransformationRecordFailedError, match="restricted to locations"):
        _validate(mapper, pol, status="Open")
    allowed = _pol(
        fundDistribution=[{"fundId": "f1", "distributionType": "percentage", "value": 100}],
        locations=[{"locationId": "x", "quantityPhysical": 3}],
    )
    _validate(mapper, allowed, status="Open")
    mapper._fund_info_cache["f2"] = _fund_info(restrict=False, locations=["x"])
    _validate(
        mapper,
        _pol(fundDistribution=[{"fundId": "f2", "distributionType": "percentage", "value": 100}]),
        status="Open",
    )


def test_unreadable_fund_skips_its_checks(mapper):
    mapper._fund_info_cache["f1"] = None
    pol = _pol(
        fundDistribution=[
            {
                "fundId": "f1",
                "expenseClassId": "ec9",
                "distributionType": "percentage",
                "value": 100,
            }
        ]
    )
    _validate(mapper, pol, status="Open")


def test_po_lines_limit(mapper):
    order = {"workflowStatus": "Pending", mapper.po_lines_key: [_pol(), _pol()]}
    mapper.po_lines_limit = 1
    with pytest.raises(TransformationRecordFailedError, match="above the tenant limit of 1"):
        mapper._validate_composite_order("1", order)
    mapper.po_lines_limit = 2
    mapper._validate_composite_order("1", order)
    mapper.po_lines_limit = None


def test_vendor_and_access_provider_must_be_active_for_open_orders(mapper):
    mapper.folio_organizations_by_id = {
        "v1": {"status": "Active", "isVendor": True, "name": "Zed"},
        "v2": {"status": "Inactive", "isVendor": True, "name": "Acme"},
        "v3": {"status": "Active", "isVendor": False, "name": "Beta"},
    }

    def order(vendor, provider=None, status="Open"):
        pol = _pol(eresource={"accessProvider": provider} if provider else {})
        return {"workflowStatus": status, "vendor": vendor, mapper.po_lines_key: [pol]}

    mapper._validate_composite_order("1", order("v1", "v1"))
    mapper._validate_composite_order("1", order("v2", "v2", status="Pending"))
    for vendor, provider, message in [
        ("v2", None, "vendor organization Acme \\(v2\\) is not Active"),
        ("v3", None, "Beta \\(v3\\) is not flagged as a vendor"),
        ("v1", "v2", "access provider Acme \\(v2\\) is not Active"),
        ("v1", "zz", "access provider zz is not an organization"),
    ]:
        with pytest.raises(TransformationRecordFailedError, match=message):
            mapper._validate_composite_order("1", order(vendor, provider))


def test_messages_use_names_for_expense_classes_and_locations(mapper):
    mapper._reference_names["/finance/expense-classes"] = {"ec2": "Electronic"}
    mapper._reference_names["/locations"] = {"x": "Main Library"}
    mapper._fund_info_cache["f1"] = _fund_info(
        restrict=True, locations=["x"], expense_classes={"ec1": "Active"}
    )
    mapper._fund_info_cache["f1"]["code"] = "LIB1"
    dist = {
        "fundId": "f1",
        "expenseClassId": "ec2",
        "distributionType": "percentage",
        "value": 100,
    }
    with pytest.raises(TransformationRecordFailedError) as error:
        _validate(mapper, _pol(fundDistribution=[dist]), status="Open")
    assert "expense class Electronic (ec2) is not in an Active budget of fund LIB1" in str(
        error.value
    )
    assert "Main Library (x)" in str(error.value)


def test_validation_failures_are_counted_by_rule_in_the_report(mapper):
    mapper.migration_report.report.pop("OrderValidationFailures", None)
    bad = _pol(
        cost={"quantityPhysical": 3, "listUnitPrice": -1},
        locations=[{"locationId": "a", "quantityPhysical": 1}],
        claimingActive=True,
    )
    with pytest.raises(TransformationRecordFailedError):
        _validate(mapper, bad)
    section = mapper.migration_report.report["OrderValidationFailures"]
    assert section["Quantities and locations"] >= 1
    assert section["Cost prices and discount"] >= 1
    assert section["Claiming"] == 1
    assert "Other" not in section


UNIT_A = "11111111-1111-4111-8111-111111111111"
UNIT_B = "22222222-2222-4222-8222-222222222222"


def _acq_mapper(mapper, units, member_of=(), permissions=None, user_access="read"):
    mapper_copy = copy.copy(mapper)
    mapper_copy._acq_units_by_id = {u["id"]: u for u in units}
    mapper_copy._acq_unit_ids_by_name = {u["name"].lower(): u["id"] for u in units}
    mapper_copy.acquisition_unit_mapping = None
    if permissions is None:
        permissions = {
            "orders.acquisitions-units-assignments.create.execute",
        }
    mapper_copy._user_acq_access = (
        (set(member_of), set(permissions)) if user_access == "read" else None
    )
    return mapper_copy


def _units(protect_a=True, protect_b=True):
    return [
        {"id": UNIT_A, "name": "Main", "protectCreate": protect_a},
        {"id": UNIT_B, "name": "Special", "protectCreate": protect_b},
    ]


def _order(*unit_ids):
    return {"workflowStatus": "Pending", "acqUnitIds": list(unit_ids), "compositePoLines": []}


def test_acquisition_unit_name_or_uuid_resolves_without_a_map(mapper):
    m = _acq_mapper(mapper, _units())
    assert m._resolve_acquisition_unit(UNIT_B, "1") == UNIT_B
    assert m._resolve_acquisition_unit(" main ", "1") == UNIT_A
    assert m._resolve_acquisition_unit("Nope", "1") == ""
    m.record_map = {"data": [{"folio_field": "acqUnitIds[0]", "legacy_field": "u"}]}
    m.mapped_from_legacy_data = {"acqUnitIds[0]": ["u"]}
    assert m.get_prop({"u": "Nope^-^Main"}, "acqUnitIds[0]", "1", "") == UNIT_A


def test_acquisition_unit_hardcoded_values_are_validated(mapper):
    m = _acq_mapper(mapper, _units())

    def validate_with(value):
        m.record_map = {"data": [{"folio_field": "acqUnitIds[0]", "value": value}]}
        m._validate_hardcoded_acquisition_units()

    validate_with("Main")
    validate_with(UNIT_A)
    validate_with("")
    with pytest.raises(TransformationProcessError):
        validate_with("Nope")
    with pytest.raises(TransformationProcessError):
        validate_with("33333333-3333-4333-8333-333333333333")


def test_acquisition_units_are_resolved_through_get_prop_with_delimiter(mapper):
    m = _acq_mapper(mapper, _units())
    m.record_map = {"data": [{"folio_field": "acqUnitIds[0]", "legacy_field": "u"}]}
    m.mapped_from_legacy_data = {"acqUnitIds[0]": ["u"]}
    assert m.get_prop({"u": "Main^-^special"}, "acqUnitIds[0]", "1", "") == f"{UNIT_A}^-^{UNIT_B}"


def test_acquisition_unit_map_handles_delimited_values(mapper):
    class StubClient:
        def folio_get_all(self, *args, **kwargs):
            return _units()

    m = _acq_mapper(mapper, _units())
    m.acquisition_unit_mapping = RefDataMapping(
        StubClient(),
        "/acquisitions-units/units",
        "acquisitionsUnits",
        [{"legacy_unit": "M", "folio_name": "Main"}, {"legacy_unit": "S", "folio_name": "Special"}, {"legacy_unit": "*", "folio_name": "Main"}],
        "name",
        "AcquisitionUnitMapping",
    )
    m.record_map = {"data": [{"folio_field": "acqUnitIds[0]", "legacy_field": "legacy_unit"}]}
    m.mapped_from_legacy_data = {"acqUnitIds[0]": ["legacy_unit"]}
    assert m.get_prop({"legacy_unit": "M^-^S"}, "acqUnitIds[0]", "1", "") == f"{UNIT_A}^-^{UNIT_B}"


def test_validation_rejects_unknown_or_inactive_units(mapper):
    units = _units()
    units[1]["isDeleted"] = True
    m = _acq_mapper(mapper, units, member_of=[UNIT_A])
    m._validate_composite_order("1", _order(UNIT_A))
    with pytest.raises(TransformationRecordFailedError, match="not active"):
        m._validate_composite_order("1", _order(UNIT_B))
    with pytest.raises(TransformationRecordFailedError, match="does not exist"):
        m._validate_composite_order("1", _order("33333333-3333-4333-8333-333333333333"))


def test_validation_requires_membership_when_every_unit_protects_create(mapper):
    m = _acq_mapper(mapper, _units(), member_of=[UNIT_B])
    m._validate_composite_order("1", _order(UNIT_A, UNIT_B))  # member of one is enough
    with pytest.raises(TransformationRecordFailedError, match="not a member"):
        m._validate_composite_order("1", _order(UNIT_A))
    m_open = _acq_mapper(mapper, _units(protect_b=False), member_of=[])
    m_open._validate_composite_order("1", _order(UNIT_A, UNIT_B))  # one unit is unprotected


def test_validation_bypass_and_missing_permission(mapper):
    create = "orders.acquisitions-units-assignments.create.execute"
    bypass = "orders.acquisition-units.bypass.execute"
    m = _acq_mapper(mapper, _units(), permissions={create, bypass})
    m._validate_composite_order("1", _order(UNIT_A))
    m = _acq_mapper(mapper, _units(), permissions=set(), member_of=[UNIT_A])
    with pytest.raises(TransformationRecordFailedError, match="lacks the permission"):
        m._validate_composite_order("1", _order(UNIT_A))


def test_membership_is_not_checked_when_user_access_cannot_be_read(mapper):
    m = _acq_mapper(mapper, _units(), user_access="unreadable")
    m._validate_composite_order("1", _order(UNIT_A))


class _PermClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def folio_get(self, path, key=None, query="", **kwargs):
        self.calls.append(path)
        self.kwargs = getattr(self, "kwargs", []) + [kwargs]
        if path not in self.responses:
            raise RuntimeError("404")
        return self.responses[path]


def test_permissions_are_read_from_eureka(mapper):
    m = copy.copy(mapper)
    m.folio_client = _PermClient({"/permissions/users/u1": {"userId": "u1", "permissions": ["a.b"]}})
    assert m._read_user_permission_names("u1") == {"a.b"}
    assert m.folio_client.calls == ["/permissions/users/u1"]


def test_permissions_fall_back_to_mod_permissions(mapper):
    m = copy.copy(mapper)
    m.folio_client = _PermClient({"/perms/users/u1/permissions": ["a.b", {"permissionName": "c.d"}]})
    assert m._read_user_permission_names("u1") == {"a.b", "c.d"}
    assert m.folio_client.kwargs[-1]["query_params"] == {"indexField": "userId", "expanded": "true"}
