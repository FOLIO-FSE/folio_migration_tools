import json
import io
from pathlib import Path
from unittest.mock import Mock

from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.mapping_file_transformation.order_mapper import (
    CompositeOrderMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.orders_transformer import OrdersTransformer


def test_merge_into_orders_with_embedded_pols():
    mocked_orders_transformer = Mock(spec=OrdersTransformer)
    mocked_orders_transformer.embedded_extradata_object_cache = set()
    mocked_orders_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_orders_transformer.extradata_writer.cache = []
    mocked_orders_transformer.mapper = Mock(spec=CompositeOrderMapper)
    mocked_orders_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    mocked_orders_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    mocked_orders_transformer.mapper.po_lines_key = "compositePoLines"
    mocked_orders_transformer.current_folio_record = {}

    order_objects = [
        {
            "id": "4b1760d2-7419-5d4c-bf6e-8df9f4cecda4",
            "metadata": {
                "createdDate": "2023-03-17T12:24:38.903",
                "createdByUserId": "",
                "updatedDate": "2023-03-17T12:24:38.903",
                "updatedByUserId": "",
            },
            "approved": False,
            "notes": ["Hello, hello, hello!", "Make it work!"],
            "poNumber": "o124",
            "orderType": "One-Time",
            "reEncumber": False,
            "vendor": "fc54327d-fd60-4f6a-ba37-a4375511b91b",
            "workflowStatus": "Pending",
            "compositePoLines": [
                {
                    "id": "079524d5-db44-4b62-b2ac-40d5e27b8e4a",
                    "instanceId": "ae1daef2-ddea-4d87-a434-3aa98ed3e687",
                    "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                    "cost": {"currency": "USD"},
                    "orderFormat": "Electronic Resource",
                    "source": "API",
                    "titleOrPackage": "Once upon a time...",
                }
            ],
        },
        {
            "id": "4b1760d2-7419-5d4c-bf6e-8df9f4cecda4",
            "metadata": {
                "createdDate": "2023-03-17T12:24:38.913",
                "createdByUserId": "",
                "updatedDate": "2023-03-17T12:24:38.913",
                "updatedByUserId": "",
            },
            "approved": False,
            "notes": ["Purchased at local yard sale."],
            "poNumber": "o124",
            "orderType": "One-Time",
            "reEncumber": False,
            "vendor": "fc54327d-fd60-4f6a-ba37-a4375511b91b",
            "workflowStatus": "Pending",
            "compositePoLines": [
                {
                    "id": "0a05342b-c142-4aa2-93fd-6a42ccf23b6d",
                    "instanceId": "2",
                    "acquisitionMethod": "837d04b6-d81c-4c49-9efd-2f62515999b3",
                    "cost": {"currency": "USD"},
                    "orderFormat": "Electronic Resource",
                    "source": "API",
                    "titleOrPackage": "Sunset Beach: the comic",
                }
            ],
        },
    ]

    # TODO don't write the file
    with io.StringIO() as results_file:
        for order in order_objects:
            mocked_orders_transformer.results_file = results_file
            OrdersTransformer.merge_into_orders_with_embedded_pols(
                mocked_orders_transformer, order
            )

        assert len(mocked_orders_transformer.current_folio_record["compositePoLines"]) == 2


def _write_tsv(path, header, rows):
    path.write_text("\n".join(["\t".join(header)] + ["\t".join(r) for r in rows]) + "\n")


def _build_orders_transformer(tmp_path, extra_task_config, extra_map_files):
    """Build a real OrdersTransformer in a temp folder structure."""
    from folio_migration_tools.library_configuration import (
        FileDefinition,
        FolioRelease,
        LibraryConfiguration,
    )

    from .test_infrastructure import mocked_classes

    base = tmp_path / "migration"
    mapping_folder = base / "mapping_files"
    source_folder = base / "iterations" / "test_iteration" / "source_data" / "orders"
    results_folder = base / "iterations" / "test_iteration" / "results"
    for folder in (mapping_folder, source_folder, results_folder):
        folder.mkdir(parents=True)

    (base / ".gitignore").write_text("")
    orders_map = {
        "data": [
            {"folio_field": "legacyIdentifier", "legacy_field": "order_number", "value": ""},
            {"folio_field": "poNumber", "legacy_field": "order_number", "value": ""},
            {"folio_field": "vendor", "legacy_field": "vendor", "value": ""},
            {"folio_field": "orderType", "legacy_field": "", "value": "One-Time"},
            {"folio_field": "compositePoLines[0].id", "legacy_field": "order_number"},
            {"folio_field": "compositePoLines[0].titleOrPackage", "legacy_field": "title"},
            {"folio_field": "compositePoLines[0].acquisitionMethod", "legacy_field": "acq"},
            {"folio_field": "compositePoLines[0].orderFormat", "legacy_field": "", "value": "Other"},
            {"folio_field": "compositePoLines[0].source", "legacy_field": "", "value": "User"},
            {"folio_field": "compositePoLines[0].cost.currency", "legacy_field": "", "value": "USD"},
            {
                "folio_field": "compositePoLines[0].physical.materialType",
                "legacy_field": "mtype",
                "value": "",
            },
        ]
    }
    for entry in orders_map["data"]:
        entry.setdefault("value", "")
        entry.setdefault("description", "")
    (mapping_folder / "orders_mapping.json").write_text(json.dumps(orders_map))
    _write_tsv(mapping_folder / "acq_methods.tsv", ["acq", "folio_value"], [["p", "Purchase"], ["*", "Other"]])
    _write_tsv(mapping_folder / "funds.tsv", ["fund", "folio_code"], [["*", "FUND"]])
    for name, (header, rows) in extra_map_files.items():
        _write_tsv(mapping_folder / name, header, rows)
    (source_folder / "orders.csv").write_text("order_number,vendor,title,acq,mtype\n1,V,T,p,B\n")

    client = mocked_classes.mocked_folio_client()
    original_get_all = client.folio_get_all
    stubbed = {
        "/material-types": [
            {"id": "mt-book", "name": "book"},
            {"id": "mt-other", "name": "unspecified"},
        ],
        "/finance/funds": [{"id": "fund-1", "code": "FUND"}],
        "/finance/expense-classes": [{"id": "ec-1", "code": "Elec", "name": "Electronic"}],
        "/organizations-storage/organizations": [{"id": "org-1", "code": "EBSCO"}],
    }

    def get_all(path, array_name, *args, **kwargs):
        if path in stubbed:
            return iter(stubbed[path])
        return original_get_all(path, array_name, *args, **kwargs)

    client.folio_get_all = get_all

    lib_config = LibraryConfiguration(
        okapi_url="okapi_url",
        tenant_id="tenant_id",
        okapi_username="username",
        okapi_password="password",  # noqa: S106
        folio_release=FolioRelease.ramsons,
        library_name="Order tester Library",
        log_level_debug=False,
        iteration_identifier="test_iteration",
        base_folder=str(base),
        multi_field_delimiter="^-^",
        add_time_stamp_to_file_names=False,
    )
    task_config = OrdersTransformer.TaskConfiguration(
        name="orders_task",
        migration_task_type="OrdersTransformer",
        files=[FileDefinition(file_name="orders.csv")],
        orders_mapping_file_name="orders_mapping.json",
        acquisition_method_map_file_name="acq_methods.tsv",
        funds_map_file_name="funds.tsv",
        **extra_task_config,
    )
    # The transformer requires a non-empty instance id map
    results_folder.joinpath("instances_id_map.json").write_text(
        json.dumps(["1", "ae1daef2-ddea-4d87-a434-3aa98ed3e687", "1"]) + "\n"
    )
    return OrdersTransformer(task_config, lib_config, client, use_logging=False)


def test_transformer_loads_material_type_map_into_mapper(tmp_path):
    transformer = _build_orders_transformer(
        tmp_path,
        {"material_type_map_file_name": "material_types.tsv"},
        {
            "material_types.tsv": (
                ["mtype", "folio_name"],
                [["B", "book"], ["*", "unspecified"]],
            )
        },
    )

    assert transformer.mapper.material_type_mapping is not None
    result = transformer.mapper.get_prop(
        {"mtype": "B"}, "compositePoLines[0].physical.materialType", "1", ""
    )
    assert result == "mt-book"
    assert "B -> book" in transformer.mapper.migration_report.report["MaterialTypeMapping"]


def test_transformer_without_material_type_map_has_no_material_type_mapping(tmp_path):
    transformer = _build_orders_transformer(tmp_path, {}, {})

    assert transformer.mapper.material_type_mapping is None


def test_transformer_loads_expense_class_map_into_mapper(tmp_path):
    transformer = _build_orders_transformer(
        tmp_path,
        {"funds_expense_class_map_file_name": "expense_classes.tsv"},
        {
            "expense_classes.tsv": (
                ["legacy_ec", "folio_code"],
                [["E", "Elec"], ["*", "Elec"]],
            )
        },
    )

    assert transformer.mapper.expense_class_mapping is not None
    prop = "compositePoLines[0].fundDistribution[0].expenseClassId"
    assert transformer.mapper.get_prop({"legacy_ec": "E"}, prop, "1", "") == "ec-1"
    # No explicit match: left empty, not defaulted to the * row
    assert transformer.mapper.get_prop({"legacy_ec": "other"}, prop, "1", "") == ""
    assert "E -> Elec" in transformer.mapper.migration_report.report["ExpenseClassMapping"]


def test_transformer_without_expense_class_map_has_no_expense_class_mapping(tmp_path):
    transformer = _build_orders_transformer(tmp_path, {}, {})

    assert transformer.mapper.expense_class_mapping is None


def test_transformer_loads_organizations_code_map_into_mapper(tmp_path):
    transformer = _build_orders_transformer(
        tmp_path,
        {"organizations_code_map_file_name": "organizations.tsv"},
        {"organizations.tsv": (["legacy_vendor", "folio_code"], [["V", "EBSCO"]])},
    )

    assert transformer.mapper.organizations_code_map == {"v": "EBSCO"}
    assert transformer.mapper.get_folio_organization_uuid("1", "V") == "org-1"
    report = transformer.mapper.migration_report.report
    assert "V -> EBSCO" in report["OrganizationsMapping"]


def test_transformer_without_organizations_code_map_uses_folio_codes(tmp_path):
    transformer = _build_orders_transformer(tmp_path, {}, {})

    assert transformer.mapper.organizations_code_map is None
    assert transformer.mapper.get_folio_organization_uuid("1", "EBSCO") == "org-1"
