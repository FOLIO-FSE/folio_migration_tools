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
    results_file = io.StringIO("")
    for idx, order in enumerate(order_objects):
        OrdersTransformer.merge_into_orders_with_embedded_pols(
            mocked_orders_transformer, order, results_file
        )

    assert len(mocked_orders_transformer.current_folio_record["compositePoLines"]) == 2
