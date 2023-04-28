import io
from pathlib import Path
from unittest.mock import Mock

from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.mapping_file_transformation.manual_fee_fines_mapper import (
    ManualFeeFinesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.manual_fee_fines_transformer import (
    ManualFeeFinesTransformer,
)


def test_merge_into_orders_with_embedded_pols():
    mocked_feefines_transformer = Mock(spec=ManualFeeFinesMapper)
    mocked_feefines_transformer.embedded_extradata_object_cache = set()
    mocked_feefines_transformer.extradata_writer = ExtradataWriter(Path(""))
    mocked_feefines_transformer.extradata_writer.cache = []
    mocked_feefines_transformer.mapper = Mock(spec=ManualFeeFinesMapper)
    mocked_feefines_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    mocked_feefines_transformer.mapper.migration_report = Mock(spec=MigrationReport)
    mocked_feefines_transformer.current_folio_record = {}

    feefines_objects = [
        {
            "account": {
                "amount": "100",
                "remaining": "50",
                "paymentStatus": {"name": "Outstanding"},
                "userId": "213",
                "itemId": "546",
                "feeFineId": "031836ec-521a-4493-9f76-0e02c2e7d241",
                "ownerId": "5abfff3f-50eb-432a-9a43-21f8f7a70194",
                "id": "48c4ce63-1f9f-4440-acf7-6aa3ac281c9b",
            },
            "feefineaction": {
                "dateAction": "2023-01-02",
                "accountId": "48c4ce63-1f9f-4440-acf7-6aa3ac281c9b",
                "userId": "213",
            },
        },
        {
            "account": {
                "amount": "20",
                "remaining": "20",
                "paymentStatus": {"name": "Outstanding"},
                "userId": "213",
                "itemId": "546",
                "feeFineId": "031836ec-521a-4493-9f76-0e02c2e7d241",
                "ownerId": "5abfff3f-50eb-432a-9a43-21f8f7a70194",
                "id": "f9cfd725-9c97-4646-ae4f-b7edaf96b34f",
            },
            "feefineaction": {
                "dateAction": "2023-04-05",
                "accountId": "f9cfd725-9c97-4646-ae4f-b7edaf96b34f",
                "userId": "213",
            },
        },
    ]

    for object in feefines_objects:
        res = mocked_feefines_transformer.mapper.store_objects(mocked_feefines_transformer, object)

        assert res
