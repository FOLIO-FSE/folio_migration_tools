import json
import logging
from pathlib import Path
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.manual_fee_fines_mapper import (
    ManualFeeFinesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.manual_fee_fines_transformer import (
    ManualFeeFinesTransformer,
)
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> ManualFeeFinesMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    print("init")
    mock_folio_client = mocked_classes.mocked_folio_client()

    library_config = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.orchid,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    feesfines_owner_map = [
        {"lending_library": "library1", "folio_owner": "The Best Fee Fine Owner"},
        {"lending_library": "library2", "folio_owner": "The Other Fee Fine Owner"},
        {"lending_library": "*", "folio_owner": "The Other Fee Fine Owner"},
    ]
    feesfines_type_map = [
        {"type": "spill", "folio_feeFineType": "Coffee spill"},
        {"type": "*", "folio_feeFineType": "Replacement library card"},
    ]
    mock_task_config = Mock(spec=ManualFeeFinesTransformer.TaskConfiguration)

    return ManualFeeFinesMapper(
        mock_folio_client,
        library_config,
        mock_task_config,
        basic_feesfines_map,
        feesfines_owner_map,
        feesfines_type_map,
        ignore_legacy_identifier=True,
    )


def test_schema(mapper: ManualFeeFinesMapper):
    schema = mapper.get_composite_feefine_schema()
    assert schema


def test_basic_mapping(mapper: ManualFeeFinesMapper):
    data = {
        "total_amount": "100",
        "remaining_amount": "50",
        "patron_barcode": "some barcode",
        "item_barcode": "some barcode",
        "billed_date": "2023-01-02",
        "lending_library": "library1",
        "type": "spill",
    }

    res, uuid = mapper.do_map(data, 1, FOLIONamespaces.feefines)

    assert res["feefineaction"]["accountId"] == res["account"]["id"]
    assert res["account"]["amount"] == "100"
    assert res["account"]["userId"] == "a FOLIO user uuid"
    assert res["account"]["itemId"] == "a FOLIO item uuid"
    assert res["account"]["feeFineId"] == "031836ec-521a-4493-9f76-0e02c2e7d241"
    assert res["account"]["ownerId"] == "5abfff3f-50eb-432a-9a43-21f8f7a70194"
    assert res["feefineaction"]["userId"] == "a FOLIO user uuid"
    assert res["feefineaction"]["dateAction"] == "2023-01-02"


def test_perform_additional_mapping(mapper: ManualFeeFinesMapper):
    data = {
        "account": {
            "amount": "100",
            "remaining": "50",
            "paymentStatus": {"name": "Outstanding"},
            "userId": "213",
            "itemId": "546",
            "feeFineId": "031836ec-521a-4493-9f76-0e02c2e7d241",
            "ownerId": "5abfff3f-50eb-432a-9a43-21f8f7a70194",
        },
        "feefineaction": {"dateAction": "2023-01-02", "accountId": "account_id", "userId": "213"},
    }

    res = mapper.perform_additional_mapping(data)

    assert res


def test_store_objects(mapper: ManualFeeFinesMapper):
    mocked_feefines_mapper = Mock(spec=ManualFeeFinesMapper)
    mocked_feefines_mapper.embedded_extradata_object_cache = set()
    mocked_feefines_mapper.extradata_writer = ExtradataWriter(Path(""))
    mocked_feefines_mapper.extradata_writer.cache = []
    mocked_feefines_mapper.migration_report = Mock(spec=MigrationReport)
    mocked_feefines_mapper.migration_report = Mock(spec=MigrationReport)
    mocked_feefines_mapper.current_folio_record = {}

    feefines_tuples = [
        (
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
            "124",
        ),
        (
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
            "456",
        ),
    ]

    for tuple in feefines_tuples:
        ManualFeeFinesMapper.store_objects(mocked_feefines_mapper, tuple)

    assert str(mocked_feefines_mapper.extradata_writer.cache).count("account\\t") == 2
    assert str(mocked_feefines_mapper.extradata_writer.cache).count("feefineaction\\t") == 2


basic_feesfines_map = {
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "account.amount",
            "legacy_field": "total_amount",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "account.remaining",
            "legacy_field": "remaining_amount",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "account.paymentStatus.name",
            "legacy_field": "",
            "value": "Outstanding",
            "description": "",
        },
        {
            "folio_field": "account.userId",
            "legacy_field": "patron_barcode",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "account.itemId",
            "legacy_field": "item_barcode",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "account.feeFineId",
            "legacy_field": "type",
            "value": "",
            "description": "This is the feefine type.",
        },
        {
            "folio_field": "account.ownerId",
            "legacy_field": "lending_library",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "feefineaction.accountId",
            "legacy_field": "",
            "value": "account_id",
            "description": "",
        },
        {
            "folio_field": "feefineaction.userId",
            "legacy_field": "patron_barcode",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "feefineaction.dateAction",
            "legacy_field": "billed_date",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "feefineaction.comments",
            "legacy_field": "legacy_comment",
            "value": "",
            "description": "",
        },
    ]
}
