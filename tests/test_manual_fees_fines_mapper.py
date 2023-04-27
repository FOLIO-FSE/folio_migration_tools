import json
import logging
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.manual_fees_fines_mapper import (
    ManualFeesFinesMapper,
)
from folio_migration_tools.migration_tasks.manual_fees_fines_transformer import (
    ManualFeesFinesTransformer,
)
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> ManualFeesFinesMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    print("init")
    mock_folio = mocked_classes.mocked_folio_client()

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
    mocked_config = Mock(spec=ManualFeesFinesTransformer.TaskConfiguration)

    return ManualFeesFinesMapper(
        mock_folio,
        basic_feesfines_map,
        feesfines_owner_map,
        feesfines_type_map,
        library_config,
        mocked_config,
    )


def test_schema(mapper: ManualFeesFinesMapper, caplog):
    schema = mapper.get_composite_feefine_schema()
    assert schema


def test_basic_mapping(mapper: ManualFeesFinesMapper, caplog):
    data = {
        "total_amount": "100",
        "remaining_amount": "50",
        "patron_barcode": "213",
        "item_barcode": "546",
        "billed_date": "2023-01-02",
        "lending_library": "library1",
        "type": "spill"
    }

    res = mapper.do_map(data, 1, FOLIONamespaces.account)

    assert res


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
