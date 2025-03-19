import pytest

from folioclient import FolioClient
from folio_migration_tools.transaction_migration.legacy_reserve import LegacyReserve
from folio_migration_tools.test_infrastructure import mocked_classes
from folio_migration_tools.custom_exceptions import TransformationProcessError


@pytest.fixture(scope="session", autouse=True)
def mocked_folio_client(pytestconfig):
    return mocked_classes.mocked_folio_client()


def test_to_dict_happy_path(mocked_folio_client: FolioClient):
    legacy_request_dict = {
        "legacy_identifier": "123456",
        "item_barcode": "78901234"
    }

    legacy_reserve = LegacyReserve(legacy_request_dict, mocked_folio_client)

    expected_output = {
        "courseListingId": legacy_reserve.course_listing_id,
        "copiedItem": {"barcode": "78901234"},
        "id": legacy_reserve.id
    }

    assert legacy_reserve.to_dict() == expected_output


def test_to_dict_missing_keys(mocked_folio_client: FolioClient):
    legacy_request_dict = {
        "some_other_header": "spam",
        "item_barcode": "78901234"
    }

    with pytest.raises(TransformationProcessError) as exc_info:
        _legacy_reserve = LegacyReserve(legacy_request_dict, mocked_folio_client)

    assert str(exc_info.value) == (
        "Critical Process issue. Check configuration, mapping files and reference data"
        "\t0"
        "\tMissing header in file. The following are required:"
        "\tlegacy_identifier, item_barcode"
    )
