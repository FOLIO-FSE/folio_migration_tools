import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID

from folio_migration_tools.transaction_migration.legacy_reserve import LegacyReserve
from folio_migration_tools.custom_exceptions import TransformationProcessError


def test_to_dict_happy_path():
    legacy_request_dict = {
        "legacy_identifier": "123456",
        "item_barcode": "78901234"
    }
    legacy_reserve = LegacyReserve(legacy_request_dict, "test_tenant")

    expected_output = {
        "courseListingId": legacy_reserve.course_listing_id,
        "copiedItem": {"barcode": "78901234"},
        "id": legacy_reserve.id
    }
    assert legacy_reserve.to_dict() == expected_output


def test_to_dict_missing_keys():
    legacy_request_dict = {
        "some_other_header": "spam",
        "item_barcode": "78901234"
    }
    with pytest.raises(TransformationProcessError) as exc_info:
        _legacy_reserve = LegacyReserve(legacy_request_dict, "test_tenant")

    expected_output = (
        "Critical Process issue. Check configuration, mapping files and reference data"
        "\t0"
        "\tMissing header in file. The following are required:"
        "\tlegacy_identifier, item_barcode"
    )
    assert str(exc_info.value) == expected_output


def test_to_source_dict_uses_input_headers():
    legacy_reserve = LegacyReserve(
        {"legacy_identifier": " 123456 ", "item_barcode": "78901234"}, "test_tenant"
    )

    assert legacy_reserve.to_source_dict() == {
        "legacy_identifier": "123456",
        "item_barcode": "78901234",
    }


def test_course_listing_id_uses_uuid_base_string():
    legacy_reserve = LegacyReserve(
        {"legacy_identifier": "123456", "item_barcode": "78901234"}, "test_tenant"
    )

    assert legacy_reserve.course_listing_id == str(
        FolioUUID("test_tenant", FOLIONamespaces.course_listing, "123456")
    )
