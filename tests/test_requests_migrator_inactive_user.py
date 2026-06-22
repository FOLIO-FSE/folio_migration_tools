"""Tests for inactive user handling in RequestsMigrator."""

from unittest.mock import Mock, patch

import pytest
from folioclient import FolioValidationError

from folio_migration_tools.migration_tasks.requests_migrator import RequestsMigrator
from folio_migration_tools.transaction_migration.legacy_request import LegacyRequest


class DummyLegacyRequest:
    def __init__(
        self,
        item_barcode: str = "item-1",
        patron_barcode: str = "patron-1",
    ):
        self.item_barcode = item_barcode
        self.patron_barcode = patron_barcode
        self.request_date = 1

    def serialize(self):
        return {
            "itemBarcode": self.item_barcode,
            "userBarcode": self.patron_barcode,
        }


class TestInactiveUserHandling:
    def _make_migrator(self):
        m = Mock(spec=RequestsMigrator)
        m.folio_client = Mock()
        m.circulation_helper = Mock()
        m.migration_report = Mock()
        m.get_user_by_barcode = RequestsMigrator.get_user_by_barcode.__get__(
            m, RequestsMigrator
        )
        m.activate_user = RequestsMigrator.activate_user.__get__(m, RequestsMigrator)
        m.deactivate_user = RequestsMigrator.deactivate_user.__get__(m, RequestsMigrator)
        m.update_user = RequestsMigrator.update_user.__get__(m, RequestsMigrator)
        m._retry_request_for_inactive_user = RequestsMigrator._retry_request_for_inactive_user.__get__(
            m, RequestsMigrator
        )
        m._create_request_with_inactive_user_retry = (
            RequestsMigrator._create_request_with_inactive_user_retry.__get__(
                m, RequestsMigrator
            )
        )
        return m

    def test_get_user_by_barcode_found(self):
        m = self._make_migrator()
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": True,
            "expirationDate": "2025-01-01",
        }
        m.folio_client.folio_get.return_value = [user_data]

        result = m.get_user_by_barcode("P001")

        assert result == user_data
        m.folio_client.folio_get.assert_called_once_with("/users", "users", query='barcode=="P001"')

    def test_get_user_by_barcode_not_found(self):
        m = self._make_migrator()
        m.folio_client.folio_get.return_value = []

        result = m.get_user_by_barcode("P001")

        assert result is None

    def test_activate_user(self):
        m = self._make_migrator()
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": False,
        }

        m.activate_user(user_data)

        assert user_data["active"] is True
        m.folio_client.folio_put.assert_called_once()
        m.migration_report.add.assert_called_once()

    def test_deactivate_user(self):
        m = self._make_migrator()
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": True,
            "expirationDate": "2025-12-31",
        }

        m.deactivate_user(user_data, "2024-12-31")

        assert user_data["active"] is False
        assert user_data["expirationDate"] == "2024-12-31"
        m.folio_client.folio_put.assert_called_once()

    def test_retry_request_for_inactive_user_success(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": False,
            "expirationDate": "2024-12-31",
        }
        m.folio_client.folio_get.return_value = [user_data]
        m.circulation_helper.create_request.return_value = True

        result = m._retry_request_for_inactive_user(legacy_request)

        assert result is True
        m.circulation_helper.create_request.assert_called_once_with(
            m.folio_client, legacy_request, m.migration_report
        )

    def test_retry_request_for_inactive_user_no_user_found(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        m.folio_client.folio_get.return_value = []

        result = m._retry_request_for_inactive_user(legacy_request)

        assert result is False

    def test_create_request_with_inactive_user_retry_initial_success(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        m.circulation_helper.create_request.return_value = True

        result = m._create_request_with_inactive_user_retry(legacy_request)

        assert result is True
        m.circulation_helper.create_request.assert_called_once()

    def test_create_request_with_inactive_user_retry_catches_inactive_user_error(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        
        # Mock the FolioValidationError response
        error_response = Mock()
        error_response.json.return_value = {
            "errors": [{"message": "Inactive users cannot make requests"}]
        }
        error_request = Mock()
        m.folio_client.handle_json_response.return_value = {
            "errors": [{"message": "Inactive users cannot make requests"}]
        }
        fve = FolioValidationError(request=error_request, response=error_response)
        m.circulation_helper.create_request.side_effect = [
            fve,
            True,
        ]
        
        # Mock the retry to succeed
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": False,
            "expirationDate": "2024-12-31",
        }
        m.folio_client.folio_get.return_value = [user_data]

        result = m._create_request_with_inactive_user_retry(legacy_request)

        assert result is True

    def test_create_request_with_inactive_user_retry_reraises_other_errors(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        
        # Mock a different error
        error_response = Mock()
        error_response.json.return_value = {
            "errors": [{"message": "Some other error"}]
        }
        error_request = Mock()
        m.folio_client.handle_json_response.return_value = {
            "errors": [{"message": "Some other error"}]
        }
        fve = FolioValidationError(request=error_request, response=error_response)
        m.circulation_helper.create_request.side_effect = fve

        with pytest.raises(FolioValidationError):
            m._create_request_with_inactive_user_retry(legacy_request)
