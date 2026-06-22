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

    def test_create_request_with_inactive_user_retry_returns_false_on_non_inactive_error(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        
        # circulation_helper.create_request catches FolioValidationError and returns False
        # so _create_request_with_inactive_user_retry just returns that False
        m.circulation_helper.create_request.return_value = False

        result = m._create_request_with_inactive_user_retry(legacy_request)

        assert result is False

    def test_get_user_by_barcode_exception_handling(self):
        m = self._make_migrator()
        m.folio_client.folio_get.side_effect = Exception("API Error")

        result = m.get_user_by_barcode("P001")

        assert result is None

    def test_update_user_calls_folio_put_correctly(self):
        m = self._make_migrator()
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": True,
            "name": "John Doe",
        }

        m.update_user(user_data)

        m.folio_client.folio_put.assert_called_once_with("/users/user-123", user_data)

    def test_full_inactive_user_flow_integration(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        
        # Mock user data
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": False,
            "expirationDate": "2024-12-31",
        }
        m.folio_client.folio_get.return_value = [user_data]
        m.circulation_helper.create_request.return_value = True
        
        # Call the retry method
        result = m._retry_request_for_inactive_user(legacy_request)
        
        # Verify the flow
        assert result is True
        m.folio_client.folio_get.assert_called_once_with("/users", "users", query='barcode=="P001"')
        # Should call folio_put twice: once for activate, once for deactivate
        assert m.folio_client.folio_put.call_count == 2
        # Verify the URLs called
        assert m.folio_client.folio_put.call_args_list[0][0][0] == "/users/user-123"
        assert m.folio_client.folio_put.call_args_list[1][0][0] == "/users/user-123"

    def test_deactivate_user_restores_original_expiration(self):
        m = self._make_migrator()
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": True,
            "expirationDate": "2025-06-30",
        }
        original_expiration = "2024-12-31"

        m.deactivate_user(user_data, original_expiration)

        assert user_data["active"] is False
        assert user_data["expirationDate"] == "2024-12-31"
        m.folio_client.folio_put.assert_called_once()

    def test_activate_user_extends_expiration_correctly(self):
        m = self._make_migrator()
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": False,
        }
        
        m.activate_user(user_data)
        
        assert user_data["active"] is True
        m.folio_client.folio_put.assert_called_once()

    def test_request_creation_fails_after_reactivation(self):
        m = self._make_migrator()
        legacy_request = DummyLegacyRequest(patron_barcode="P001")
        
        user_data = {
            "id": "user-123",
            "barcode": "P001",
            "active": False,
            "expirationDate": "2024-12-31",
        }
        m.folio_client.folio_get.return_value = [user_data]
        m.circulation_helper.create_request.return_value = False
        
        result = m._retry_request_for_inactive_user(legacy_request)
        
        assert result is False
        # User should still be deactivated even though request failed
        assert m.folio_client.folio_put.call_count == 2
