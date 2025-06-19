import logging

from folio_migration_tools.migration_tasks.user_transformer import (
    UserTransformer,
    remove_empty_addresses,
    find_primary_addresses,
)
from folio_uuid.folio_namespaces import FOLIONamespaces

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_get_object_type():
    assert UserTransformer.get_object_type() == FOLIONamespaces.users


def test_clean_user_all_false():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "addressLine1": "addressLine1", "primaryAddress": False},
                {"id": "some other id", "addressLine1": "addressLine1", "primaryAddress": False},
            ],
            "metadata": "hm",
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    primary_true = [
        a
        for a in folio_user["personal"]["addresses"]
        if isinstance(a["primaryAddress"], bool) and a["primaryAddress"] is True
    ]
    assert len(primary_true) == 1


def test_clean_user_all_false_one_string():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "addressLine1": "addressLine1", "primaryAddress": False},
                {"id": "some other id", "addressLine1": "addressLine1", "primaryAddress": "False"},
            ],
            "metadata": "hm",
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    primary_true = [
        a
        for a in folio_user["personal"]["addresses"]
        if isinstance(a["primaryAddress"], bool) and a["primaryAddress"] is True
    ]
    primary_false = [
        a
        for a in folio_user["personal"]["addresses"]
        if a.get("primaryAddress") is False
    ]
    assert len(primary_true) == 1
    assert len(primary_false) == 1


def test_clean_user_all_true():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "addressLine1": "addressLine1", "primaryAddress": True},
                {"id": "some other id", "addressLine1": "addressLine1", "primaryAddress": "True"},
            ]
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    primary_true = [
        a
        for a in folio_user["personal"]["addresses"]
        if isinstance(a["primaryAddress"], bool) and a["primaryAddress"] is True
    ]
    assert len(primary_true) == 1


def test_clean_user_no_primary_info():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "addressLine1": "addressLine1"},
                {"id": "some other id", "addressLine1": "addressLine1"},
            ]
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    primary_true = [
        a
        for a in folio_user["personal"]["addresses"]
        if isinstance(a["primaryAddress"], bool) and a["primaryAddress"] is True
    ]
    assert len(primary_true) == 1


def test_clean_user_no_real_address_data():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "addressLine1": "addressLine1"},
                {"id": "some other id", "primaryAddress": True, "addressTypeId": "some type"},
            ]
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    primary_true = [
        a
        for a in folio_user["personal"]["addresses"]
        if isinstance(a["primaryAddress"], bool) and a["primaryAddress"] is True
    ]
    assert len(folio_user["personal"]["addresses"]) == 1
    assert len(primary_true) == 1


def test_clean_user_no_valid_address_data():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "primaryAddress": False, "addressTypeId": "some other type"},
                {"id": "some other id", "primaryAddress": True, "addressTypeId": "some type"},
            ]
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    assert "addresses" not in folio_user["personal"]


def test_remove_empty_addresses_removes_empty():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "1", "addressTypeId": "type1", "primaryAddress": True},
                {"id": "2", "addressLine1": "123 Main St", "primaryAddress": False},
                {"id": "3", "addressLine2": "Apt 4", "addressTypeId": "type2"},
                {"id": "4", "primaryAddress": False, "addressTypeId": "type3"},
            ]
        }
    }
    result = remove_empty_addresses(folio_user)
    # Only address 2 and 3 have fields other than id, addressTypeId, primaryAddress
    assert len(result) == 2
    assert any(a["id"] == "2" for a in result)
    assert any(a["id"] == "3" for a in result)


def test_remove_empty_addresses_no_addresses():
    folio_user = {"personal": {}}
    result = remove_empty_addresses(folio_user)
    assert result == []


def test_remove_empty_addresses_all_valid():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "1", "addressLine1": "A", "primaryAddress": True},
                {"id": "2", "addressLine2": "B", "primaryAddress": False},
            ]
        }
    }
    result = remove_empty_addresses(folio_user)
    assert len(result) == 2


def test_find_primary_addresses_sets_missing_to_false():
    addresses = [
        {"addressLine1": "A"},
        {"addressLine1": "B", "primaryAddress": True},
        {"addressLine1": "C", "primaryAddress": "True"},
        {"addressLine1": "D", "primaryAddress": "False"},
    ]
    primary = find_primary_addresses(addresses)
    assert len(primary) == 2
    assert addresses[0]["primaryAddress"] is False
    assert addresses[3]["primaryAddress"] is False


def test_find_primary_addresses_handles_bool_and_str():
    addresses = [
        {"addressLine1": "A", "primaryAddress": True},
        {"addressLine1": "B", "primaryAddress": "true"},
        {"addressLine1": "C", "primaryAddress": False},
        {"addressLine1": "D", "primaryAddress": "False"},
    ]
    primary = find_primary_addresses(addresses)
    assert len(primary) == 2
    for addr in addresses:
        if addr["addressLine1"] in ["A", "B"]:
            assert addr["primaryAddress"] is True or addr["primaryAddress"] == "true"
        else:
            assert addr["primaryAddress"] is False


def test_find_primary_addresses_all_missing():
    addresses = [
        {"addressLine1": "A"},
        {"addressLine1": "B"},
    ]
    primary = find_primary_addresses(addresses)
    assert len(primary) == 0
    for addr in addresses:
        assert addr["primaryAddress"] is False
