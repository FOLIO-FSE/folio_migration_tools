import logging

from folio_migration_tools.migration_tasks.user_transformer import UserTransformer
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
