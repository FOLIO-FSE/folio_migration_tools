import logging

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.user_transformer import UserTransformer

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_get_object_type():
    assert UserTransformer.get_object_type() == FOLIONamespaces.users


def test_clean_user():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "primaryAddress": False},
                {"id": "some other id", "primaryAddress": False},
            ]
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    assert any(a["primaryAddress"] for a in folio_user["personal"]["addresses"])


def test_clean_user_all_true():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "primaryAddress": True},
                {"id": "some other id", "primaryAddress": True},
            ]
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    assert any(a["primaryAddress"] is not True for a in folio_user["personal"]["addresses"])

def test_clean_user_no_primary_info():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id"},
                {"id": "some other id"},
            ]
        }
    }
    UserTransformer.clean_user(folio_user, "id")
    assert any(a["primaryAddress"] is not True for a in folio_user["personal"]["addresses"])
