import logging

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
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
    with pytest.raises(TransformationProcessError):
        UserTransformer.clean_user(folio_user, "id")


def test_clean_user_all_true():
    folio_user = {
        "personal": {
            "addresses": [
                {"id": "some id", "primaryAddress": True},
                {"id": "some other id", "primaryAddress": True},
            ]
        }
    }
    with pytest.raises(TransformationProcessError):
        UserTransformer.clean_user(folio_user, "id")
