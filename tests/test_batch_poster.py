from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster


def test_get_object_type():
    assert BatchPoster.get_object_type() == FOLIONamespaces.other
