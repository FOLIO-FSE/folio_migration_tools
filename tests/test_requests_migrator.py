from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.requests_migrator import RequestsMigrator


def test_get_object_type():
    assert RequestsMigrator.get_object_type() == FOLIONamespaces.requests
