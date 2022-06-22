from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.reserves_migrator import ReservesMigrator


def test_get_object_type():
    assert ReservesMigrator.get_object_type() == FOLIONamespaces.reserve
