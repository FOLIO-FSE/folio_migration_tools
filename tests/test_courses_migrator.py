from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.courses_migrator import CoursesMigrator


def test_get_object_type():
    assert CoursesMigrator.get_object_type() == FOLIONamespaces.course
