from folio_migration_tools.migration_tasks.organization_transformer import OrganizationTransformer
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase


def test_subclass_inheritance():
    assert issubclass(OrganizationTransformer, MigrationTaskBase)
