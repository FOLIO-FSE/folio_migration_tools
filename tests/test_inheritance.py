from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer
from folio_migration_tools.migration_tasks.migration_task_base import (
    MigrationTaskBase
    )


def test_subclass_inheritance():
    assert issubclass(ItemsTransformer, MigrationTaskBase)