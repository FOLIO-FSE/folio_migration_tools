import sys
print(sys.path)

with open('folio_migration_tools.migration_tasks.migration_task_base.py', 'w') as outfile:
    outfile.write(str(sys.path))
with open('folio_migration_tools.migration_tasks.organization_transformere.py', 'w') as outfile2:
    outfile2.write(str(sys.path))

from folio_migration_tools.migration_tasks.organization_transformer import (
    OrganizationTransformer
    )
from folio_migration_tools.migration_tasks.migration_task_base import (
    MigrationTaskBase
    )



def test_subclass_inheritance():
    assert issubclass(OrganizationTransformer, MigrationTaskBase)