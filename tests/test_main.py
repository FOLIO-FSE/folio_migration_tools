from folio_migration_tools import __main__
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase


def test_inheritance():
    inheritors = [f.__name__ for f in __main__.inheritors(MigrationTaskBase)]
    assert "HoldingsMarcTransformer" in inheritors
    assert "CoursesMigrator" in inheritors
    assert "UserTransformer" in inheritors
    assert "LoansMigrator" in inheritors
    assert "ItemsTransformer" in inheritors
    assert "HoldingsCsvTransformer" in inheritors
    assert "RequestsMigrator" in inheritors
    assert "OrganizationTransformer" in inheritors
    assert "ReservesMigrator" in inheritors
    assert "BibsTransformer" in inheritors
    assert "BatchPoster" in inheritors
    assert "AuthorityTransformer" in inheritors
