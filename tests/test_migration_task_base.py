from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.custom_exceptions import TransformationProcessError
import pytest


def test_load_id_map():
    with pytest.raises(TransformationProcessError):
        MigrationTaskBase.load_id_map("tests/test_data/empty_id_file.json", True)
