from pathlib import Path

import pytest

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase


def test_load_id_map():
    with pytest.raises(TransformationProcessError, match=r"Legacy id map is empty"):
        MigrationTaskBase.load_id_map("tests/test_data/empty_id_file.json", True)


def test_list_source_files():
    with pytest.raises(
        TransformationProcessError,
        match=r"None of the files listed in task configuration found in .*",
    ):
        MigrationTaskBase.check_source_files(Path("./tests/test_data/default/"), [])


def test_list_source_files_one_file():
    with pytest.raises(
        TransformationProcessError, match=r"Some files listed in task configuration not found in"
    ):
        MigrationTaskBase.check_source_files(
            Path("./tests/test_data/default/"),
            [FileDefinition(file_name="isbn_c.xml"), FileDefinition(file_name="isbn_n.xml")],
        )
