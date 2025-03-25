import csv
import io
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


def test_verify_ref_data_mapping_file_structure_invalid():
    with io.StringIO(
        "legacy_code\tfolio_code\n\nstacks\tstacks\nref\tref\t\n"
    ) as ref_data:
        ref_data.name = "test.tsv"
        ref_data.readline()
        current_pos = ref_data.tell()
        with pytest.raises(
            TransformationProcessError, match=r"Mapping file test.tsv has rows with different number of columns \(Rows 2, 4\)"
        ):
            MigrationTaskBase.verify_ref_data_mapping_file_structure(
                ref_data
            )
        assert ref_data.tell() == current_pos

    with io.StringIO(
        "legacy_code\tfolio_code\nstacks\tstacks\nref\tref\t\nreserve\treserve\treserve\n"
    ) as ref_data_multi:
        ref_data_multi.name = "test2.tsv"
        ref_data_multi.readline()
        current_pos_multi = ref_data_multi.tell()
        with pytest.raises(
            TransformationProcessError, match=r"Mapping file test2.tsv has rows with different number of columns \(Rows 3, 4\)"
        ):
            MigrationTaskBase.verify_ref_data_mapping_file_structure(
                ref_data_multi
            )
        assert ref_data_multi.tell() == current_pos_multi


def test_verify_ref_data_mapping_file_structure_valid():
    with io.StringIO(
        "legacy_code\tfolio_code\nstacks\tstacks\nref\tref\n"
    ) as ref_data:
        ref_data.name = "test.tsv"
        ref_data.readline()
        current_pos = ref_data.tell()
        MigrationTaskBase.verify_ref_data_mapping_file_structure(
            ref_data
        )
        assert ref_data.tell() == current_pos

    with io.StringIO(
        "legacy_code\tfolio_code\nstacks\tstacks\nref\tref"
    ) as ref_data_no_newline:
        ref_data_no_newline.name = "test.tsv"
        ref_data_no_newline.readline()
        current_pos = ref_data_no_newline.tell()
        MigrationTaskBase.verify_ref_data_mapping_file_structure(
            ref_data_no_newline
        )
        assert ref_data_no_newline.tell() == current_pos


def test_verify_ref_data_mapping_file_structure_empty():
    with io.StringIO("legacy_code\tfolio_code\n") as ref_data:
        ref_data.name = "test.tsv"
        ref_data.readline()
        current_pos = ref_data.tell()
        with pytest.raises(
            TransformationProcessError, match=r"Map has no rows: test.tsv"
        ):
            MigrationTaskBase.verify_ref_data_mapping_file_structure(
                ref_data
            )
        assert ref_data.tell() == current_pos


def test_load_ref_data_mapping_file():
    csv.register_dialect("tsv", delimiter="\t")
    valid_file = Path("tests/test_data/ref_data_maps/locations_map_valid.tsv")
    invalid_too_many_columns = Path(
        "tests/test_data/ref_data_maps/locations_map_too_many_columns.tsv"
    )
    invalid_empty = Path("tests/test_data/ref_data_maps/locations_map_empty.tsv")
    with pytest.raises(
        TransformationProcessError,
        match=r"Mapping file .* has rows with different number of columns \(Rows 3, 4\)",
    ):
        MigrationTaskBase.load_ref_data_mapping_file("permanentLocationId", invalid_too_many_columns, ["permanentLocationId"])
    with pytest.raises(
        TransformationProcessError, match=r"Map has no rows: .*",
    ):
        MigrationTaskBase.load_ref_data_mapping_file("permanentLocationId", invalid_empty, ["permanentLocationId"])
    ref_data_map = MigrationTaskBase.load_ref_data_mapping_file("permanentLocationId", valid_file, ["permanentLocationId"])
    assert ref_data_map == [{'folio_code': 'STACKS', 'legacy_code': 'stacks'}, {'folio_code': 'VAULT', 'legacy_code': 'vault'}, {'folio_code': 'REF', 'legacy_code': 'ref'}]
