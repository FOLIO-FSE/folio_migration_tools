import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.migration_tasks.instance_csv_transformer import (
    InstanceCsvTransformer,
)


def test_get_object_type():
    assert InstanceCsvTransformer.get_object_type() == FOLIONamespaces.instances


def test_id_map_entry_without_hrid():
    assert InstanceCsvTransformer.get_id_map_entry("l1", {"id": "u1"}) == ("l1", "u1")


def test_id_map_entry_with_hrid():
    entry = InstanceCsvTransformer.get_id_map_entry("l1", {"id": "u1", "hrid": "in1"})
    assert entry == ("l1", "u1", "in1")


def test_handle_notes_removes_empty_notes():
    rec = {
        "notes": [
            {"instanceNoteTypeId": "t1", "note": "keep"},
            {"instanceNoteTypeId": "t1", "note": ""},
        ]
    }
    InstanceCsvTransformer.handle_notes(rec)
    assert rec["notes"] == [{"instanceNoteTypeId": "t1", "note": "keep"}]


def test_handle_notes_deletes_when_all_empty():
    rec = {"notes": [{"instanceNoteTypeId": "t1", "note": ""}]}
    InstanceCsvTransformer.handle_notes(rec)
    assert "notes" not in rec


def test_handle_notes_missing_type_fails():
    with pytest.raises(TransformationProcessError):
        InstanceCsvTransformer.handle_notes({"notes": [{"note": "text"}]})


from unittest.mock import MagicMock  # noqa: E402

from folio_migration_tools.custom_exceptions import (  # noqa: E402
    TransformationRecordFailedError,
)
from folio_migration_tools.library_configuration import FileDefinition  # noqa: E402


def _bare_task(tmp_path):
    task = InstanceCsvTransformer.__new__(InstanceCsvTransformer)
    task.mapper = MagicMock()
    task.total_records = 0
    task.instance_id_map = {}
    task.print_progress = MagicMock()
    task.folder_structure = MagicMock()
    task.folder_structure.legacy_records_folder = tmp_path
    return task


def test_handle_record_exception_dispatch(tmp_path):
    task = _bare_task(tmp_path)
    task._handle_record_exception(1, TransformationProcessError("i", "m", "v"))
    task.mapper.handle_transformation_process_error.assert_called_once()
    task._handle_record_exception(2, TransformationRecordFailedError("i", "m", "v"))
    task.mapper.handle_transformation_record_failed_error.assert_called_once()
    task._handle_record_exception(3, ValueError("x"))
    task.mapper.handle_generic_exception.assert_called_once()


def test_handle_record_exception_attribute_error_exits(tmp_path):
    task = _bare_task(tmp_path)
    with pytest.raises(SystemExit):
        task._handle_record_exception(1, AttributeError("boom"))


def test_fail_file_exits_and_reports(tmp_path):
    task = _bare_task(tmp_path)
    with pytest.raises(SystemExit):
        task._fail_file(FileDefinition(file_name="a.tsv"), ValueError("bad"))
    task.mapper.migration_report.add.assert_called_once()


def test_process_single_file_writes_records(tmp_path):
    task = _bare_task(tmp_path)
    (tmp_path / "a.tsv").write_text("id\n1\n2\n", encoding="utf-8")
    task.mapper.get_objects.return_value = [{"id": "1"}, {"id": "2"}]
    task.mapper.do_map.side_effect = [
        ({"id": "u1"}, "1"),
        ({"id": "u2", "hrid": "h2"}, "2"),
    ]
    results = MagicMock()
    task.process_single_file(FileDefinition(file_name="a.tsv"), results)
    assert task.total_records == 2
    assert task.instance_id_map == {"1": ("1", "u1"), "2": ("2", "u2", "h2")}


def test_process_single_file_handles_record_error(tmp_path):
    task = _bare_task(tmp_path)
    (tmp_path / "a.tsv").write_text("id\n1\n", encoding="utf-8")
    task.mapper.get_objects.return_value = [{"id": "1"}]
    task.mapper.do_map.side_effect = ValueError("bad")
    task.process_single_file(FileDefinition(file_name="a.tsv"), MagicMock())
    task.mapper.handle_generic_exception.assert_called_once()
    assert task.total_records == 1
