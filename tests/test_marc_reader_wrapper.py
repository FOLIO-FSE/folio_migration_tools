from pymarc.field import Indicators
from types import SimpleNamespace

from pymarc import Field, MARCReader, Record, Subfield

from folio_migration_tools.marc_rules_transformation.marc_reader_wrapper import (
    DEFAULT_MARC_RECORD_PREPROCESSORS,
    MARCReaderWrapper,
)
from folio_migration_tools.migration_report import MigrationReport


def add_local_note(
    record: Record,
    note_text: str = "Imported by preprocessor",
    migration_report: MigrationReport | None = None,
    **kwargs,
) -> Record:
    record.add_field(
        Field(
            tag="590",
            indicators=Indicators(*[" ", " "]),
            subfields=[Subfield(code="a", value=note_text)],
        )
    )
    if migration_report is not None:
        migration_report.add_general_statistics("Custom preprocessor invoked")
    return record


def replace_control_number(record: Record, value: str = "updated", **kwargs) -> Record:
    record["001"].data = value
    return record


def replace_control_number_no_kwargs(record: Record, value: str = "updated") -> Record:
    record["001"].data = value
    return record


def require_migration_report(
    record: Record,
    migration_report: MigrationReport,
    **kwargs,
) -> Record:
    migration_report.add("LeaderManipulation", "Migration report injected")
    return record


def get_test_record() -> Record:
    path = "./tests/test_data/corrupt_leader.mrc"
    with open(path, "rb") as marc_file:
        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
        reader.hide_utf8_warnings = True
        reader.force_utf8 = True
        return next(reader)


def build_processor(preprocessors, preprocessors_args=None, mapping_files_folder="."):
    return SimpleNamespace(
        folder_structure=SimpleNamespace(mapping_files_folder=mapping_files_folder),
        mapper=SimpleNamespace(
            migration_report=MigrationReport(),
            task_configuration=SimpleNamespace(
                marc_record_preprocessors=preprocessors,
                preprocessors_args=preprocessors_args or {},
            ),
        ),
    )


def test_set_leader():
    migration_report = MigrationReport()
    record = get_test_record()
    MARCReaderWrapper.set_leader(record, migration_report)
    assert str(record.leader).endswith("4500")
    assert record.leader[9] == "a"
    assert record.leader[10] == "2"
    assert record.leader[11] == "2"
    vals = migration_report.report["LeaderManipulation"].items()
    assert len(vals) == 5


def test_get_marc_record_preprocessor_uses_default_list():
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    marc_record_preprocessor = MARCReaderWrapper.get_marc_record_preprocessor(processor)

    assert len(marc_record_preprocessor.preprocessors) == 1
    assert marc_record_preprocessor.preprocessors[0][0].__name__ == "set_leader"


def test_preprocess_record_runs_custom_preprocessors_and_injects_migration_report():
    processor = build_processor(
        [
            "tests.test_marc_reader_wrapper.add_local_note",
            "tests.test_marc_reader_wrapper.replace_control_number",
            "tests.test_marc_reader_wrapper.require_migration_report",
        ],
        {
            "add_local_note": {"note_text": "Configured note"},
            "replace_control_number": {"value": "new-control-number"},
        },
    )
    marc_record_preprocessor = MARCReaderWrapper.get_marc_record_preprocessor(processor)

    record = MARCReaderWrapper.preprocess_record(
        get_test_record(),
        marc_record_preprocessor,
    )

    assert record["001"].data == "new-control-number"
    assert record.get_fields("590")[0].get_subfields("a") == ["Configured note"]
    assert (
        processor.mapper.migration_report.report["GeneralStatistics"]["Custom preprocessor invoked"]
        == 1
    )
    assert (
        processor.mapper.migration_report.report["LeaderManipulation"]["Migration report injected"]
        == 1
    )


def test_load_preprocessors_args_from_file(tmp_path):
    args_path = tmp_path / "preprocessors.json"
    args_path.write_text('{"replace_control_number": {"value": "from-file"}}')
    processor = build_processor(
        ["tests.test_marc_reader_wrapper.replace_control_number"],
        "preprocessors.json",
        tmp_path,
    )
    marc_record_preprocessor = MARCReaderWrapper.get_marc_record_preprocessor(processor)

    record = MARCReaderWrapper.preprocess_record(
        get_test_record(),
        marc_record_preprocessor,
    )

    assert record["001"].data == "from-file"


def test_preprocessor_without_kwargs_is_skipped(caplog):
    processor = build_processor(
        ["tests.test_marc_reader_wrapper.replace_control_number_no_kwargs"],
        {"replace_control_number_no_kwargs": {"value": "from-file"}},
    )
    marc_record_preprocessor = MARCReaderWrapper.get_marc_record_preprocessor(processor)

    original_record = get_test_record()
    original_control_number = original_record["001"].data
    record = MARCReaderWrapper.preprocess_record(
        original_record,
        marc_record_preprocessor,
    )

    assert record["001"].data == original_control_number
    assert "must accept **kwargs" in caplog.text
