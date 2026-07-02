import sys
from io import BytesIO
from pymarc.field import Indicators
from types import SimpleNamespace
from unittest.mock import Mock

from pymarc import Field, MARCReader, Record, Subfield

from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.marc_rules_transformation.marc_reader_wrapper import (
    DEFAULT_MARC_RECORD_PREPROCESSORS,
    MARCReaderWrapper,
)
from folio_migration_tools.migration_report import MigrationReport


class FakeReader:
    def __init__(self, current_chunk, current_exception):
        self.current_chunk = current_chunk
        self.current_exception = current_exception
        self._records = iter([None])

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._records)


class WarningReader:
    def __init__(self, records, warning_lines=None):
        self.records = iter(records)
        self.warning_lines = warning_lines or []

    def __iter__(self):
        return self

    def __next__(self):
        record = next(self.records)
        for line in self.warning_lines:
            print(line, file=sys.stderr)
        self.warning_lines = []
        return record


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


def build_latin1_recoverable_chunk() -> bytes:
    """Build a chunk that fails UTF-8 decode but is recoverable as Latin-1."""
    record = Record()
    record.add_field(Field(tag="001", data="anon-l1"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value="Sample title")],
        )
    )
    chunk = bytearray(record.as_marc())
    # Mark as UTF-8 and inject a non-UTF-8 byte in field data.
    chunk[9] = ord("a")
    title_offset = bytes(chunk).find(b"Sample")
    if title_offset == -1:
        raise AssertionError("Could not locate title bytes in synthetic Latin-1 chunk")
    chunk[title_offset + 1] = 0xE1
    return bytes(chunk)


def build_dagger_repair_chunk() -> bytes:
    """Build a chunk with a MARCMaker dagger marker replacing subfield delimiter."""
    record = Record()
    record.add_field(Field(tag="001", data="anon-dag"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value="Anonymized title")],
        )
    )
    chunk = record.as_marc()

    leader = chunk[:24]
    base_addr = int(leader[12:17].decode("ascii"))
    directory = chunk[24 : base_addr - 1]
    data = chunk[base_addr:]

    entries = []
    for i in range(0, len(directory), 12):
        entry = directory[i : i + 12]
        tag = entry[:3].decode("ascii")
        length = int(entry[3:7].decode("ascii"))
        offset = int(entry[7:12].decode("ascii"))
        entries.append((tag, length, offset))

    rebuilt_fields = []
    rebuilt_entries = []
    running_offset = 0
    replaced = False

    for tag, length, offset in entries:
        field = data[offset : offset + length]
        body = field[:-1]

        if tag == "245":
            needle = b"\x1fa"
            needle_offset = body.find(needle)
            if needle_offset == -1:
                raise AssertionError(
                    "Could not locate subfield delimiter bytes in synthetic dagger chunk"
                )
            # Replace delimiter + code with dagger + same code, preserving title bytes.
            body = body[:needle_offset] + b"\xe2\x80\xa1a" + body[needle_offset + 2 :]
            replaced = True

        new_field = body + b"\x1e"
        rebuilt_fields.append(new_field)
        rebuilt_entries.append((tag, len(new_field), running_offset))
        running_offset += len(new_field)

    if not replaced:
        raise AssertionError("Could not locate 245 field in synthetic dagger chunk")

    new_data = b"".join(rebuilt_fields) + b"\x1d"
    new_base_addr = 24 + 12 * len(rebuilt_entries) + 1
    new_record_len = new_base_addr + len(new_data)

    new_leader = bytearray(leader)
    new_leader[0:5] = f"{new_record_len:05d}".encode("ascii")
    new_leader[12:17] = f"{new_base_addr:05d}".encode("ascii")

    new_directory = b"".join(
        tag.encode("ascii") + f"{length:04d}{offset:05d}".encode("ascii")
        for tag, length, offset in rebuilt_entries
    ) + b"\x1e"

    return bytes(new_leader) + new_directory + new_data


def build_unrecoverable_structural_chunk() -> bytes:
    """Build a chunk that is structurally invalid and cannot be repaired."""
    record = Record()
    record.add_field(Field(tag="001", data="anon-unrecoverable"))
    # Remove the record terminator so leader length no longer matches data length.
    return record.as_marc()[:-1]


def build_bad_directory_chunk() -> bytes:
    """Build a chunk with an invalid directory length field."""
    record = Record()
    record.add_field(Field(tag="001", data="anon-dir-bad"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value="Directory corruption")],
        )
    )
    chunk = bytearray(record.as_marc())
    # First directory entry starts at byte 24. Its length occupies bytes 27:31.
    # Make the length non-numeric so directory parsing fails immediately.
    chunk[27:31] = b"ABCD"
    return bytes(chunk)


def build_bad_directory_numeric_length_chunk() -> bytes:
    """Build a chunk with a numeric but unrealistic directory length value."""
    record = Record()
    record.add_field(Field(tag="001", data="anon-dir-bad-num"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value="Directory length corruption")],
        )
    )
    chunk = bytearray(record.as_marc())
    # First directory entry starts at byte 24. Its length occupies bytes 27:31.
    # Use a numeric but unrealistic length to emulate a likely directory corruption.
    chunk[27:31] = b"9999"
    return bytes(chunk)


def build_mislabelled_utf8_chunk(title: str) -> bytes:
    """Build a UTF-8 record whose leader[9] is intentionally mislabelled as blank."""
    record = Record()
    record.add_field(Field(tag="001", data="anon-mislabelled"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value=title)],
        )
    )
    chunk = bytearray(record.as_marc())
    chunk[9] = ord(" ")
    return bytes(chunk)


def test_read_records_recovers_parse_failure_and_logs_data_issue(monkeypatch):
    reader = FakeReader(
        current_chunk=b"bad marc chunk",
        current_exception=UnicodeDecodeError(
            "utf-8",
            b"bad marc chunk",
            0,
            1,
            "invalid start byte",
        ),
    )
    file_def = FileDefinition(file_name="crashes.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processed_records = []
    processor.process_record = lambda idx, record, source_file: processed_records.append(
        (idx, record, source_file)
    )

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )
    monkeypatch.setattr(
        MARCReaderWrapper,
        "recover_failed_record",
        lambda reader: (get_test_record(), "marc8_leader_heuristic"),
    )

    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(logged_issues) == 1
    assert "MARC-8 leader heuristic" in logged_issues[0][1]
    assert "exception=" in logged_issues[0][2]
    assert len(processed_records) == 1
    assert processed_records[0][0] == 0
    assert processed_records[0][1]["001"].value() == get_test_record()["001"].value()


def test_read_records_logs_marc8_warning_without_attempting_repair(monkeypatch):
    reader = WarningReader(
        [get_test_record()],
        ["Multi-byte position 93 exceeds length of marc8 string 92"],
    )
    file_def = FileDefinition(file_name="warnings.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processed_records = []
    processor.process_record = lambda idx, record, source_file: processed_records.append(
        (idx, record, source_file)
    )

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )

    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(processed_records) == 1
    assert processed_records[0][0] == 0
    assert logged_issues == [
        (
            "warnings.mrc:0",
            "MARC-8 decoding warning",
            "Multi-byte position 93 exceeds length of marc8 string 92",
        )
    ]


def test_recover_failed_record_requires_marc8_signals():
    # leader[9] is 'a', but no MARC-8 single-shift bytes (0x8E/0x8F),
    # so heuristic recovery should be skipped.
    chunk = b"00023nam a2200000 i 4500abc\x1d"
    reader = FakeReader(
        current_chunk=chunk,
        current_exception=UnicodeDecodeError(
            "utf-8",
            b"\x80",
            0,
            1,
            "invalid start byte",
        ),
    )

    recovered_record, recovery_strategy = MARCReaderWrapper.recover_failed_record(reader)
    assert recovered_record is None
    assert recovery_strategy == "none"


def test_recover_failed_record_attempts_latin1_when_marc8_signals_absent():
    chunk = build_latin1_recoverable_chunk()
    reader = FakeReader(
        current_chunk=chunk,
        current_exception=UnicodeDecodeError(
            "utf-8",
            b"\xe9\xa0",
            0,
            1,
            "invalid continuation byte",
        ),
    )

    _, recovery_strategy = MARCReaderWrapper.recover_failed_record(reader)
    assert recovery_strategy == "latin1_leader_heuristic"


def test_recover_failed_record_repairs_marcmaker_dagger_subfield_marker():
    chunk = build_dagger_repair_chunk()
    current_exception = UnicodeDecodeError(
        "ascii",
        b"\xe2",
        0,
        1,
        "ordinal not in range(128)",
    )
    reader = FakeReader(
        current_chunk=chunk,
        current_exception=current_exception,
    )

    recovered_record, recovery_strategy = MARCReaderWrapper.recover_failed_record(reader)

    assert recovered_record is not None
    assert recovery_strategy == "dagger_subfield_heuristic"
    assert recovered_record["001"].value() == "anon-dag"


def test_read_records_logs_failure_when_repair_is_unavailable(monkeypatch, caplog):
    reader = FakeReader(
        current_chunk=b"bad marc chunk",
        current_exception=UnicodeDecodeError(
            "utf-8",
            b"bad marc chunk",
            0,
            1,
            "invalid start byte",
        ),
    )
    file_def = FileDefinition(file_name="bad.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processor.process_record = Mock()

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )
    monkeypatch.setattr(MARCReaderWrapper, "recover_failed_record", lambda reader: (None, "none"))

    caplog.set_level(26)
    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(logged_issues) == 1
    assert "could not be repaired with configured heuristics" in logged_issues[0][1]
    assert "bad.mrc:0" in logged_issues[0][0]
    assert "RECORD FAILED" in caplog.text
    processor.process_record.assert_not_called()


def test_read_records_logs_failure_for_unrecoverable_structural_record(monkeypatch, caplog):
    chunk = build_unrecoverable_structural_chunk()
    reader = MARCReader(BytesIO(chunk), to_unicode=True, permissive=True)
    reader.hide_utf8_warnings = True
    reader.force_utf8 = False

    file_def = FileDefinition(file_name="unrecoverable.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processor.process_record = Mock()

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )

    caplog.set_level(26)
    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(logged_issues) == 1
    assert "could not be repaired with configured heuristics" in logged_issues[0][1]
    assert "unrecoverable.mrc:0" in logged_issues[0][0]
    assert "Record length in leader is greater than the length of data" in logged_issues[0][2]
    assert "RECORD FAILED" in caplog.text
    processor.process_record.assert_not_called()


def test_read_records_logs_failure_for_bad_directory_record(monkeypatch, caplog):
    chunk = build_bad_directory_chunk()
    reader = MARCReader(BytesIO(chunk), to_unicode=True, permissive=True)
    reader.hide_utf8_warnings = True
    reader.force_utf8 = False

    file_def = FileDefinition(file_name="bad_directory.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processor.process_record = Mock()

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )

    caplog.set_level(26)
    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(logged_issues) == 1
    assert "could not be repaired with configured heuristics" in logged_issues[0][1]
    assert "bad_directory.mrc:0" in logged_issues[0][0]
    assert "invalid literal for int() with base 10: 'ABCD'" in logged_issues[0][2]
    assert "RECORD FAILED" in caplog.text
    processor.process_record.assert_not_called()


def test_read_records_tolerates_numeric_bad_directory_length(monkeypatch, caplog):
    chunk = build_bad_directory_numeric_length_chunk()
    reader = MARCReader(BytesIO(chunk), to_unicode=True, permissive=True)
    reader.hide_utf8_warnings = True
    reader.force_utf8 = False

    file_def = FileDefinition(file_name="bad_directory_numeric_length.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processor.process_record = Mock()

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )

    caplog.set_level(26)
    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    # Permissive pymarc often tolerates numeric directory-length corruption,
    # so this should not be treated as an unrecoverable parsing failure.
    assert all(
        "could not be repaired with configured heuristics" not in message
        for _, message, _ in logged_issues
    )
    assert "RECORD FAILED" not in caplog.text
    processor.process_record.assert_called_once()


def test_read_records_logs_text_fidelity_warning_for_latin1_recovery(monkeypatch):
    record = Record()
    record.add_field(Field(tag="001", data="anon-fidelity"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value="Bad\ufffd text")],
        )
    )
    reader = FakeReader(
        current_chunk=b"bad marc chunk",
        current_exception=UnicodeDecodeError(
            "utf-8",
            b"\xe1",
            0,
            1,
            "invalid continuation byte",
        ),
    )
    file_def = FileDefinition(file_name="fidelity.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processor.process_record = Mock()

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )
    monkeypatch.setattr(
        MARCReaderWrapper,
        "recover_failed_record",
        lambda reader: (record, "latin1_leader_heuristic"),
    )

    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(logged_issues) == 2
    fidelity_issue = next(issue for issue in logged_issues if issue[1] == "MARC text fidelity warning")
    assert fidelity_issue[0] == "fidelity.mrc:0"
    assert "replacement_character_detected" in fidelity_issue[2]
    assert processor.mapper.migration_report.report["GeneralStatistics"][
        "Records with text fidelity warnings"
    ] == 1


def test_read_records_applies_probable_utf8_mislabeling_safeguard(monkeypatch):
    chunk = build_mislabelled_utf8_chunk("François")
    reader = MARCReader(BytesIO(chunk), to_unicode=True, permissive=True, utf8_handling="strict")
    reader.hide_utf8_warnings = True
    reader.force_utf8 = False

    file_def = FileDefinition(file_name="mislabelled_utf8.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processed_records = []
    processor.process_record = lambda idx, record, source_file: processed_records.append(
        (idx, record, source_file)
    )

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )

    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(processed_records) == 1
    assert processed_records[0][1]["245"].value() == "François"
    assert any(
        issue[1] == "Probable UTF-8 mislabeling detected; record re-decoded using UTF-8 safeguard"
        for issue in logged_issues
    )
    assert processor.mapper.migration_report.report["GeneralStatistics"][
        "Records with probable UTF-8 mislabeling override applied"
    ] == 1


def test_read_records_skips_probable_utf8_safeguard_without_signals(monkeypatch):
    chunk = build_mislabelled_utf8_chunk("simple ascii title")
    reader = MARCReader(BytesIO(chunk), to_unicode=True, permissive=True, utf8_handling="strict")
    reader.hide_utf8_warnings = True
    reader.force_utf8 = False

    file_def = FileDefinition(file_name="mislabelled_ascii.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processed_records = []
    processor.process_record = lambda idx, record, source_file: processed_records.append(
        (idx, record, source_file)
    )

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )

    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(processed_records) == 1
    assert processed_records[0][1]["245"].value() == "simple ascii title"
    assert not any(
        issue[1] == "Probable UTF-8 mislabeling detected; record re-decoded using UTF-8 safeguard"
        for issue in logged_issues
    )
    assert "Records with probable UTF-8 mislabeling override applied" not in (
        processor.mapper.migration_report.report.get("GeneralStatistics", {})
    )


def test_read_records_logs_text_fidelity_warning_for_marc8_recovery(monkeypatch):
    record = Record()
    record.add_field(Field(tag="001", data="anon-fidelity-2"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value="This has Ã and â€™ markers")],
        )
    )
    reader = FakeReader(
        current_chunk=b"bad marc chunk",
        current_exception=UnicodeDecodeError(
            "utf-8",
            b"\x8f",
            0,
            1,
            "invalid start byte",
        ),
    )
    file_def = FileDefinition(file_name="fidelity_mojibake.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processor.process_record = Mock()

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )
    monkeypatch.setattr(
        MARCReaderWrapper,
        "recover_failed_record",
        lambda reader: (record, "marc8_leader_heuristic"),
    )

    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert len(logged_issues) == 2
    fidelity_issue = next(issue for issue in logged_issues if issue[1] == "MARC text fidelity warning")
    assert fidelity_issue[0] == "fidelity_mojibake.mrc:0"
    assert "possible_mojibake_detected" in fidelity_issue[2]
    assert processor.mapper.migration_report.report["GeneralStatistics"][
        "Records with text fidelity warnings"
    ] == 1


def test_read_records_skips_text_fidelity_warning_without_recovery_strategy(monkeypatch):
    record = Record()
    record.add_field(Field(tag="001", data="anon-fidelity-3"))
    record.add_field(
        Field(
            tag="245",
            indicators=Indicators(*["1", "0"]),
            subfields=[Subfield(code="a", value="Bad\ufffd text")],
        )
    )
    reader = WarningReader([record])
    file_def = FileDefinition(file_name="fidelity_skipped.mrc")
    processor = build_processor(DEFAULT_MARC_RECORD_PREPROCESSORS)
    processor.process_record = Mock()

    logged_issues = []
    monkeypatch.setattr(
        Helper,
        "log_data_issue",
        lambda index_or_id, message, legacy_value: logged_issues.append(
            (index_or_id, message, legacy_value)
        ),
    )

    MARCReaderWrapper.read_records(reader, file_def, BytesIO(), processor)

    assert logged_issues == []
    assert "Records with text fidelity warnings" not in processor.mapper.migration_report.report.get(
        "GeneralStatistics",
        {},
    )
