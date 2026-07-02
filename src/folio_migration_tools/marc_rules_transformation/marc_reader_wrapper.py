"""MARC record reader wrapper.

Provides a wrapper around pymarc's MARCReader with enhanced error handling,
encoding detection, and record validation. Supports multiple MARC file formats
and handles corrupted records gracefully.
"""

import json
import logging
import sys
from contextlib import redirect_stderr
from io import IOBase, StringIO
from itertools import count
from pathlib import Path
from typing import List

import i18n
from folio_data_import.marc_preprocessors import MARCPreprocessor
from pymarc import Leader, MARCReader, Record

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.migration_report import MigrationReport

logger = logging.getLogger(__name__)

MARC8_SIGNAL_BYTES = (b"\x8e", b"\x8f")
MARC8_WARNING_PREFIX = "Multi-byte position "
TEXT_FIDELITY_CHECK_STRATEGIES = {
    "marc8_leader_heuristic",
    "latin1_leader_heuristic",
}
REPLACEMENT_CHAR = "\ufffd"
MOJIBAKE_PATTERNS = (
    "Ã",
    "Â",
    "â€™",
    "â€œ",
    "â€",
    "â€“",
    "â€”",
    "ı̉",
)
WEAK_MOJIBAKE_PATTERNS = {
    "©",
    "♭",
    "ʹ",
}

DEFAULT_MARC_RECORD_PREPROCESSORS = [
    "folio_migration_tools.marc_rules_transformation.marc_reader_wrapper.set_leader"
]


def set_leader(
    marc_record: Record,
    migration_report: MigrationReport | None = None,
    **kwargs,
) -> Record:
    migration_report = migration_report or kwargs.get("migration_report")

    if marc_record.leader[9] != "a":
        if migration_report is not None:
            migration_report.add(
                "LeaderManipulation",
                i18n.t(
                    "Set leader 09 (Character coding scheme) from %{field} to a",
                    field=marc_record.leader[9],
                ),
            )
        marc_record.leader = Leader(f"{marc_record.leader[:9]}a{marc_record.leader[10:]}")

    if not str(marc_record.leader).endswith("4500"):
        if migration_report is not None:
            migration_report.add(
                "LeaderManipulation",
                i18n.t(
                    "Set leader 20-23 from %{field} to 4500",
                    field=marc_record.leader[-4:],
                ),
            )
        marc_record.leader = Leader(f"{marc_record.leader[:-4]}4500")

    if marc_record.leader[10] != "2":
        if migration_report is not None:
            migration_report.add(
                "LeaderManipulation",
                i18n.t(
                    "Set leader 10 (Indicator count) from %{field} to 2",
                    field=marc_record.leader[10],
                ),
            )
        marc_record.leader = Leader(f"{marc_record.leader[:10]}2{marc_record.leader[11:]}")

    if marc_record.leader[11] != "2":
        if migration_report is not None:
            migration_report.add(
                "LeaderManipulation",
                i18n.t(
                    "Set leader 11 (Subfield code count) from %{record} to 2",
                    record=marc_record.leader[11],
                ),
            )
        marc_record.leader = Leader(f"{marc_record.leader[:11]}2{marc_record.leader[12:]}")

    return marc_record


class MARCReaderWrapper:
    @staticmethod
    def get_marc_record_preprocessor(processor: MarcFileProcessor) -> MARCPreprocessor:
        task_config = processor.mapper.task_configuration
        preprocessors = (
            getattr(task_config, "marc_record_preprocessors", None)
            or DEFAULT_MARC_RECORD_PREPROCESSORS
        )
        if isinstance(preprocessors, list):
            preprocessors = ",".join(preprocessors)
        preprocessors_args = MARCReaderWrapper.load_preprocessors_args(
            task_config,
            processor.folder_structure,
        )
        default_args = dict(preprocessors_args.get("default", {}))
        default_args["migration_report"] = processor.mapper.migration_report
        preprocessors_args["default"] = default_args
        return MARCPreprocessor(preprocessors, **preprocessors_args)

    @staticmethod
    def load_preprocessors_args(task_config, folder_structure: FolderStructure) -> dict:
        preprocessors_args = getattr(task_config, "preprocessors_args", {}) or {}
        if isinstance(preprocessors_args, str):
            with open(folder_structure.mapping_files_folder / preprocessors_args, "r") as f:
                return json.load(f)
        return dict(preprocessors_args)

    @staticmethod
    def preprocess_record(
        marc_record: Record,
        marc_record_preprocessor: MARCPreprocessor,
    ) -> Record:
        try:
            return marc_record_preprocessor.do_work(marc_record)
        except TypeError as error:
            logger.warning(
                "Skipping MARC record preprocessing: preprocessors must accept **kwargs (%s)",
                error,
            )
            return marc_record

    @staticmethod
    def process_single_file(
        file_def: FileDefinition,
        processor,
        failed_records_path: Path,
        folder_structure: FolderStructure,
    ):
        try:
            with open(failed_records_path, "ab") as failed_marc_records_file:
                with open(
                    folder_structure.legacy_records_folder / file_def.file_name,
                    "rb",
                ) as marc_file:
                    reader = MARCReader(
                        marc_file, to_unicode=True, permissive=True, utf8_handling="strict"
                    )
                    reader.hide_utf8_warnings = False
                    reader.force_utf8 = True
                    logger.info("Running %s", file_def.file_name)
                    MARCReaderWrapper.read_records(
                        reader, file_def, failed_marc_records_file, processor
                    )
        except TransformationProcessError as tpe:
            logger.critical(tpe)
            sys.exit(1)
        except Exception:
            logger.exception("Failure in Main: %s", file_def.file_name, stack_info=True)

    @staticmethod
    def read_records(
        reader,
        source_file: FileDefinition,
        failed_records_file: IOBase,
        processor: MarcFileProcessor,
    ):
        """Read and process records while preserving per-record parser diagnostics.

        We intentionally call ``next(reader)`` inside ``redirect_stderr`` so we can
        capture pymarc MARC-8 decode warnings emitted to stderr and associate each
        warning with the specific record index.
        """
        marc_record_preprocessor = MARCReaderWrapper.get_marc_record_preprocessor(processor)
        for idx in count():
            stderr_buffer = StringIO()
            # Pull exactly one record per iteration so warning output stays scoped
            # to this record index.
            with redirect_stderr(stderr_buffer):
                try:
                    record = next(reader)
                except StopIteration:
                    break
            processor.mapper.migration_report.add_general_statistics(
                i18n.t("Records in file before parsing")
            )
            try:
                recovery_strategy = "none"
                # A permissive MARCReader yields None when parsing fails.
                if record is None:
                    recovered_record, recovery_strategy = MARCReaderWrapper.recover_failed_record(
                        reader
                    )
                    MARCReaderWrapper.log_parsing_issue(
                        reader,
                        source_file,
                        idx,
                        processor.mapper.migration_report,
                        recovered=bool(recovered_record),
                        recovery_strategy=recovery_strategy,
                    )
                    if recovered_record is None:
                        report_failed_parsing(
                            reader,
                            source_file,
                            failed_records_file,
                            idx,
                            processor.mapper.migration_report,
                        )
                    record = recovered_record
                # Normal successful decode path.
                if record is not None:
                    # Log MARC-8 truncation warnings, but do not alter decoding.
                    MARCReaderWrapper.log_marc8_decoding_warnings(
                        source_file,
                        idx,
                        stderr_buffer.getvalue(),
                    )
                    if recovery_strategy in TEXT_FIDELITY_CHECK_STRATEGIES:
                        MARCReaderWrapper.log_record_text_fidelity_warnings(
                            source_file,
                            idx,
                            record,
                            processor.mapper.migration_report,
                            recovery_strategy,
                        )
                    record = MARCReaderWrapper.preprocess_record(
                        record,
                        marc_record_preprocessor,
                    )
                    processor.mapper.migration_report.add_general_statistics(
                        i18n.t("Records successfully decoded from MARC21"),
                    )
                    processor.process_record(idx, record, source_file)
            except TransformationRecordFailedError as error:
                error.log_it()
                processor.mapper.migration_report.add_general_statistics(
                    i18n.t("Records that failed transformation. Check log for details"),
                )
            except ValueError as error:
                logger.exception(error)
        logger.info("Done reading %s records from file", idx + 1)

    @staticmethod
    def set_leader(marc_record: Record, migration_report: MigrationReport):
        return set_leader(marc_record, migration_report)

    @staticmethod
    def decode_candidate_chunk(
        candidate_chunk: bytes,
        strategy: str,
        **decode_kwargs,
    ) -> tuple[Record | None, str]:
        try:
            recovered_record = Record()
            recovered_record.decode_marc(
                candidate_chunk,
                to_unicode=True,
                force_utf8=False,
                hide_utf8_warnings=True,
                utf8_handling="strict",
                **decode_kwargs,
            )
            return recovered_record, strategy
        except Exception:
            logger.debug(
                "Unable to recover MARC record using strategy '%s'",
                strategy,
                exc_info=True,
            )
            return None, strategy

    @staticmethod
    def recover_failed_record(reader) -> tuple[Record | None, str]:
        current_exception = getattr(reader, "current_exception", None)
        current_chunk = getattr(reader, "current_chunk", b"") or b""

        strategies = [
            (
                "marc8_leader_heuristic",
                MARCReaderWrapper.should_attempt_marc8_redecode(
                    current_exception,
                    current_chunk,
                ),
                MARCReaderWrapper.get_marc8_candidate_chunk(current_chunk),
                {},
            ),
            (
                "dagger_subfield_heuristic",
                MARCReaderWrapper.should_attempt_dagger_subfield_fix(
                    current_exception,
                    current_chunk,
                ),
                MARCReaderWrapper.repair_dagger_subfield_markers(current_chunk),
                {},
            ),
            (
                "latin1_leader_heuristic",
                MARCReaderWrapper.should_attempt_latin1_redecode(
                    current_exception,
                    current_chunk,
                ),
                MARCReaderWrapper.get_latin1_candidate_chunk(current_chunk),
                {"encoding": "iso8859-1"},
            ),
        ]

        for strategy, should_attempt, candidate_chunk, decode_kwargs in strategies:
            if not should_attempt or candidate_chunk is None:
                continue
            recovered_record, _ = MARCReaderWrapper.decode_candidate_chunk(
                candidate_chunk,
                strategy,
                **decode_kwargs,
            )
            if recovered_record is not None:
                return recovered_record, strategy

        return None, "none"

    @staticmethod
    def should_attempt_marc8_redecode(current_exception, current_chunk: bytes) -> bool:
        if not isinstance(current_exception, UnicodeDecodeError):
            return False
        if len(current_chunk) < 10:
            return False
        return any(signal in current_chunk for signal in MARC8_SIGNAL_BYTES)

    @staticmethod
    def should_attempt_latin1_redecode(current_exception, current_chunk: bytes) -> bool:
        if not isinstance(current_exception, UnicodeDecodeError):
            return False
        if len(current_chunk) < 10:
            return False
        if any(signal in current_chunk for signal in MARC8_SIGNAL_BYTES):
            return False
        return True

    @staticmethod
    def should_attempt_dagger_subfield_fix(current_exception, current_chunk: bytes) -> bool:
        if not isinstance(current_exception, UnicodeDecodeError):
            return False
        if len(current_chunk) < 24:
            return False
        if b"\xe2\x80\xa1" not in current_chunk:
            return False
        return True

    @staticmethod
    def parse_directory_entries(
        chunk: bytes,
    ) -> tuple[bytes, int, List[tuple[str, int, int]]] | None:
        leader = chunk[:24]
        try:
            base_addr = int(leader[12:17].decode("ascii"))
        except Exception:
            return None

        if base_addr <= 25 or base_addr > len(chunk):
            return None

        directory = chunk[24 : base_addr - 1]
        entries = []
        for i in range(0, len(directory), 12):
            entry = directory[i : i + 12]
            if len(entry) < 12:
                continue
            try:
                tag = entry[:3].decode("ascii")
                length = int(entry[3:7].decode("ascii"))
                offset = int(entry[7:12].decode("ascii"))
            except Exception:
                return None
            entries.append((tag, length, offset))
        return leader, base_addr, entries

    @staticmethod
    def repair_dagger_subfield_markers(current_chunk: bytes) -> bytes | None:
        parsed = MARCReaderWrapper.parse_directory_entries(current_chunk)
        if parsed is None:
            return None

        leader, base_addr, entries = parsed
        data = current_chunk[base_addr:]
        if not data.endswith(b"\x1d"):
            return None

        new_entries = []
        new_fields = []
        running_offset = 0
        replacements = 0

        for tag, length, offset in entries:
            field = data[offset : offset + length]
            if len(field) != length or not field.endswith(b"\x1e"):
                return None

            body = field[:-1]
            if (
                tag >= "010"
                and len(body) >= 6
                and body[2:5] == b"\xe2\x80\xa1"
                and b"\x1f" not in body[2:]
            ):
                body = body[:2] + b"\x1f" + body[5:]
                replacements += 1

            new_field = body + b"\x1e"
            new_fields.append(new_field)
            new_entries.append((tag, len(new_field), running_offset))
            running_offset += len(new_field)

        if replacements == 0:
            return None

        new_data = b"".join(new_fields) + b"\x1d"
        new_base_addr = 24 + 12 * len(new_entries) + 1
        new_record_len = new_base_addr + len(new_data)

        new_leader = bytearray(leader)
        new_leader[0:5] = f"{new_record_len:05d}".encode("ascii")
        new_leader[12:17] = f"{new_base_addr:05d}".encode("ascii")

        new_directory = (
            b"".join(
                tag.encode("ascii") + f"{length:04d}{offset:05d}".encode("ascii")
                for tag, length, offset in new_entries
            )
            + b"\x1e"
        )

        repaired_chunk = bytes(new_leader) + new_directory + new_data
        if len(repaired_chunk) != new_record_len:
            return None
        return repaired_chunk

    @staticmethod
    def get_marc8_candidate_chunk(current_chunk: bytes) -> bytes | None:
        if len(current_chunk) < 10:
            return None
        if current_chunk[9:10] == b"a":
            return current_chunk[:9] + b" " + current_chunk[10:]
        return current_chunk

    @staticmethod
    def get_latin1_candidate_chunk(current_chunk: bytes) -> bytes | None:
        if len(current_chunk) < 10:
            return None
        if current_chunk[9:10] == b"a":
            return current_chunk[:9] + b" " + current_chunk[10:]
        return current_chunk

    @staticmethod
    def build_parsing_issue_context(reader) -> str:
        current_chunk = getattr(reader, "current_chunk", b"") or b""
        leader_preview = current_chunk[:24].decode("ascii", errors="replace")
        return (
            f"exception={reader.current_exception}; "
            f"leader={leader_preview!r}; "
            f"chunk_len={len(current_chunk)}; "
            f"chunk_hex={current_chunk[:32].hex()}"
        )

    @staticmethod
    def log_parsing_issue(
        reader,
        source_file: FileDefinition,
        idx: int,
        migration_report: MigrationReport,
        recovered: bool,
        recovery_strategy: str,
    ):
        legacy_id = f"{source_file.file_name}:{idx}"
        context = MARCReaderWrapper.build_parsing_issue_context(reader)
        if recovered:
            migration_report.add_general_statistics(
                i18n.t("Records with encoding errors - repaired"),
            )
            recovery_message = {
                "marc8_leader_heuristic": i18n.t(
                    "MARC parsing issue repaired with MARC-8 leader heuristic"
                ),
                "dagger_subfield_heuristic": i18n.t(
                    "MARC parsing issue repaired by converting MARCMaker dagger "
                    "to subfield delimiter"
                ),
                "latin1_leader_heuristic": i18n.t(
                    "MARC parsing issue repaired with Latin-1 leader heuristic"
                ),
            }.get(recovery_strategy, i18n.t("MARC parsing issue repaired after re-decode"))
            Helper.log_data_issue(
                legacy_id,
                recovery_message,
                context,
            )
        else:
            migration_report.add_general_statistics(
                i18n.t("Records with encoding errors - parsing failed"),
            )
            Helper.log_data_issue(
                legacy_id,
                i18n.t("MARC parsing issue could not be repaired with configured heuristics"),
                context,
            )

    @staticmethod
    def log_marc8_decoding_warnings(
        source_file: FileDefinition,
        idx: int,
        stderr_output: str,
    ):
        for line in stderr_output.splitlines():
            if not line.startswith(MARC8_WARNING_PREFIX):
                continue
            Helper.log_data_issue(
                f"{source_file.file_name}:{idx}",
                i18n.t("MARC-8 decoding warning"),
                line,
            )

    @staticmethod
    def iter_text_values(record: Record):
        for field in record.get_fields():
            if field.is_control_field():
                continue
            subfields = getattr(field, "subfields", [])
            if not subfields:
                continue
            first = subfields[0]
            if hasattr(first, "value"):
                for subfield in subfields:
                    value = getattr(subfield, "value", None)
                    if isinstance(value, str):
                        yield value
                continue
            for value in subfields[1::2]:
                if isinstance(value, str):
                    yield value

    @staticmethod
    def detect_text_fidelity_signals(record: Record) -> list[str]:
        replacement_found = False
        strong_mojibake_hits: set[str] = set()
        weak_mojibake_hits: set[str] = set()
        for value in MARCReaderWrapper.iter_text_values(record):
            if REPLACEMENT_CHAR in value:
                replacement_found = True
            for token in MOJIBAKE_PATTERNS:
                if token in value:
                    strong_mojibake_hits.add(token)
            for token in WEAK_MOJIBAKE_PATTERNS:
                if token in value:
                    weak_mojibake_hits.add(token)

        mojibake_hits = set(strong_mojibake_hits)
        # Weak symbols like copyright/music marks are only suspicious when they
        # co-occur with stronger mojibake indicators in the same record.
        if strong_mojibake_hits:
            mojibake_hits.update(weak_mojibake_hits)

        signals: list[str] = []
        if replacement_found:
            signals.append("replacement_character_detected")
        if mojibake_hits:
            signals.append(f"possible_mojibake_detected(tokens={','.join(sorted(mojibake_hits))})")
        return signals

    @staticmethod
    def log_record_text_fidelity_warnings(
        source_file: FileDefinition,
        idx: int,
        record: Record,
        migration_report: MigrationReport,
        recovery_strategy: str,
    ):
        signals = MARCReaderWrapper.detect_text_fidelity_signals(record)
        if not signals:
            return

        migration_report.add_general_statistics(
            i18n.t("Records with text fidelity warnings"),
        )
        context = f"recovery_strategy={recovery_strategy}; signals={'; '.join(signals)}"
        Helper.log_data_issue(
            f"{source_file.file_name}:{idx}",
            i18n.t("MARC text fidelity warning"),
            context,
        )


def report_failed_parsing(
    reader, source_file, failed_bibs_file, idx, migration_report: MigrationReport
):
    failed_bibs_file.write(reader.current_chunk)
    raise TransformationRecordFailedError(
        f"Index in {source_file.file_name}:{idx}",
        f"MARC parsing error: {reader.current_exception}",
        (
            "Failed records stored in results/failed_bib_records.mrc; "
            f"{MARCReaderWrapper.build_parsing_issue_context(reader)}"
        ),
    )
