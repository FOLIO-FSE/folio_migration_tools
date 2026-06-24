"""MARC record reader wrapper.

Provides a wrapper around pymarc's MARCReader with enhanced error handling,
encoding detection, and record validation. Supports multiple MARC file formats
and handles corrupted records gracefully.
"""

import json
import logging
import sys
from io import IOBase
from pathlib import Path

import i18n
from folio_data_import.marc_preprocessors import MARCPreprocessor
from pymarc import Leader, MARCReader, Record

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.migration_report import MigrationReport

logger = logging.getLogger(__name__)

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
                    reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                    reader.hide_utf8_warnings = True
                    reader.force_utf8 = False
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
        marc_record_preprocessor = MARCReaderWrapper.get_marc_record_preprocessor(processor)
        for idx, record in enumerate(reader):
            processor.mapper.migration_report.add_general_statistics(
                i18n.t("Records in file before parsing")
            )
            try:
                # None = Something bad happened
                if record is None:
                    report_failed_parsing(
                        reader,
                        source_file,
                        failed_records_file,
                        idx,
                        processor.mapper.migration_report,
                    )
                # The normal case
                else:
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


def report_failed_parsing(
    reader, source_file, failed_bibs_file, idx, migration_report: MigrationReport
):
    migration_report.add_general_statistics(
        i18n.t("Records with encoding errors - parsing failed"),
    )
    failed_bibs_file.write(reader.current_chunk)
    raise TransformationRecordFailedError(
        f"Index in {source_file.file_name}:{idx}",
        f"MARC parsing error: {reader.current_exception}",
        "Failed records stored in results/failed_bib_records.mrc",
    )
