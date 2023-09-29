import logging
import sys
import i18n
from io import IOBase
from pathlib import Path

from pymarc import MARCReader
from pymarc import Record

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.migration_report import MigrationReport


class MARCReaderWrapper:
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
                    logging.info("Running %s", file_def.file_name)
                    MARCReaderWrapper.read_records(
                        reader, file_def, failed_marc_records_file, processor
                    )
        except TransformationProcessError as tpe:
            logging.critical(tpe)
            sys.exit(1)
        except Exception:
            logging.exception("Failure in Main: %s", file_def.file_name, stack_info=True)

    @staticmethod
    def read_records(
        reader,
        source_file: FileDefinition,
        failed_records_file: IOBase,
        processor,
    ):
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
                    MARCReaderWrapper.set_leader(record, processor.mapper.migration_report)
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
                logging.error(error)
        logging.info("Done reading %s records from file", idx + 1)

    @staticmethod
    def set_leader(marc_record: Record, migration_report: MigrationReport):
        if marc_record.leader[9] != "a":
            migration_report.add(
                "LeaderManipulation",
                i18n.t(
                    "Set leader 09 (Character coding scheme) from %{field} to a",
                    field=marc_record.leader[9],
                ),
            )
            marc_record.leader = f"{marc_record.leader[:9]}a{marc_record.leader[10:]}"

        if not marc_record.leader.endswith("4500"):
            migration_report.add(
                "LeaderManipulation",
                i18n.t("Set leader 20-23 from %{field} to 4500", field=marc_record.leader[-4:]),
            )
            marc_record.leader = f"{marc_record.leader[:-4]}4500"

        if marc_record.leader[10] != "2":
            migration_report.add(
                "LeaderManipulation",
                i18n.t(
                    "Set leader 10 (Indicator count) from %{field} to 2",
                    field=marc_record.leader[10],
                ),
            )
            marc_record.leader = f"{marc_record.leader[:10]}2{marc_record.leader[11:]}"

        if marc_record.leader[11] != "2":
            migration_report.add(
                "LeaderManipulation",
                i18n.t(
                    "Set leader 11 (Subfield code count) from %{record} to 2",
                    record=marc_record.leader[11],
                ),
            )
            marc_record.leader = f"{marc_record.leader[:11]}2{marc_record.leader[12:]}"


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
