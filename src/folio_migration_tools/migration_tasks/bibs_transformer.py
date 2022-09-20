import logging
import sys
from io import IOBase
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import BaseModel
from pymarc import MARCReader
from pymarc.record import Record

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs


class BibsTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        deactivate035_from001: Optional[bool] = False
        migration_task_type: str
        use_tenant_mapping_rules: Optional[bool] = True
        hrid_handling: Optional[HridHandling] = HridHandling.default
        files: List[FileDefinition]
        ils_flavour: IlsFlavour
        tags_to_delete: Optional[List[str]] = []
        reset_hrid_settings: Optional[bool] = False
        never_update_hrid_settings: Optional[bool] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.instances

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):

        super().__init__(library_config, task_config, use_logging)
        self.processor: BibsProcessor = None
        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_configuration.files
        )
        logging.info(self.task_configuration.json(indent=4))
        self.mapper = BibsRulesMapper(self.folio_client, library_config, self.task_configuration)
        self.bib_ids = set()
        logging.info("Init done")
        if (
            self.task_configuration.reset_hrid_settings
            and not self.task_configuration.never_update_hrid_settings
        ):
            self.mapper.reset_instance_hrid_counter()

    def do_work(self):
        logging.info("Starting....")

        with open(self.folder_structure.created_objects_path, "w+") as created_records_file:
            self.processor = BibsProcessor(
                self.mapper,
                self.folio_client,
                created_records_file,
                self.folder_structure,
            )
            with open(self.folder_structure.failed_bibs_file, "wb") as failed_bibs_file:
                for file_obj in self.task_configuration.files:
                    try:
                        with open(
                            self.folder_structure.legacy_records_folder / file_obj.file_name,
                            "rb",
                        ) as marc_file:
                            reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                            reader.hide_utf8_warnings = True
                            reader.force_utf8 = False
                            logging.info("running %s", file_obj.file_name)
                            self.read_records(reader, file_obj, failed_bibs_file)
                    except TransformationProcessError as tpe:
                        logging.critical(tpe)
                        sys.exit(1)
                    except Exception:
                        logging.exception(file_obj, stack_info=True)
                        logging.critical(
                            "File %s failed for unknown reason. Halting", file_obj.file_name
                        )
                        sys.exit(1)

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        self.processor.wrap_up()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                "Bibliographic records transformation report",
                report_file,
                self.start_datetime,
            )
            Helper.print_mapping_report(
                report_file,
                self.mapper.parsed_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )

        logging.info(
            "Done. Transformation report written to %s",
            self.folder_structure.migration_reports_file.name,
        )

    def read_records(self, reader, source_file: FileDefinition, failed_bibs_file: IOBase):
        for idx, record in enumerate(reader):
            self.mapper.migration_report.add_general_statistics("Records in file before parsing")
            try:
                # None = Something bad happened
                if record is None:
                    self.report_failed_parsing(reader, source_file, failed_bibs_file, idx)
                # The normal case
                else:
                    self.set_leader(record, self.mapper.migration_report)
                    self.mapper.migration_report.add_general_statistics(
                        "Records successfully decoded from MARC21",
                    )
                    self.processor.process_record(idx, record, source_file)
            except TransformationRecordFailedError as error:
                error.log_it()
        logging.info("Done reading %s records from file", idx + 1)

    def report_failed_parsing(self, reader, source_file, failed_bibs_file, idx):
        self.mapper.migration_report.add_general_statistics(
            "Records with encoding errors - parsing failed",
        )
        failed_bibs_file.write(reader.current_chunk)
        raise TransformationRecordFailedError(
            f"Index in {source_file.file_name}:{idx}",
            f"MARC parsing error: {reader.current_exception}",
            "Failed records stored in results/failed_bib_records.mrc",
        )

    @staticmethod
    def set_leader(marc_record: Record, migration_report: MigrationReport):
        if marc_record.leader[9] != "a":
            migration_report.add(
                Blurbs.LeaderManipulation,
                f"Set leader 09 (Character coding scheme) from {marc_record.leader[9]} to a",
            )
            marc_record.leader = f"{marc_record.leader[:9]}a{marc_record.leader[10:]}"

        if not marc_record.leader.endswith("4500"):
            migration_report.add(
                Blurbs.LeaderManipulation,
                f"Set leader 20-23 from {marc_record.leader[-4:]} to 4500",
            )
            marc_record.leader = f"{marc_record.leader[:-4]}4500"

        if marc_record.leader[10] != "2":
            migration_report.add(
                Blurbs.LeaderManipulation,
                f"Set leader 10 (Indicator count) from {marc_record.leader[10]} to 2",
            )
            marc_record.leader = f"{marc_record.leader[:10]}2{marc_record.leader[11:]}"

        if marc_record.leader[11] != "2":
            migration_report.add(
                Blurbs.LeaderManipulation,
                f"Set leader 10 (Subfield code count) from {marc_record.leader[11]} to 2",
            )
            marc_record.leader = f"{marc_record.leader[:11]}2{marc_record.leader[12:]}"
