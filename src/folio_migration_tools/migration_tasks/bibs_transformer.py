import logging
import sys
import time
from datetime import datetime as dt
from os.path import isfile
from typing import List, Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_migration_tools.colors import Bcolors
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from pydantic import BaseModel
from pymarc import MARCReader
from pymarc.record import Record


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
        self.task_config = task_config
        logging.info(task_config.json(indent=4))
        self.files = [
            f
            for f in self.task_config.files
            if isfile(self.folder_structure.legacy_records_folder / f.file_name)
        ]
        if not any(self.files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                "",
                f"Files {ret_str} not found in {self.folder_structure.data_folder / 'items'}",
            )
        logging.info(self.files)
        logging.info("# of files to process: %s", len(self.files))
        for file_path in self.files:
            logging.info("\t%s", file_path)
        self.mapper = BibsRulesMapper(self.folio_client, library_config, task_config)
        self.processor = None
        self.bib_ids = set()
        logging.info("Init done")

    def do_work(self):
        logging.info("Starting....")
        with open(
            self.folder_structure.created_objects_path, "w+"
        ) as created_records_file:
            self.processor = BibsProcessor(
                self.mapper,
                self.folio_client,
                created_records_file,
                self.folder_structure,
            )
            for file_obj in self.files:
                try:
                    with open(
                        self.folder_structure.legacy_records_folder
                        / file_obj.file_name,
                        "rb",
                    ) as marc_file:
                        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                        reader.hide_utf8_warnings = True
                        reader.force_utf8 = False
                        logging.info("running %s", file_obj.file_name)
                        self.read_records(reader, file_obj)
                except TransformationProcessError as tpe:
                    logging.critical(tpe)
                    sys.exit()
                except Exception:
                    logging.exception(file_obj, stack_info=True)
                    logging.critical(
                        "File %s failed for unknown reason. Halting", file_obj.file_name
                    )
                    sys.exit()

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        self.processor.wrap_up()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            report_file.write("# Bibliographic records transformation results   \n")
            report_file.write(f"Time Run: {dt.isoformat(dt.utcnow())}   \n")
            self.mapper.migration_report.write_migration_report(report_file)
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

    def read_records(self, reader, source_file: FileDefinition):
        with open(self.folder_structure.failed_bibs_file, "wb") as failed_bibs_file:
            for idx, record in enumerate(reader):
                self.mapper.migration_report.add_general_statistics(
                    "Records in file before parsing"
                )
                try:
                    if record is None:
                        self.mapper.migration_report.add_general_statistics(
                            "Records with encoding errors - parsing failed",
                        )
                        failed_bibs_file.write(reader.current_chunk)
                        raise TransformationRecordFailedError(
                            f"Index in {source_file.file_name}:{idx}",
                            f"MARC parsing error: {reader.current_exception}",
                            "Failed records stored in results/failed_bib_records.mrc",
                        )

                    else:
                        self.set_leader(record)
                        self.mapper.migration_report.add_general_statistics(
                            "Records successfully parsed from MARC21",
                        )
                        self.processor.process_record(
                            idx, record, source_file.suppressed
                        )
                except TransformationRecordFailedError as error:
                    error.log_it()
            logging.info("Done reading %s records from file", idx + 1)

    @staticmethod
    def set_leader(marc_record: Record):
        new_leader = marc_record.leader
        marc_record.leader = f"{new_leader[:9]}a{new_leader[10:]}"
