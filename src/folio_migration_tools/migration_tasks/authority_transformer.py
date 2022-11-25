import logging
from typing import Annotated
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.marc_reader_wrapper import (
    MARCReaderWrapper,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_autorities import (
    AuthorityMapper,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class AuthorityTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: Annotated[str, Field(description=("Name of this task"))]
        migration_task_type: Annotated[
            str, Field(description=("The string represenation of this class. Do not set"))
        ]
        files: Annotated[
            List[FileDefinition],
            Field(description=("List of MARC21 files with authority records")),
        ]
        ils_flavour: IlsFlavour
        tags_to_delete: Optional[List[str]] = []

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.athorities

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        super().__init__(library_config, task_config, use_logging)
        self.processor: MarcFileProcessor
        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_configuration.files
        )
        self.mapper: AuthorityMapper = AuthorityMapper(
            self.folio_client, library_config, task_config
        )
        self.auth_ids: set = set()
        logging.info("Init done")

    def do_work(self):
        logging.info("Starting....")
        with open(self.folder_structure.created_objects_path, "w+") as created_records_file:
            self.processor = MarcFileProcessor(
                self.mapper,
                self.folio_client,
                created_records_file,
                self.folder_structure,
            )
            for file_def in self.task_configuration.files:
                MARCReaderWrapper.process_single_file(
                    file_def,
                    self.processor,
                    self.folder_structure.failed_auth_file,
                    self.folder_structure,
                )

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        self.extradata_writer.flush()
        self.processor.wrap_up()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                "Autority records transformation report",
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
