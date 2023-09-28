import logging
from typing import Annotated
from typing import List
import i18n

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_authorities import (
    AuthorityMapper,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class AuthorityTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: Annotated[
            str,
            Field(
                description=(
                    "Name of this migration task. The name is being used to call the specific "
                    "task, and to distinguish tasks of similar types"
                )
            ),
        ]
        migration_task_type: Annotated[
            str,
            Field(
                title="Migration task type",
                description=("The type of migration task you want to perform"),
            ),
        ]
        files: Annotated[
            List[FileDefinition],
            Field(
                title="Source files", description=("List of MARC21 files with authority records")
            ),
        ]
        ils_flavour: Annotated[
            IlsFlavour,
            Field(
                title="ILS flavour", description="The type of ILS you are migrating records from."
            ),
        ]
        tags_to_delete: Annotated[
            List[str],
            Field(
                title="Tags to delete from MARC record",
                description=(
                    "Tags in the incoming MARC authority that the process should remove "
                    "before adding them into FOLIO. These tags will be used in the "
                    "transformation before getting removed."
                ),
            ),
        ] = []
        create_source_records: Annotated[
            bool,
            Field(
                title="Create source records",
                description=(
                    "Controls wheter or not to retain the MARC records in "
                    "Source Record Storage."
                ),
            ),
        ] = True

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.authorities

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
        self.do_work_marc_transformer()

    def wrap_up(self):
        logging.info("Done. Transformer Wrapping up...")
        self.extradata_writer.flush()
        self.processor.wrap_up()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                i18n.t("Authority records transformation report"),
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
        self.clean_out_empty_logs()
