import logging
from typing import Annotated
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class BibsTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        add_administrative_notes_with_legacy_ids: Annotated[
            bool,
            Field(
                description=(
                    "If set to true, an Administrative note will be added to the records "
                    "containing the legacy ID. Use this in order to protect the values from "
                    "getting overwritten by overlays,"
                ),
            ),
        ] = True
        migration_task_type: str
        hrid_handling: Optional[HridHandling] = HridHandling.default
        deactivate035_from001: Optional[bool] = False
        files: List[FileDefinition]
        ils_flavour: IlsFlavour
        tags_to_delete: Optional[List[str]] = []
        reset_hrid_settings: Optional[bool] = False
        never_update_hrid_settings: Optional[bool] = False
        create_source_records: Annotated[
            bool, Field(description="Controls wheter or not to retain the MARC records in SRS.")
        ] = True

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
        self.processor: MarcFileProcessor
        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_configuration.files
        )
        self.mapper = BibsRulesMapper(self.folio_client, library_config, self.task_configuration)
        self.bib_ids: set = set()
        if (
            self.task_configuration.reset_hrid_settings
            and not self.task_configuration.never_update_hrid_settings
        ):
            self.mapper.reset_instance_hrid_counter()
        logging.info("Init done")

    def do_work(self):
        self.do_work_marc_transformer()

    def wrap_up(self):
        logging.info("Done. Transformer wrapping up...")
        self.extradata_writer.flush()
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
        self.clean_out_empty_logs()
