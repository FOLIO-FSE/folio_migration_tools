import logging
from typing import Annotated
from typing import List
import i18n

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
                description=("The type of migration task you want to perform."),
            ),
        ]
        files: Annotated[
            List[FileDefinition],
            Field(
                title="Source files",
                description=("List of MARC21 files with bibliographic records."),
            ),
        ]
        ils_flavour: Annotated[
            IlsFlavour,
            Field(
                title="ILS flavour", description="The type of ILS you are migrating records from."
            ),
        ]
        custom_bib_id_field: Annotated[
            str,
            Field(
                title="Custom BIB ID field",
                description=(
                    'A string representing a MARC field with optional subfield indicated by a "$" '
                    '(eg. "991$a") from which to draw legacy Bib ID. Use this in combination '
                    'with `ilsFlavour: "custom"`. Defaults to "001", and is ignored for all other '
                    "ilsFlavours."
                ),
            ),
        ] = "001"
        add_administrative_notes_with_legacy_ids: Annotated[
            bool,
            Field(
                title="Add administrative notes with legacy IDs",
                description=(
                    "If set to true, an Administrative note will be added to the records "
                    "containing the legacy ID. Use this in order to protect the values from "
                    "getting overwritten by overlays,"
                ),
            ),
        ] = True
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
        parse_cataloged_date: Annotated[
            bool,
            Field(
                title="Parse cataloged date",
                description=(
                    "Parse fields mapped to catalogedDate into a FOLIO accepted date string using "
                    "dateutil.parser. Verify results carefully when using"
                ),
            ),
        ] = False
        hrid_handling: Annotated[
            HridHandling,
            Field(
                title="HRID Handling",
                description=(
                    "Setting to default will make FOLIO generate HRIDs and move the existing "
                    "001:s into a 035, concatenated with the 003. Choosing preserve001 means "
                    "the 001:s will remain in place, and that they will also become the HRIDs"
                ),
            ),
        ] = HridHandling.default
        reset_hrid_settings: Annotated[
            bool,
            Field(
                title="Reset HRID settings",
                description=(
                    "Setting to true means the task will "
                    "reset the HRID counters for this particular record type"
                ),
            ),
        ] = False
        update_hrid_settings: Annotated[
            bool,
            Field(
                title="Update HRID settings",
                description="At the end of the run, update FOLIO with the HRID settings",
            ),
        ] = True
        deactivate035_from001: Annotated[
            bool,
            Field(
                title="Create 035 from 001 and 003",
                description=(
                    "This deactivates the FOLIO default functionality of moving the previous 001 "
                    "into a 035, prefixed with the value from 003"
                ),
            ),
        ] = False

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
            and self.task_configuration.update_hrid_settings
        ):
            self.mapper.hrid_handler.reset_instance_hrid_counter()
        logging.info("Init done")

    def do_work(self):
        self.do_work_marc_transformer()

    def wrap_up(self):
        logging.info("Done. Transformer wrapping up...")
        self.extradata_writer.flush()
        self.processor.wrap_up()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                i18n.t("Bibliographic records transformation report"),
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
