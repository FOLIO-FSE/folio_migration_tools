import logging
from typing import Annotated, List

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_tasks.migration_task_base import MarcTaskConfigurationBase, MigrationTaskBase


class BibsTransformer(MigrationTaskBase):
    class TaskConfiguration(MarcTaskConfigurationBase):
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
        data_import_marc: Annotated[
            bool,
            Field(
                title="Generate a MARC file for data import overlay of instances",
                description=(
                    "If set to true, the process will generate a file of binary MARC records that can"
                    "be imported into FOLIO using the Data Import APIs. If set to false, only a file"
                    "of FOLIO instance records (and optional SRS records) will be generated."
                ),
            )
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

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.instances

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        super().__init__(library_config, task_config, folio_client, use_logging)
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
