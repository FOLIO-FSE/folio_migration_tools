'''Main "script."'''

import csv
import json
import logging
from typing import Annotated, List

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_tasks.migration_task_base import MarcTaskConfigurationBase, MigrationTaskBase


class HoldingsMarcTransformer(MigrationTaskBase):
    class TaskConfiguration(MarcTaskConfigurationBase):
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
                title="Source files",
                description=("List of MARC21 files with holdings records"),
            ),
        ]
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
        holdings_type_uuid_for_boundwiths: Annotated[
            str,
            Field(
                title="Holdings Type for Boundwith Holdings",
                description=(
                    "UUID for a Holdings type (set in Settings->Inventory) "
                    "for Bound-with Holdings)"
                ),
            ),
        ] = ""
        boundwith_relationship_file_path: Annotated[
            str,
            Field(
                title="Boundwith relationship file path",
                description=(
                    "Path to a file outlining Boundwith relationships, in the style of Voyager."
                    " A TSV file with MFHD_ID and BIB_ID headers and values"
                ),
            ),
        ] = ""
        update_hrid_settings: Annotated[
            bool,
            Field(
                title="Update HRID settings",
                description="At the end of the run, update FOLIO with the HRID settings",
            ),
        ] = True
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
        legacy_id_marc_path: Annotated[
            str,
            Field(
                title="Path to legacy id in the records",
                description=(
                    "The path to the field where the legacy id is located. "
                    "Example syntax: '001' or '951$c'"
                ),
            ),
        ]
        deduplicate_holdings_statements: Annotated[
            bool,
            Field(
                title="Deduplicate holdings statements",
                description=(
                    "If set to False, duplicate holding statements within the same record will "
                    "remain in place"
                ),
            ),
        ] = True
        location_map_file_name: Annotated[
            str,
            Field(
                title="Path to location map file",
                description="Must be a TSV file located in the mapping_files folder",
            ),
        ]
        default_call_number_type_name: Annotated[
            str,
            Field(
                title="Default callnumber type name",
                description="The name of the callnumber type that will be used as fallback",
            ),
        ]
        fallback_holdings_type_id: Annotated[
            str,
            Field(
                title="Fallback holdings type id",
                description="The UUID of the Holdings type that will be used for unmapped values",
            ),
        ]
        supplemental_mfhd_mapping_rules_file: Annotated[
            str,
            Field(
                title="Supplemental MFHD mapping rules file",
                description="The name of the file in the mapping_files directory containing supplemental MFHD mapping rules",
            ),
        ] = ""

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.holdings

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.task_config = task_config
        self.holdings_types = list(
            self.folio_client.folio_get_all("/holdings-types", "holdingsTypes")
        )
        self.default_holdings_type = next(
            (
                h
                for h in self.holdings_types
                if h["id"] == self.task_config.fallback_holdings_type_id
            ),
            {"name": ""},
        )
        if not self.default_holdings_type:
            raise TransformationProcessError(
                "",
                (
                    f"Holdings type with ID {self.task_config.fallback_holdings_type_id}"
                    " not found in FOLIO."
                ),
            )
        logging.info(
            "%s will be used as default holdings type",
            self.default_holdings_type.get("name", ""),
        )

        # Load Boundwith relationship map
        self.boundwith_relationship_map = []
        if self.task_config.boundwith_relationship_file_path:
            with open(
                self.folder_structure.legacy_records_folder
                / self.task_config.boundwith_relationship_file_path
            ) as boundwith_relationship_file:
                self.boundwith_relationship_map = list(
                    csv.DictReader(boundwith_relationship_file, dialect="tsv")
                )
            logging.info(
                "Rows in Bound with relationship map: %s",
                len(self.boundwith_relationship_map),
            )

        location_map_path = (
            self.folder_structure.mapping_files_folder
            / self.task_config.location_map_file_name
        )
        with open(location_map_path) as location_map_file:
            self.location_map = list(csv.DictReader(location_map_file, dialect="tsv"))
            logging.info("Locations in map: %s", len(self.location_map))

        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_config.files
        )
        self.instance_id_map = self.load_instance_id_map(True)
        self.mapper = RulesMapperHoldings(
            self.folio_client,
            self.location_map,
            self.task_config,
            self.library_configuration,
            self.instance_id_map,
            self.boundwith_relationship_map,
        )
        self.add_supplemental_mfhd_mappings()
        if (
            self.task_configuration.reset_hrid_settings
            and self.task_configuration.update_hrid_settings
        ):
            self.mapper.hrid_handler.reset_holdings_hrid_counter()
        logging.info("%s Instance ids in map", len(self.instance_id_map))
        logging.info("Init done")

    def add_supplemental_mfhd_mappings(self):
        if self.task_config.supplemental_mfhd_mapping_rules_file:
            try:
                with open(
                    (
                        self.folder_structure.mapping_files_folder
                        / self.task_config.supplemental_mfhd_mapping_rules_file
                    ),
                    "r",
                ) as new_rules_file:
                    new_rules = json.load(new_rules_file)
                    if not isinstance(new_rules, dict):
                        raise TransformationProcessError(
                            "",
                            "Supplemental MFHD mapping rules file must contain a dictionary",
                            json.dumps(new_rules),
                        )
            except FileNotFoundError:
                raise TransformationProcessError(
                    "",
                    "Provided supplemental MFHD mapping rules file not found",
                    self.task_config.supplemental_mfhd_mapping_rules_file,
                )
        else:
            new_rules = {}
        self.mapper.integrate_supplemental_mfhd_mappings(new_rules)

    def do_work(self):
        self.do_work_marc_transformer()

    def wrap_up(self):
        logging.info("Done. Transformer Wrapping up...")
        self.extradata_writer.flush()
        self.processor.wrap_up()
        if self.mapper.boundwith_relationship_map:
            with open(
                self.folder_structure.boundwith_relationships_map_path, "w+"
            ) as boundwith_relationship_file:
                logging.info(
                    "Writing boundwiths relationship map to %s",
                    boundwith_relationship_file.name,
                )
                for key, val in self.mapper.boundwith_relationship_map.items():
                    boundwith_relationship_file.write(json.dumps((key, val)) + "\n")

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
