'''Main "script."'''
import csv
import json
import logging
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.marc_reader_wrapper import (
    MARCReaderWrapper,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class HoldingsMarcTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        legacy_id_marc_path: str
        deduplicate_holdings_statements: Optional[bool] = True
        migration_task_type: str
        use_tenant_mapping_rules: bool
        hrid_handling: Optional[HridHandling] = HridHandling.default
        deactivate035_from001: Optional[bool] = False
        files: List[FileDefinition]
        mfhd_mapping_file_name: str
        location_map_file_name: str
        default_call_number_type_name: str
        fallback_holdings_type_id: str
        create_source_records: Optional[bool] = False
        reset_hrid_settings: Optional[bool] = False
        never_update_hrid_settings: Optional[bool] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.holdings

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(library_config, task_config, use_logging)
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
        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_config.files
        )
        self.instance_id_map = self.load_id_map(self.folder_structure.instance_id_map_path, True)
        logging.info("%s Instance ids in map", len(self.instance_id_map))
        logging.info("Init done")

    def do_work(self):
        loc_map_path = (
            self.folder_structure.mapping_files_folder / self.task_config.location_map_file_name
        )
        map_path = (
            self.folder_structure.mapping_files_folder / self.task_config.mfhd_mapping_file_name
        )
        with open(loc_map_path) as loc_map_f, open(map_path) as map_f:
            location_map = list(csv.DictReader(loc_map_f, dialect="tsv"))
            logging.info("Locations in map: %s", len(location_map))
            rules_file = json.load(map_f)
            logging.info("Default location code %s", rules_file["defaultLocationCode"])
            mapper = RulesMapperHoldings(
                self.folio_client,
                self.instance_id_map,
                location_map,
                self.task_config,
                self.library_configuration,
            )
            mapper.mappings = rules_file["rules"]
            if (
                self.task_configuration.reset_hrid_settings
                and not self.task_configuration.never_update_hrid_settings
            ):
                mapper.reset_holdings_hrid_counter()
            processor = MarcFileProcessor(mapper, self.folder_structure)
            for file_def in self.task_config.files:
                MARCReaderWrapper.process_single_file(
                    file_def,
                    processor,
                    self.folder_structure.failed_mfhds_file,
                    self.folder_structure,
                )
            processor.wrap_up()

    def wrap_up(self):
        logging.info("wapping up")
        self.extradata_writer.flush()
