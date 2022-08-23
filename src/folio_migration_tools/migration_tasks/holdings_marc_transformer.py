'''Main "script."'''
import csv
import json
import logging
import sys
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import BaseModel
from pymarc import MARCReader

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.holdings_processor import (
    HoldingsProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs


class HoldingsMarcTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        legacy_id_marc_path: str
        deduplicate_holdings_statements: Optional[bool] = True
        migration_task_type: str
        use_tenant_mapping_rules: bool
        hrid_handling: HridHandling
        files: List[FileDefinition]
        mfhd_mapping_file_name: str
        location_map_file_name: str
        default_call_number_type_name: str
        fallback_holdings_type_id: str
        create_source_records: Optional[bool] = False

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
        if self.task_configuration.hrid_handling == HridHandling.preserve001:
            raise TransformationProcessError(
                "This HridHandling is not yet implemented for MFHD. "
                "Choose default or default_reset"
            )
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
            if self.task_configuration.hrid_handling == HridHandling.default_reset:
                logging.info("Resetting HRID settings to 1")
                mapper.holdings_hrid_counter = 1
            mapper.migration_report.set(
                Blurbs.GeneralStatistics, "HRID starting number", mapper.holdings_hrid_counter
            )
            processor = HoldingsProcessor(mapper, self.folder_structure)
            for file_def in self.task_config.files:
                self.process_single_file(file_def, processor)
            processor.wrap_up()

    def process_single_file(self, file_def: FileDefinition, processor: HoldingsProcessor):
        try:
            with open(
                self.folder_structure.legacy_records_folder / file_def.file_name,
                "rb",
            ) as marc_file:
                reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                reader.hide_utf8_warnings = True
                reader.force_utf8 = False
                logging.info("Running %s", file_def.file_name)
                read_records(reader, processor, file_def)
        except TransformationProcessError as tpe:
            logging.critical(tpe)
            sys.exit(1)
        except Exception:
            logging.exception("Failure in Main: %s", file_def.file_name, stack_info=True)

    def wrap_up(self):
        logging.info("wapping up")


def read_records(reader, processor: HoldingsProcessor, file_def: FileDefinition):
    for idx, record in enumerate(reader):
        try:
            if record is None:
                processor.mapper.migration_report.add_general_statistics(
                    "Records with encoding errors. See data issues log for details"
                )
                raise TransformationRecordFailedError(
                    f"Index in file:{idx}",
                    f"MARC parsing error: {reader.current_exception}",
                    f"{reader.current_chunk}",
                )
            else:
                processor.process_record(record, file_def)
        except TransformationRecordFailedError as error:
            error.log_it()
        except ValueError as error:
            logging.error(error)
