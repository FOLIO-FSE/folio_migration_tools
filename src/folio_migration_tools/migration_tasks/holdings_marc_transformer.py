'''Main "script."'''
import csv
import json
import logging
import sys
from os import listdir
from os.path import isfile
from typing import List, Optional
from pydantic import BaseModel

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.holdings_processor import (
    HoldingsProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from pymarc import MARCReader

from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase


class HoldingsMarcTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        legacy_id_marc_path: str
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
            "",
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
            self.default_holdings_type["name"],
        )
        self.instance_id_map = self.load_id_map(
            self.folder_structure.instance_id_map_path
        )
        logging.info("%s Instance ids in map", len(self.instance_id_map))
        logging.info("Init done")

    def do_work(self):
        files = self.list_source_files()
        loc_map_path = (
            self.folder_structure.mapping_files_folder
            / self.task_config.location_map_file_name
        )
        map_path = (
            self.folder_structure.mapping_files_folder
            / self.task_config.mfhd_mapping_file_name
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
            processor = HoldingsProcessor(mapper, self.folder_structure)
            for file_def in files:
                self.process_single_file(file_def, processor)
            processor.wrap_up()

    def list_source_files(self):
        files = [
            f
            for f in self.task_config.files
            if isfile(self.folder_structure.legacy_records_folder / f.file_name)
        ]
        if not any(files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                "",
                f"Files {ret_str} not found in {self.folder_structure.data_folder / 'holdings'}",
            )

        return files

    def process_single_file(
        self, file_def: FileDefinition, processor: HoldingsProcessor
    ):
        try:
            with open(
                self.folder_structure.legacy_records_folder / file_def.file_name,
                "rb",
            ) as marc_file:
                reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                reader.hide_utf8_warnings = True
                reader.force_utf8 = True
                logging.info("Running %s", file_def.file_name)
                read_records(reader, processor, file_def)
        except TransformationProcessError as tpe:
            logging.critical(tpe)
            sys.exit()
        except Exception:
            logging.exception(
                "Failure in Main: %s", file_def.file_name, stack_info=True
            )

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
