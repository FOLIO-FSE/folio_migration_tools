'''Main "script."'''
import csv
import json
import logging
import os
import sys
from os import listdir
from os.path import isfile
from typing import List
from pydantic import BaseModel

from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    LibraryConfiguration,
)
from migration_tools.marc_rules_transformation.holdings_processor import (
    HoldingsProcessor,
)
from migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from pymarc import MARCReader

from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase


class HoldingsMarcTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        migration_task_type: str
        use_tenant_mapping_rules: bool
        hrid_handling: HridHandling
        files: List[FileDefinition]
        mfhd_mapping_file_name: str
        location_map_file_name: str
        default_call_number_type_name: str
        default_holdings_type_id: str

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.holdings

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(library_config, task_config)
        self.instance_id_map = {}
        self.task_config = task_config
        self.default_holdings_type = next(
            (
                h
                for h in self.holdings_types
                if h["id"] == self.task_config.default_holdings_type_id
            ),
            "",
        )
        if not self.default_holdings_type:
            raise TransformationProcessError(
                (
                    f"Holdings type with ID {self.task_config.default_holdings_type_id}"
                    " not found in FOLIO."
                )
            )
        logging.info(
            "%s will be used as default holdings type",
            self.default_holdings_type["name"],
        )

    def do_work(self):
        files = [
            f
            for f in self.task_config.files
            if isfile(self.folder_structure.legacy_records_folder / f.file_name)
        ]
        if not any(files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                f"Files {ret_str} not found in {self.folder_structure.data_folder / 'items'}"
            )
        with open(
            self.folder_structure.mapping_files_folder
            / self.task_config.location_map_file_name
        ) as location_map_f, open(
            self.folder_structure.mapping_files_folder
            / self.task_config.mfhd_mapping_file_name
        ) as mapping_rules_file:
            self.load_instance_id_map()
            location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
            rules_file = json.load(mapping_rules_file)
            logging.info("Locations in map: %s", len(location_map))
            logging.info(any(location_map))
            logging.info("Default location code %s", rules_file["defaultLocationCode"])
            logging.info("%s Instance ids in map", len(self.instance_id_map))
            mapper = RulesMapperHoldings(
                self.folio_client,
                self.instance_id_map,
                location_map,
                self.task_config.default_call_number_type_name,
                self.task_config.default_holdings_type_id,
            )
            mapper.mappings = rules_file["rules"]

            processor = HoldingsProcessor(mapper, self.folder_structure)
            for file_def in files:
                try:
                    with open(
                        self.folder_structure.legacy_records_folder
                        / file_def.file_name,
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
            processor.wrap_up()

    def wrap_up(self):
        logging.info("wapping up")

    def load_instance_id_map(self):
        with open(self.folder_structure.instance_id_map_path) as instance_id_map_file:
            for index, json_string in enumerate(instance_id_map_file):
                # {"legacy_id", "folio_id","instanceLevelCallNumber", "suppressed"}
                map_object = json.loads(json_string)
                if index % 50000 == 0:
                    print(
                        f"{(index+1)} instance ids loaded to map {map_object['legacy_id']}",
                        end="\r",
                    )
                self.instance_id_map[map_object["legacy_id"]] = map_object
        logging.info("loaded %s migrated instance IDs", (index + 1))


def read_records(reader, processor: HoldingsProcessor, file_def: FileDefinition):
    for idx, record in enumerate(reader):
        try:
            if record is None:
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
