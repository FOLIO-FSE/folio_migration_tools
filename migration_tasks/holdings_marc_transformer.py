'''Main "script."'''
import csv
import json
import logging
import os
import sys
from datetime import datetime as dt
from os import listdir
from os.path import isfile

import requests
from argparse_prompt import PromptParser
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient.FolioClient import FolioClient
from migration_tools.colors import Bcolors
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from migration_tools.marc_rules_transformation.holdings_processor import (
    HoldingsProcessor,
)
from migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper
from migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from migration_tools.migration_configuration import MigrationConfiguration
from pymarc import MARCReader
from pymarc.reader import MARCReader
from pymarc.record import Record

from migration_tasks.migration_task_base import MigrationTaskBase


class HoldingsMarcTransformer(MigrationTaskBase):
    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.holdings

    def __init__(self, configuration: MigrationConfiguration):
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(configuration)
        self.instance_id_map = {}

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

    def do_work(self):
        files = [
            os.path.join(self.folder_structure.legacy_records_folder, f)
            for f in listdir(self.folder_structure.legacy_records_folder)
            if isfile(os.path.join(self.folder_structure.legacy_records_folder, f))
        ]
        with open(self.folder_structure.locations_map_path) as location_map_f, open(
            self.folder_structure.mfhd_rules_path
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
                rules_file["defaultLocationCode"],
                self.configuration.args.default_call_number_type_name,
            )
            mapper.mappings = rules_file["rules"]

            processor = HoldingsProcessor(
                mapper,
                self.folio_client,
                self.folder_structure,
                self.configuration.args.suppress,  # pylint: disable=no-member
            )
            for records_file in files:
                try:
                    with open(records_file, "rb") as marc_file:
                        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                        reader.hide_utf8_warnings = True
                        reader.force_utf8 = True
                        logging.info("Running %s", records_file)
                        read_records(reader, processor)
                except TransformationProcessError as tpe:
                    logging.critical(tpe)
                    sys.exit()
                except Exception:
                    logging.exception(
                        "Failure in Main: %s", records_file, stack_info=True
                    )
            processor.wrap_up()

    def wrap_up(self):
        logging.info("wapping up")

    @staticmethod
    def add_arguments(sub_parser):
        MigrationTaskBase.add_common_arguments(sub_parser)
        sub_parser.add_argument(
            "--default_call_number_type_name",
            help=(
                "Name of the default callnumber type. Needs to exist "
                " in the tenant verbatim"
            ),
            default="Other scheme",
        )
        sub_parser.add_argument(
            "--suppress",
            "-ds",
            help="This batch of records are to be suppressed in FOLIO.",
            default=False,
            type=bool,
        )


def read_records(reader, processor: HoldingsProcessor):
    for idx, record in enumerate(reader):
        try:
            if record is None:
                raise TransformationRecordFailedError(
                    f"Index in file:{idx}",
                    f"MARC parsing error: {reader.current_exception}",
                    f"{reader.current_chunk}",
                )
            else:
                processor.process_record(record)
        except TransformationRecordFailedError as error:
            error.log_it()
        except ValueError as error:
            logging.error(error)
