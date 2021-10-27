'''Main "script."'''
import csv
import ctypes
import json
import logging
import sys
import time
import traceback
import uuid
from os import listdir
from os.path import isfile, join
from pathlib import Path
from typing import List

import requests
from argparse_prompt import PromptParser
from folioclient.FolioClient import FolioClient

from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.main_base import MainBase
from migration_tools.mapping_file_transformation.item_mapper import ItemMapper
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.report_blurbs import Blurbs

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


def setup_holdings_id_map(folder_structure: FolderStructure):
    logging.info("Loading holdings id map. This can take a while...")
    with open(folder_structure.holdings_id_map_path, "r") as holdings_id_map_file:
        holdings_id_map = json.load(holdings_id_map_file)
        logging.info("Loaded %s holdings ids", len(holdings_id_map))
        return holdings_id_map


class Worker(MainBase):
    """Class that is responsible for the acutal work"""

    def __init__(
        self, source_files, folio_client: FolioClient, folder_structure: FolderStructure
    ):
        super().__init__()
        self.total_records = 0
        self.folio_keys = []
        self.folder_structure = folder_structure
        self.folio_client = folio_client
        self.items_map = self.setup_records_map()
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.items_map
        )
        self.source_files = source_files
        csv.register_dialect("tsv", delimiter="\t")
        self.failed_files: List[str] = list()
        if "statisticalCodes" in self.folio_keys:
            statcode_mapping = self.load_ref_data_mapping_file(
                "statisticalCodeIds",
                self.folder_structure.statistical_codes_map_path,
                False,
            )
        else:
            statcode_mapping = None

        if self.folder_structure.temp_loan_type_map_path.is_file():
            temporary_loan_type_mapping = self.load_ref_data_mapping_file(
                "temporaryLoanTypeId", self.folder_structure.temp_loan_type_map_path
            )
        else:
            logging.info(
                "%s not found. No temporary loan type mapping will be performed",
                self.folder_structure.temp_loan_type_map_path,
            )
            temporary_loan_type_mapping = None

        if self.folder_structure.temp_locations_map_path.is_file():
            temporary_location_mapping = self.load_ref_data_mapping_file(
                "temporaryLocationId", self.folder_structure.temp_locations_map_path
            )
        else:
            logging.info(
                "%s not found. No temporary location mapping will be performed",
                self.folder_structure.temp_locations_map_path,
            )
            temporary_location_mapping = None
        self.mapper = ItemMapper(
            self.folio_client,
            self.items_map,
            self.load_ref_data_mapping_file(
                "materialTypeId", self.folder_structure.material_type_map_path
            ),
            self.load_ref_data_mapping_file(
                "permanentLoanTypeId", self.folder_structure.loan_type_map_path
            ),
            self.load_ref_data_mapping_file(
                "permanentLocationId", self.folder_structure.locations_map_path
            ),
            self.load_ref_data_mapping_file(
                "itemLevelCallNumberTypeId",
                self.folder_structure.call_number_type_map_path,
                False,
            ),
            setup_holdings_id_map(self.folder_structure),
            statcode_mapping,
            self.load_ref_data_mapping_file(
                "status.name", self.folder_structure.item_statuses_map_path, False
            ),
            temporary_loan_type_mapping,
            temporary_location_mapping,
        )
        logging.info("Init done")

    def load_ref_data_mapping_file(
        self, folio_property_name: str, map_file_path: Path, required: bool = True
    ):
        if (
            folio_property_name in self.folio_keys
            or required
            or folio_property_name.startswith("statisticalCodeIds")
        ):
            try:
                with open(map_file_path) as map_file:
                    ref_data_map = list(csv.DictReader(map_file, dialect="tsv"))
                    logging.info(
                        "Found %s rows in %s map",
                        len(ref_data_map),
                        folio_property_name,
                    )
                    logging.info(
                        "%s will be used for determinig %s",
                        ",".join(ref_data_map[0].keys()),
                        folio_property_name,
                    )
                    return ref_data_map
            except Exception as ee:
                raise TransformationProcessError(
                    f"{folio_property_name} not mapped in legacy->folio mapping file "
                    f"({map_file_path}) ({ee}). Did you map this field, "
                    "but forgot to add a mapping file?"
                )
        else:
            logging.info("No mapping setup for %s", folio_property_name)
            logging.info("%s will have default mapping if any ", folio_property_name)
            logging.info(
                "Add a file named %s and add the field to "
                "the item.mapping.json file.",
                map_file_path,
            )
            return None

    def setup_records_map(self):
        with open(self.folder_structure.items_map_path) as items_mapper_f:
            items_map = json.load(items_mapper_f)
            logging.info("%s fields in item mapping file map", len(items_map["data"]))
            mapped_fields = (
                f
                for f in items_map["data"]
                if f["legacy_field"] and f["legacy_field"] != "Not mapped"
            )
            logging.info(
                "%s Mapped fields in item mapping file map", len(list(mapped_fields))
            )
            return items_map

    def work(self):
        logging.info("Starting....")
        with open(self.folder_structure.created_objects_path, "w+") as results_file:
            for file_name in self.source_files:
                try:
                    self.process_single_file(file_name, results_file)
                except Exception as ee:
                    error_str = f"\n\nProcessing of {file_name} failed:\n{ee}."
                    logging.exception(error_str, stack_info=True)
                    logging.fatal(
                        "Check source files for empty lines or missing reference data. Halting"
                    )
                    self.mapper.migration_report.add(
                        Blurbs.FailedFiles, f"{file_name} - {ee}"
                    )
                    logging.fatal(error_str)
                    sys.exit()
        logging.info(
            f"processed {self.total_records:,} records "
            f"in {len(self.source_files)} files"
        )

    def process_single_file(self, file_name, results_file):
        logging.info("Processing %s", file_name)
        records_in_file = 0
        with open(file_name, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                "Number of files processed"
            )
            start = time.time()
            for idx, record in enumerate(
                self.mapper.get_objects(records_file, file_name)
            ):
                try:
                    if idx == 0:
                        logging.info("First legacy record:")
                        logging.info(json.dumps(record, indent=4))
                    folio_rec = self.mapper.do_map(record, f"row {idx}")
                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(folio_rec, indent=4))
                    self.handle_circiulation_notes(folio_rec)
                    # TODO: turn this into a asynchrounous task
                    Helper.write_to_file(results_file, folio_rec)
                    self.mapper.migration_report.add_general_statistics(
                        "Number of records written to disk"
                    )
                    self.mapper.report_folio_mapping(folio_rec, self.mapper.schema)
                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as data_error:
                    self.mapper.handle_transformation_record_failed_error(
                        idx, data_error
                    )
                except AttributeError as attribute_error:
                    traceback.print_exc()
                    logging.fatal(attribute_error)
                    logging.info("Quitting...")
                    sys.exit()
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)
                self.mapper.migration_report.add(
                    Blurbs.GeneralStatistics,
                    f"Number of Legacy items in {file_name}",
                )
                self.mapper.migration_report.add_general_statistics(
                    "Number of Legacy items in total"
                )
                self.print_progress(idx, start)
                records_in_file = idx + 1

            total_records = 0
            total_records += records_in_file
            logging.info(
                f"Done processing {file_name} containing {records_in_file:,} records. "
                f"Total records processed: {total_records:,}"
            )
        self.total_records = total_records

    def handle_circiulation_notes(self, folio_rec):
        for circ_note in folio_rec.get("circulationNotes", []):
            circ_note["id"] = str(uuid.uuid4())
            circ_note["source"] = {
                "id": self.folio_client.current_user,
                "personal": {"lastName": "Data", "firstName": "Migration"},
            }

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        with open(
            self.folder_structure.migration_reports_file, "w"
        ) as migration_report_file:
            logging.info(
                "Writing migration and mapping report to %s",
                self.folder_structure.migration_reports_file,
            )
            Helper.write_migration_report(
                migration_report_file, self.mapper.migration_report
            )
            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        logging.info("All done!")


def parse_args():
    """Parse CLI Arguments"""
    parser = PromptParser()
    parser.add_argument("base_folder", help="Base folder of the client.")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("--password", help="the api users password", secure=True)
    parser.add_argument(
        "--default_call_number_type_id",
        help="UUID of the default callnumber type",
        default="95467209-6d7b-468b-94df-0f5d7ad2747d",
    )
    parser.add_argument(
        "--suppress",
        "-ds",
        help="This batch of records are to be suppressed in FOLIO.",
        default=False,
        type=bool,
    )
    parser.add_argument(
        "--time_stamp",
        "-ts",
        help="Time Stamp String (YYYYMMDD-HHMMSS) from Instance transformation. Required",
    )
    parser.add_argument(
        "--log_level_debug",
        "-debug",
        help="Set log level to debug",
        default=False,
        type=bool,
    )
    args = parser.parse_args()
    if len(args.time_stamp) != 15:
        logging.critical(f"Time stamp ({args.time_stamp}) is not set properly")
        sys.exit()
    logging.info("Okapi URL:\t%s", args.okapi_url)
    logging.info("Tenant Id:\t%s", args.tenant_id)
    logging.info("Username:   \t%s", args.username)
    logging.info("Password:   \tSecret")
    return args


def main():
    """Main Method. Used for bootstrapping."""
    args = parse_args()
    folder_structure: FolderStructure = FolderStructure(
        args.base_folder, args.time_stamp
    )
    folder_structure.setup_migration_file_structure("item")
    MainBase.setup_logging(folder_structure, args.log_level_debug)
    folder_structure.log_folder_structure()

    # Source data files
    files = [
        join(folder_structure.legacy_records_folder, f)
        for f in listdir(folder_structure.legacy_records_folder)
        if isfile(join(folder_structure.legacy_records_folder, f))
    ]
    logging.info("Files to process:")
    for f in files:
        logging.info("\t%s", f)

    try:
        folio_client = FolioClient(
            args.okapi_url, args.tenant_id, args.username, args.password
        )
    except requests.exceptions.SSLError:
        logging.critical("SSL error. Check your VPN or Internet connection. Exiting")
        sys.exit()
    try:
        worker = Worker(files, folio_client, folder_structure)
        worker.work()
        worker.wrap_up()
    except TransformationProcessError as pocess_error:
        logging.critical(pocess_error)
        logging.critical("Halting")
        sys.exit()
    except Exception as process_error:
        logging.info("======= UNKNOWN ERROR in MAIN: %s ===========", process_error)
        logging.exception("=======Stack Trace===========")


if __name__ == "__main__":
    main()
