'''Main "script."'''
import argparse
import csv
import ctypes
import json
import logging
from marc_to_folio import custom_exceptions
from marc_to_folio.mapping_file_transformation.mapper_base import MapperBase
from marc_to_folio.helper import Helper

from argparse_prompt import PromptParser
from marc_to_folio.main_base import MainBase
import os
import time
import traceback
from os import listdir
from os.path import isfile, join
from typing import Dict, List
from datetime import datetime

import pymarc
from folioclient.FolioClient import FolioClient

from marc_to_folio.custom_exceptions import (
    TransformationCriticalDataError,
    TransformationProcessError,
)
from marc_to_folio.mapping_file_transformation.item_mapper import ItemMapper

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


def setup_path(path, filename):
    path = os.path.join(path, filename)
    if not isfile(path):
        raise Exception(f"No file called {filename} present in {path}")
    return path


def setup_holdings_id_map(result_path):
    holdings_id_dict_path = setup_path(result_path, "holdings_id_map.json")
    with open(holdings_id_dict_path, "r") as holdings_id_map_file:
        holdings_id_map = json.load(holdings_id_map_file)
        logging.info(f"Loaded {len(holdings_id_map)} holdings ids")
        return holdings_id_map


class Worker(MainBase):
    """Class that is responsible for the acutal work"""

    def __init__(self, source_files, args):
        self.folio_keys = []
        self.result_path = args.result_path
        self.map_folder_path = args.map_folder_path
        self.holdings_id_map = setup_holdings_id_map(self.result_path)
        self.folio_client = FolioClient(
            args.okapi_url, args.tenant_id, args.username, args.password
        )
        self.items_map = self.setup_records_map(args)
        self.folio_keys = MapperBase.get_mapped_folio_properties_from_map(
            self.items_map
        )
        self.source_files = source_files
        
        self.failed_files: List[str] = list()
        csv.register_dialect("tsv", delimiter="\t")

        self.mapper = ItemMapper(
                self.folio_client,
                self.items_map,
                self.load_ref_data_mapping_file("materialTypeId","material_types.tsv"),
                self.load_ref_data_mapping_file("permanentLoanTypeId","loan_types.tsv"),
                self.load_ref_data_mapping_file("permanentLocationId", "locations.tsv"),
                self.load_ref_data_mapping_file("itemLevelCallNumberTypeId","call_number_type_mapping.tsv", False),
                self.holdings_id_map,
                self.load_ref_data_mapping_file("statisticalCodeIds", "statcodes.tsv", False),
                self.load_ref_data_mapping_file("status.name", "item_statuses.tsv", False)                
            )
        logging.info("Init done")

    def load_ref_data_mapping_file(
        self, folio_property_name: str, map_file_name: str, required: bool = True
    ):
        if folio_property_name in self.folio_keys or required:
            map_path = setup_path(self.map_folder_path, map_file_name)
            try:
                with open(map_path) as map_file:
                    map = list(csv.DictReader(map_file, dialect="tsv"))
                    logging.info(f"Found {len(map)} rows in {folio_property_name} map")
                    logging.info(
                        f'{",".join(map[0].keys())} '
                        f"will be used for determinig {folio_property_name}"
                    )
                    return map
            except Exception as ee:
                raise TransformationProcessError(
                    f"{folio_property_name} not mapped in legacy->folio mapping file "
                    f"({map_file_name}) ({ee}). Did you map this field, "
                    "but forgot to add a mapping file?"
                )
        else:
            logging.info(
                f"No mapping setup for {folio_property_name}. "
                f"{folio_property_name} will have default mapping if any"
                f"Add a file named {map_file_name} to  {self.map_folder_path} and add the field to "
                "the item.mapping.json file."
            )
            return None

    def setup_records_map(self, args):
        items_map_path = setup_path(args.map_folder_path, "item_mapping.json")
        with open(items_map_path) as items_mapper_f:
            items_map = json.load(items_mapper_f)
            logging.info(f'{len(items_map["data"])} fields in item mapping file map')
            mapped_fields = (
                f
                for f in items_map["data"]
                if f["legacy_field"] and f["legacy_field"] != "Not mapped"
            )
            logging.info(
                f"{len(list(mapped_fields))} Mapped fields in item mapping file map"
            )
            return items_map

    def work(self):
        logging.info("Starting....")
        with open(
            os.path.join(self.result_path, "folio_items.json"), "w+"
        ) as results_file:
            for file_name in self.source_files:
                logging.info(f"Processing {file_name}")
                try:
                    self.process_single_file(file_name, results_file)
                except Exception as ee:
                    error_str = (
                        f"\n\nProcessing of {file_name} failed:\n{ee}."
                        "Check source files for empty lines or missing reference data. Halting"
                    )
                    logging.exception(error_str, stack_info=True)
                    self.mapper.add_to_migration_report(
                        "Failed files", f"{file_name} - {ee}"
                    )
                    logging.fatal(error_str)
                    exit()
        logging.info(
            f"processed {self.total_records:,} records in {len(self.source_files)} files"
        )

    def process_single_file(self, file_name, results_file):
        with open(file_name, encoding="utf-8") as records_file:
            self.mapper.add_to_migration_report(
                "General statistics", "Number of files processed"
            )
            start = time.time()
            for idx, record in enumerate(
                self.mapper.get_objects(records_file, file_name)
            ):
                try:
                    folio_rec = self.mapper.do_map(record, f"row {idx}")
                    # Hard code circ note sources
                    # TODO: Add more levels (recursive) to mapping
                    for circ_note in folio_rec.get("circulationNotes",[]):
                        circ_note["source"] = {
                            "id": self.folio_client.current_user,
                            "personal":{
                                "lastName":"Data",
                                "firstName": "Migration"
                            }
                        }

                    Helper.write_to_file(results_file, folio_rec)
                    self.mapper.add_to_migration_report(
                        "General statistics",
                        "Number of records written to disk",
                    )
                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationCriticalDataError as data_error:
                    self.mapper.handle_transformation_critical_error(idx, data_error)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)

                self.mapper.add_to_migration_report(
                    "General statistics",
                    f"Number of Legacy items in {file_name}",
                )
                self.mapper.add_to_migration_report(
                    "General statistics", f"Number of Legacy items in total"
                )
                if idx > 1 and idx % 10000 == 0:
                    elapsed = idx / (time.time() - start)
                    elapsed_formatted = "{0:.4g}".format(elapsed)
                    logging.info(
                        f"{idx:,} records processed. " f"Recs/sec: {elapsed_formatted} "
                    )

            total_records = 0
            total_records += idx
            logging.info(
                f"Done processing {file_name} containing {idx:,} records. "
                f"Total records processed: {total_records:,}"
            )
        self.total_records = total_records

        if self.mapper.num_exeptions > 10:
            raise Exception(
                f"Number of exceptions exceeded limit of {self.mapper.num_exeptions}. Stopping."
            )

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        migration_report_file_path = os.path.join(
            self.result_path,
            "item_transformation_report.md",
        )
        with open(migration_report_file_path, "w") as migration_report_file:
            logging.info(
                f"Writing migration- and mapping report to {migration_report_file_path}"
            )
            self.mapper.write_migration_report(migration_report_file)
            self.mapper.print_mapping_report(migration_report_file, self.total_records)
        logging.info("All done!")


def parse_args():
    """Parse CLI Arguments"""
    parser = PromptParser()
    parser.add_argument("records_path", help="path to source records folder", type=str)
    parser.add_argument("result_path", help="path to results folder", type=str)
    parser.add_argument("map_folder_path", help="Path to folder with mapping files", type=str)
    parser.add_argument("okapi_url", help="OKAPI base url")
    parser.add_argument("tenant_id", help="id of the FOLIO tenant.")
    parser.add_argument("username", help="the api user")
    parser.add_argument("--password", help="the api users password", secure=True)
    parser.add_argument(
        "--log_level_debug",
        "-debug",
        help="Set log level to debug",
        default=False,
        type=bool,
    )
    return parser.parse_args()


def main():
    """Main Method. Used for bootstrapping. """
    args = parse_args()
    Worker.setup_logging(
        os.path.join(args.result_path, "item_transformation.log"), args.log_level_debug
    )

    # Source data files
    files = [
        join(args.records_path, f)
        for f in listdir(args.records_path)
        if isfile(join(args.records_path, f))
    ]
    logging.info(f"Files to process:")
    for f in files:
        logging.info(f"\t{f}")

    # All the paths...
    try:
        worker = Worker(files, args)
        worker.work()
        worker.wrap_up()
    except Exception as process_error:
        logging.info(f"=======ERROR in MAIN: {process_error}===========")
        logging.exception("=======Stack Trace===========")


if __name__ == "__main__":
    main()
