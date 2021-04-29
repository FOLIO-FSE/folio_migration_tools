'''Main "script."'''
import argparse
import csv
import ctypes
import json
import logging
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


class Worker(MainBase):
    """Class that is responsible for the acutal work"""

    def __init__(
        self,
        folio_client: FolioClient,
        mapper: ItemMapper,
        files,
        results_path,
        error_file,
    ):
        self.folio_client = folio_client
        self.files = files
        self.results_path = results_path
        self.mapper = mapper
        self.failed_files: List[str] = list()
        self.num_exeptions = 0
        self.error_file = error_file
        logging.info("Init done")

    def work(self):
        total_records = 0
        logging.info("Starting....")
        with open(
            os.path.join(self.results_path, "folio_items.json"), "w+"
        ) as results_file:
            for file_name in self.files:
                logging.info(f"Processing {file_name}")
                try:
                    with open(file_name, encoding="utf-8-sig") as records_file:
                        self.mapper.add_to_migration_report(
                            "General statistics", "Number of files processed"
                        )
                        start = time.time()
                        for idx, record in enumerate(
                            self.mapper.get_objects(records_file, file_name)
                        ):
                            try:
                                folio_rec = self.mapper.do_map(record, f"row {idx}")
                                Helper.write_to_file(results_file, folio_rec)
                                self.mapper.add_to_migration_report(
                                    "General statistics",
                                    "Number of records written to disk",
                                )
                            except TransformationProcessError as process_error:
                                logging.error(f"{idx}\t{process_error}")
                            except TransformationCriticalDataError as data_error:
                                logging.error(f"{idx}\t{data_error}")
                            except Exception as excepion:
                                self.num_exeptions += 1
                                print("\n=======ERROR===========")
                                print(
                                    f"row {idx:,} failed with the following Exception: {excepion} "
                                    f" of type {type(excepion).__name__}"
                                )
                                print("\n=======Stack Trace===========")
                                traceback.print_exc()
                                if self.num_exeptions > 10:
                                    raise Exception(
                                        f"Number of exceptions exceeded limit of "
                                        f"{self.num_exeptions}. Stopping."
                                    )
                            self.mapper.add_to_migration_report(
                                "General statistics",
                                f"Number of Legacy items in {file_name}",
                            )
                            self.mapper.add_to_migration_report(
                                "General statistics", f"Number of Legacy items in total"
                            )
                            if idx % 10000 == 0:
                                elapsed = idx / (time.time() - start)
                                elapsed_formatted = "{0:.4g}".format(elapsed)
                                logging.info(
                                    f"{idx:,} records processed. "
                                    f"Recs/sec: {elapsed_formatted} "
                                )
                        total_records += idx
                        logging.info(
                            f"Done processing {file_name} containing {idx:,} records. "
                            f"Total records processed: {total_records:,}"
                        )

                except Exception as ee:
                    error_str = (
                        f"Processing of {file_name} failed:\n{ee}."
                        "Check source files for empty lines or missing reference data"
                    )
                    logging.exception(error_str, stack_info=True)
                    self.mapper.add_to_migration_report(
                        "Failed files", f"{file_name} - {ee}"
                    )
        logging.info(f"processed {total_records:,} records in {len(self.files)} files")
        self.total_records = total_records

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        self.mapper.print_dict_to_md_table(self.mapper.stats)
        p = os.path.join(
            self.results_path,
            "item_transformation_report.md",
        )
        with open(p, "w") as migration_report_file:
            logging.info(f"Writing migration- and mapping report to {p}")
            self.mapper.write_migration_report(migration_report_file)
            self.mapper.print_mapping_report(migration_report_file, self.total_records)
        logging.info("All done!")


def parse_args():
    """Parse CLI Arguments"""
    parser = PromptParser()
    parser.add_argument("records_path", help="path to source records folder", type=str)
    parser.add_argument("result_path", help="path to results folder", type=str)
    parser.add_argument("map_path", help="Path to folder with mapping files", type=str)
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
    args = parser.parse_args()
    return args


def main():
    """Main Method. Used for bootstrapping. """
    csv.register_dialect("tsv", delimiter="\t")
    args = parse_args()
    Worker.setup_logging(
        os.path.join(args.result_path, "item_transformation.log"), args.log_level_debug
    )
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
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
        items_map_path = setup_path(args.map_path, "item_mapping.json")
        with open(items_map_path) as items_mapper_f:
            items_map = json.load(items_mapper_f)
            folio_keys = list(
                k["folio_field"]
                for k in items_map["data"]
                if k["legacy_field"] not in ["", "Not mapped"]
            )        
            logging.info(f'{len(items_map["data"])} fields in item mapping file map')
            mapped_fields = (
                f
                for f in items_map["data"]
                if f["legacy_field"] and f["legacy_field"] != "Not mapped"
            )
            logging.info(
                f"{len(list(mapped_fields))} Mapped fields in item mapping file map"
            )
        
        holdings_id_dict_path = setup_path(args.result_path, "holdings_id_map.json")
       
        # items_map_path = setup_path(args.map_path, "holdings_mapping.json")
        error_file_path = os.path.join(args.result_path, "item_transform_errors.tsv")
        location_map_path = setup_path(args.map_path, "locations.tsv")
        loans_type_map_path = setup_path(args.map_path, "loan_types.tsv")
        
        material_type_map_path = setup_path(args.map_path, "material_types.tsv")

        # Files found, let's go!
        with open(material_type_map_path) as material_type_file:
            material_type_map = list(csv.DictReader(material_type_file, dialect="tsv"))
            logging.info(f"Found {len(material_type_map)} rows in material type map")
            logging.info(
                f'{",".join(material_type_map[0].keys())} will be used for determinig Material type'
            )

        with open(loans_type_map_path) as loans_type_file:
            loan_type_map = list(csv.DictReader(loans_type_file, dialect="tsv"))
            logging.info(f"Found {len(loan_type_map)} rows in loan type map")
            logging.info(
                f'{",".join(loan_type_map[0].keys())} will be used for determinig loan type'
            )

        if "statisticalCodeIds" in folio_keys:
            statcode_map_path = setup_path(
                args.map_path, "statcodes.tsv"
            )
            with open(statcode_map_path) as statcode_map_file:
                statcode_map = list(
                    csv.DictReader(statcode_map_file, dialect="tsv")
                )
                logging.info(
                    f"Found {len(statcode_map)} rows in statistical codes map"
                )
                logging.info(
                    f'{",".join(statcode_map[0].keys())} '
                    "will be used for determinig Statistical codes"
                )
        else:
               	statcode_map = None 
        
        if "itemLevelCallNumberTypeId" in folio_keys:
            call_number_type_map_path = setup_path(
                args.map_path, "call_number_type_mapping.tsv"
            )
            with open(call_number_type_map_path) as call_number_type_map_file:
                call_number_type_map = list(
                    csv.DictReader(call_number_type_map_file, dialect="tsv")
                )
                logging.info(
                    f"Found {len(call_number_type_map)} rows in callnumber type map"
                )
                logging.info(
                    f'{",".join(call_number_type_map[0].keys())} '
                    "will be used for determinig callnumber type"
                )
        else:
            call_number_type_map = None

        with open(holdings_id_dict_path, "r") as holdings_id_map_file, open(location_map_path) as location_map_f, open(
            error_file_path, "w"
        ) as error_file:
            holdings_id_map = json.load(holdings_id_map_file)
            logging.info(f"Loaded {len(holdings_id_map)} holdings ids")
            
            location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
            logging.info(
                f'{",".join(loan_type_map[0].keys())} will be used for determinig location'
            )
            logging.info(f"Found {len(location_map)} rows in location map")

            mapper = ItemMapper(
                folio_client,
                items_map,
                material_type_map,
                loan_type_map,
                location_map,
                call_number_type_map,
                holdings_id_map,
                statcode_map,
                error_file,
            )
            worker = Worker(folio_client, mapper, files, args.result_path, error_file)
            worker.work()
            worker.wrap_up()
    except TransformationProcessError as process_error:
        logging.info("\n=======ERROR===========")
        logging.info(f"{process_error}")
        logging.info("\n=======Stack Trace===========")
        traceback.print_exc()


if __name__ == "__main__":
    main()
