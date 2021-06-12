'''Main "script."'''
import argparse
import ast
import copy
import csv
import ctypes
import json
import logging
from marc_to_folio.folder_structure import FolderStructure
import os
import time
import traceback
import uuid
from os import listdir
from os.path import isfile, join
from typing import List

import pymarc
import requests.exceptions
from argparse_prompt import PromptParser
from folioclient.FolioClient import FolioClient
from requests.api import request

from marc_to_folio.custom_exceptions import (TransformationCriticalDataError,
                                             TransformationProcessError)
from marc_to_folio.helper import Helper
from marc_to_folio.holdings_processor import HoldingsProcessor
from marc_to_folio.main_base import MainBase
from marc_to_folio.mapping_file_transformation.holdings_mapper import \
    HoldingsMapper
from marc_to_folio.mapping_file_transformation.mapper_base import MapperBase
csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


class Worker(MainBase):
    """Class that is responsible for the acutal work"""

    def __init__(
        self,
        folio_client: FolioClient,
        mapper: HoldingsMapper,
        files,
        folder_structure: FolderStructure,
        holdings_merge_criteria,
    ):
        self.holdings = {}
        self.folder_structure = folder_structure
        self.folio_client = folio_client
        self.files = files
        self.legacy_map = {}
        self.holdings_merge_criteria = holdings_merge_criteria
        if "_" in self.holdings_merge_criteria:
            self.excluded_hold_type_id = self.holdings_merge_criteria.split("_")[-1]
            logging.info(self.excluded_hold_type_id)

        self.results_path = self.folder_structure.created_objects_path
        self.mapper = mapper
        self.failed_files: List[str] = list()
        self.num_exeptions = 0
        self.holdings_types = list(
            self.folio_client.folio_get_all("/holdings-types", "holdingsTypes")
        )
        logging.info(f"{len(self.holdings_types)}\tholdings types in tenant")

        self.default_holdings_type = next(
            (h["id"] for h in self.holdings_types if h["name"] == "Unmapped"), ""
        )
        if not self.default_holdings_type:
            raise TransformationProcessError(
                f"Holdings type named Unmapped not found in FOLIO."
            )
        logging.info("Init done")

    def work(self):
        total_records = 0
        logging.info("Starting....")
        for file_name in self.files:
            logging.info(f"Processing {file_name}")
            try:
                with open(
                    file_name,
                    encoding="utf-8-sig",
                ) as records_file:
                    self.mapper.add_to_migration_report(
                        "General statistics", "Number of files processed"
                    )
                    start = time.time()
                    for idx, record in enumerate(
                        self.mapper.get_objects(records_file, file_name)
                    ):
                        try:
                            self.process_holding(idx, record)
                        except TransformationProcessError as process_error:
                            logging.error(f"{idx}\t{process_error}")
                        except TransformationCriticalDataError as error:
                            self.num_exeptions += 1
                            logging.error(error)
                            if self.num_exeptions > 500:
                                logging.fatal(
                                    f"Number of exceptions exceeded limit of "
                                    f"{self.num_exeptions}. Stopping."
                                )
                                exit()
                        except Exception as excepion:
                            self.num_exeptions += 1
                            print("\n=======ERROR===========\n")
                            print("\n=======Stack Trace===========")
                            traceback.print_exc()
                            print("\n============Data========")
                            print(json.dumps(record, indent=4))
                            print("\n=======Message===========")
                            print(
                                f"Row {idx:,} failed with the following Exception: {excepion} "
                                f" of type {type(excepion).__name__}"
                            )
                            exit()

                        self.mapper.add_to_migration_report(
                            "General statistics", "Number of Legacy items in file"
                        )
                        if idx > 1 and idx % 10000 == 0:
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
                logging.exception(error_str)
                self.mapper.add_to_migration_report(
                    "Failed files", f"{file_name} - {ee}"
                )
        logging.info(f"processed {total_records:,} records in {len(self.files)} files")
        self.total_records = total_records

    def process_holding(self, idx, row):
        folio_rec = self.mapper.do_map(row, f"row # {idx}")
        folio_rec["holdingsTypeId"] = self.default_holdings_type
        holdings_from_row = []
        if len(folio_rec["instanceId"]) == 1:  # Normal case.
            folio_rec["instanceId"] = folio_rec["instanceId"][0]
            holdings_from_row.append(folio_rec)
        elif len(folio_rec["instanceId"]) > 1:  # Bound-with.
            holdings_from_row.extend(self.create_bound_with_holdings(folio_rec))
        else:
            logging.critical(f"No instance id at row {idx}")

        for folio_holding in holdings_from_row:
            self.merge_holding_in(folio_holding)

    def create_bound_with_holdings(self, folio_rec):
        # Add former ids
        temp_ids = []
        for former_id in folio_rec.get("formerIds", []):
            if former_id.startswith("[") and former_id.endswith("]") and ',' in former_id:
                ids = former_id[1:-1].replace('"', "").replace(" ", "").replace("'", "").split(",")
                temp_ids.extend(ids)
            else:
                temp_ids.append(former_id)
        folio_rec["formerIds"] = temp_ids

        # Add note
        note = {
            "holdingsNoteTypeId": "e19eabab-a85c-4aef-a7b2-33bd9acef24e",  # Default binding note type
            "note": (
                f'This Record is a Bound-with. It is bound with {len(folio_rec["instanceId"])} '
                "instances. Below is a json structure allowing you to move this into the future "
                "Bound-with functionality in FOLIO\n"
                f'{{"instances": {json.dumps(folio_rec["instanceId"], indent=4)}}}'
            ),
            "staffOnly": True,
        }
        note2 = {
            "holdingsNoteTypeId": "e19eabab-a85c-4aef-a7b2-33bd9acef24e",  # Default binding note type
            "note": (
                f'This Record is a Bound-with. It is bound with {len(folio_rec["instanceId"])} other records. '
                'In order to locate the other records, make a search for the Class mark, but without brackets.'
            ),
            "staffOnly": False,
        }
        if "notes" in folio_rec:
            folio_rec["notes"].append(note)
            folio_rec["notes"].append(note2)
        else:
            folio_rec["notes"] = [note, note2]

        for bwidx, id in enumerate(folio_rec["instanceId"]):
            if not id:
                raise Exception(f"No ID for record {folio_rec}")
            call_numbers = ast.literal_eval(folio_rec["callNumber"])
            if isinstance(call_numbers, str):
                call_numbers = [call_numbers]
            c = copy.deepcopy(folio_rec)
            c["instanceId"] = id
            c["callNumber"] = call_numbers[bwidx]
            c["holdingsTypeId"] = "7b94034e-ac0d-49c9-9417-0631a35d506b"
            c["id"] = str(uuid.uuid4())
            self.mapper.add_to_migration_report(
                "General statistics", "Bound-with holdings created"
            )
            yield c

    def merge_holding_in(self, folio_holding):
        new_holding_key = self.to_key(folio_holding, self.holdings_merge_criteria)
        existing_holding = self.holdings.get(new_holding_key, None)
        exclude = (
            self.holdings_merge_criteria.startswith("u_")
            and folio_holding["holdingsTypeId"] == self.excluded_hold_type_id
        )
        if exclude or not existing_holding:
            self.mapper.add_to_migration_report(
                "General statistics", "Unique Holdings created from Items"
            )
            self.holdings[new_holding_key] = folio_holding
        else:
            self.mapper.add_to_migration_report(
                "General statistics", "Holdings already created from Item"
            )
            self.merge_holding(new_holding_key, existing_holding, folio_holding)

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        if any(self.holdings):
            print(f"Saving holdings created to {self.folder_structure.created_objects_path}")
            with open(self.folder_structure.created_objects_path, "w+") as holdings_file:
                for key, holding in self.holdings.items():
                    for legacy_id in holding["formerIds"]:
                        logging.debug(f"Legacy id:{legacy_id}")

                        # Prevent the first item in a boundwith to be overwritten
                        if legacy_id not in self.legacy_map:
                            self.legacy_map[legacy_id] = {"id": holding["id"]}

                    Helper.write_to_file(holdings_file, holding)
                    self.mapper.add_to_migration_report(
                        "General statistics", "Holdings Records Written to disk"
                    )
            with open(self.folder_structure.holdings_id_map_path, "w") as legacy_map_path_file:
                json.dump(self.legacy_map, legacy_map_path_file)
                logging.info(f"Wrote {len(self.legacy_map)} id:s to legacy map")
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            logging.info(f"Writing migration- and mapping report to {p}")
            self.mapper.write_migration_report(migration_report_file)
            self.mapper.print_mapping_report(migration_report_file, self.total_records)
        logging.info("All done!")

    @staticmethod
    def to_key(holding, fields_criteria):
        """creates a key if key values in holding record
        to determine uniquenes"""
        try:
            """creates a key of key values in holding record
            to determine uniquenes"""
            call_number = (
                "".join(holding.get("callNumber", "").split())
                if "c" in fields_criteria
                else ""
            )
            instance_id = holding["instanceId"] if "b" in fields_criteria else ""
            location_id = (
                holding["permanentLocationId"] if "l" in fields_criteria else ""
            )
            return "-".join([instance_id, call_number, location_id, ""])
        except Exception as ee:
            print(holding)
            raise ee

    def merge_holding(self, key, old_holdings_record, new_holdings_record):
        # TODO: Move to interface or parent class and make more generic
        if self.holdings[key].get("notes", None):
            self.holdings[key]["notes"].extend(new_holdings_record.get("notes", []))
            self.holdings[key]["notes"] = dedupe(self.holdings[key].get("notes", []))
        if self.holdings[key].get("holdingsStatements", None):
            self.holdings[key]["holdingsStatements"].extend(
                new_holdings_record.get("holdingsStatements", [])
            )
            self.holdings[key]["holdingsStatements"] = dedupe(
                self.holdings[key]["holdingsStatements"]
            )
        if self.holdings[key].get("formerIds", None):
            self.holdings[key]["formerIds"].extend(
                new_holdings_record.get("formerIds", [])
            )
            self.holdings[key]["formerIds"] = list(set(self.holdings[key]["formerIds"]))


def parse_args():
    """Parse CLI Arguments"""
    # parser = argparse.ArgumentParser()
    parser = PromptParser()
    parser.add_argument("base_folder", help="Base folder of the client.")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("--password", help="the api users password", secure=True)

    parser.add_argument(
        "--suppress",
        "-ds",
        help="This batch of records are to be suppressed in FOLIO.",
        default=False,
        type=bool,
    )
    flavourhelp = (
        "What criterias do you want to use when merging holdings?\t "
        "All these parameters need to be the same in order to become "
        "the same Holdings record in FOLIO. \n"
        "\tclb\t-\tCallNumber, Location, Bib ID\n"
        "\tlb\t-\tLocation and Bib ID only\n"
        "\tclb_7b94034e-ac0d-49c9-9417-0631a35d506b\t-\t "
        "Exclude bound-with holdings from merging. Requires a "
        "Holdings type in the tenant with this Id"
    )
    parser.add_argument(
        "--holdings_merge_criteria", "-hmc", default="clb", help=flavourhelp
    )
    parser.add_argument(
        "--log_level_debug",
        "-debug",
        help="Set log level to debug",
        default=False,
        type=bool,
    )
    args = parser.parse_args()
    logging.info(f"\tOkapi URL:\t{args.okapi_url}")
    logging.info(f"\tTenanti Id:\t{args.tenant_id}")
    logging.info(f"\tUsername:\t{args.username}")
    logging.info(f"\tPassword:\tSecret")
    return args


def dedupe(list_of_dicts):
    # TODO: Move to interface or parent class
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]


def main():
    """Main Method. Used for bootstrapping. """
    csv.register_dialect("tsv", delimiter="\t")
    args = parse_args()
    folder_structure = FolderStructure(args.base_folder, args.time_stamp)
    folder_structure.setup_migration_file_structure("holdingsrecord", "item")
    Worker.setup_logging(folder_structure, args.log_level_debug)
    folder_structure.log_folder_structure()
    try:
        folio_client = FolioClient(
            args.okapi_url, args.tenant_id, args.username, args.password
        )
    except requests.exceptions.SSLError as sslerror:
        logging.error(f"{sslerror}")
        logging.error("Network Error. Are you connected to the Internet? Do you need VPN? {}")
        exit()


    # Source data files
    files = [
        os.path.join(folder_structure.legacy_records_folder, f)
        for f in listdir(folder_structure.legacy_records_folder)
        if isfile(os.path.join(folder_structure.legacy_records_folder, f))
    ]
    logging.info(f"Files to process:")
    for f in files:
        logging.info(f"\t{f}")

    # All the paths...
    try:
        with open(folder_structure.call_number_type_map_path, "r") as callnumber_type_map_f, open(
            folder_structure.instance_id_map_path, "r"
        ) as instance_id_map_file, open(folder_structure.holdings_id_map_path) as holdings_mapper_f, open(
            folder_structure.locations_map_path
        ) as location_map_f:
            instance_id_map = {}
            for index, json_string in enumerate(instance_id_map_file):
                # {"legacy_id", "folio_id","instanceLevelCallNumber"}
                if index % 100000 == 0:
                    print(f"{index} instance ids loaded to map", end="\r")
                map_object = json.loads(json_string)
                instance_id_map[map_object["legacy_id"]] = map_object
            logging.info(f"Loaded {index} migrated instance IDs")
            holdings_map = json.load(holdings_mapper_f)
            logging.info(
                f'{len(holdings_map["data"])} fields in holdings mapping file map'
            )
            mapped_fields = MapperBase.get_mapped_folio_properties_from_map(
                holdings_map
            )
            logging.info(
                f"{len(list(mapped_fields))} Mapped fields in holdings mapping file map"
            )
            location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
            call_number_type_map = list(
                csv.DictReader(callnumber_type_map_f, dialect="tsv")
            )
            logging.info(f"Found {len(location_map)} rows in location map")

            mapper = HoldingsMapper(
                folio_client,
                holdings_map,
                location_map,
                call_number_type_map,
                instance_id_map,
            )
            worker = Worker(
                folio_client,
                mapper,
                files,
                folder_structure,
                args.holdings_merge_criteria,
            )
            worker.work()
            worker.wrap_up()
    except TransformationProcessError as process_error:
        logging.info("\n=======ERROR===========")
        logging.info(f"{process_error}")
        logging.info("\n=======Stack Trace===========")
        traceback.print_exc()


if __name__ == "__main__":
    main()
