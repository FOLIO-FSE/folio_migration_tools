'''Main "script."'''
import argparse
import csv
import ctypes
import json
import logging

from argparse_prompt import PromptParser
from marc_to_folio.helper import Helper
import os
import time
from os import listdir
from os.path import isfile, join
import traceback
from typing import List

import pymarc
from folioclient.FolioClient import FolioClient

from marc_to_folio.custom_exceptions import TransformationProcessError
from marc_to_folio.holdings_processor import HoldingsProcessor
from marc_to_folio.main_base import MainBase
from marc_to_folio.mapping_file_transformation.holdings_mapper import HoldingsMapper
from marc_to_folio.rules_mapper_holdings import RulesMapperHoldings

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


class Worker(MainBase):
    """Class that is responsible for the acutal work"""

    def __init__(
        self,
        folio_client: FolioClient,
        mapper: HoldingsMapper,
        files,
        results_path,
        error_file,
    ):
        self.holdings = {}
        self.folio_client = folio_client
        self.files = files
        self.legacy_map = {}
        self.results_path = results_path
        self.mapper = mapper
        self.failed_files: List[str] = list()
        self.num_exeptions = 0
        self.error_file = error_file
        logging.info("Init done")
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
                    self.mapper.add_stats("Number of files processed")
                    start = time.time()
                    for idx, record in enumerate(
                        self.mapper.get_objects(records_file, file_name)
                    ):
                        try:
                            folio_rec = self.mapper.do_map(record, f"row {idx}")
                            folio_rec["holdingsTypeId"] = self.default_holdings_type
                            holding_key = self.to_key(folio_rec)
                            existing_holding = self.holdings.get(holding_key, None)
                            if not existing_holding:
                                self.mapper.add_stats(
                                    "Unique Holdings created from Items"
                                )
                                self.holdings[self.to_key(folio_rec)] = folio_rec
                            else:
                                self.mapper.add_stats(
                                    "Holdings already created from Item"
                                )
                                self.merge_holding(folio_rec)
                        except TransformationProcessError as process_error:
                            logging.error(f"{idx}\t{process_error}")
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
                        self.mapper.add_stats("Number of Legacy items in file")
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
                logging.exception(error_str)
                self.mapper.add_to_migration_report(
                    "Failed files", f"{file_name} - {ee}"
                )
        logging.info(f"processed {total_records:,} records in {len(self.files)} files")
        self.total_records = total_records

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        if any(self.holdings):
            results_path = os.path.join(self.results_path, "folio_holdings.json")
            print(f"Saving holdings created to {results_path}")
            with open(results_path, "w+") as holdings_file:
                for key, holding in self.holdings.items():
                    for legacy_id in holding["formerIds"]:
                        self.legacy_map[legacy_id] = {"id": holding["id"]}
                    Helper.write_to_file(holdings_file, holding)
                    self.mapper.add_stats("Holdings Records Written to disk")
            legacy_path = os.path.join(self.results_path, "holdings_id_map.json")
            with open(legacy_path, "w") as legacy_map_path_file:
                json.dump(self.legacy_map, legacy_map_path_file)
                logging.info(f"Wrote {len(self.legacy_map)} id:s to legacy map")
        self.mapper.print_dict_to_md_table(self.mapper.stats)
        p = os.path.join(
            self.results_path,
            "holdings_transformation_report.md",
        )
        with open(p, "w") as migration_report_file:
            logging.info(f"Writing migration- and mapping report to {p}")
            self.mapper.write_migration_report(migration_report_file)
            self.mapper.print_mapping_report(migration_report_file, self.total_records)
        logging.info("All done!")

    @staticmethod
    def to_key(holding):
        """creates a key if key values in holding record
        to determine uniquenes"""
        try:
            """creates a key of key values in holding record
            to determine uniquenes"""
            call_number = (
                "".join(holding["callNumber"].split())
                if "callNumber" in holding
                else ""
            )
            return "-".join(
                [holding["instanceId"], call_number, holding["permanentLocationId"], ""]
            )
        except Exception as ee:
            print(holding)
            raise ee

    def merge_holding(self, holdings_record):
        # TODO: Move to interface or parent class
        key = self.to_key(holdings_record)
        if self.holdings[key].get("notes", None):
            self.holdings[key]["notes"].extend(holdings_record.get("notes", []))
            self.holdings[key]["notes"] = dedupe(self.holdings[key].get("notes", []))
        if self.holdings[key].get("holdingsStatements", None):
            self.holdings[key]["holdingsStatements"].extend(
                holdings_record.get("holdingsStatements", [])
            )
            self.holdings[key]["holdingsStatements"] = dedupe(
                self.holdings[key]["holdingsStatements"]
            )
        if self.holdings[key].get("formerIds", None):
            self.holdings[key]["formerIds"].extend(holdings_record.get("formerIds", []))


def parse_args():
    """Parse CLI Arguments"""
    # parser = argparse.ArgumentParser()
    parser = PromptParser()
    parser.add_argument("records_path", help="path to legacy item records folder")
    parser.add_argument("result_path", help="path to results folder")
    parser.add_argument("map_path", help=("path to mapping files"))
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
    args = parser.parse_args()
    logging.info(f"\tresults are stored at:\t{args.result_path}")
    logging.info(f"\tOkapi URL:\t{args.okapi_url}")
    logging.info(f"\tTenanti Id:\t{args.tenant_id}")
    logging.info(f"\tUsername:\t{args.username}")
    logging.info(f"\tPassword:\tSecret")
    return args


def setup_path(path, filename):
    new_path = ""
    try:
        new_path = os.path.join(path, filename)
    except:
        raise Exception(
            f"Something went wrong when joining {path} and {filename} into a path"
        )
    if not isfile(new_path):
        raise Exception(f"No file called {filename} present in {path}")
    return new_path


def dedupe(list_of_dicts):
    # TODO: Move to interface or parent class
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]


def main():
    """Main Method. Used for bootstrapping. """
    csv.register_dialect("tsv", delimiter="\t")
    args = parse_args()
    Worker.setup_logging(os.path.join(args.result_path, "holdings_transformation.log"))
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
        instance_id_dict_path = setup_path(args.result_path, "instance_id_map.json")
        holdings_map_path = setup_path(args.map_path, "holdingsrecord_mapping.json")
        error_file_path = os.path.join(
            args.result_path, "holdings_transform_errors.tsv"
        )
        location_map_path = setup_path(args.map_path, "locations.tsv")
        call_number_type_map_path = setup_path(
            args.map_path, "call_number_type_mapping.tsv"
        )
        # Files found, let's go!

        with open(call_number_type_map_path, "r") as callnumber_type_map_f, open(
            instance_id_dict_path, "r"
        ) as instance_id_map_file, open(holdings_map_path) as holdings_mapper_f, open(
            location_map_path
        ) as location_map_f, open(
            error_file_path, "w"
        ) as error_file:
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
            mapped_fields = (
                f
                for f in holdings_map["data"]
                if f["legacy_field"] and f["legacy_field"] != "Not mapped"
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
