'''Main "script."'''
import ast
import copy
import csv
import ctypes
import json
import logging
import os
import sys
import time
import traceback
import uuid
from os import listdir
from os.path import isfile
from typing import List

import requests.exceptions
from argparse_prompt import PromptParser
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient.FolioClient import FolioClient

from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.main_base import MainBase
from migration_tools.mapping_file_transformation.holdings_mapper import HoldingsMapper
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.report_blurbs import Blurbs

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
        super().__init__()
        self.holdings = {}
        self.total_records = 0
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
        self.holdings_types = list(
            self.folio_client.folio_get_all("/holdings-types", "holdingsTypes")
        )
        logging.info("%s\tholdings types in tenant", len(self.holdings_types))

        self.default_holdings_type = next(
            (h["id"] for h in self.holdings_types if h["name"] == "Unmapped"), ""
        )
        if not self.default_holdings_type:
            raise TransformationProcessError(
                "Holdings type named Unmapped not found in FOLIO."
            )

        logging.info("Init done")

    def work(self):
        logging.info("Starting....")
        for file_name in self.files:
            logging.info("Processing %s", file_name)
            try:
                self.process_single_file(file_name)
            except Exception as ee:
                error_str = (
                    f"Processing of {file_name} failed:\n{ee}."
                    "Check source files for empty lines or missing reference data"
                )
                logging.exception(error_str)
                self.mapper.migration_report.add(
                    Blurbs.FailedFiles, f"{file_name} - {ee}"
                )
                sys.exit()
        logging.info(  # pylint: disable=logging-fstring-interpolation
            f"processed {self.total_records:,} records in {len(self.files)} files"
        )

    def process_single_file(self, file_name):
        with open(file_name, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                "Number of files processed"
            )
            start = time.time()
            for idx, record in enumerate(
                self.mapper.get_objects(records_file, file_name)
            ):
                try:
                    self.process_holding(idx, record)

                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as error:
                    self.mapper.handle_transformation_record_failed_error(idx, error)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)
                self.mapper.migration_report.add_general_statistics(
                    "Number of rows in legacy data file file(s)"
                )
                if idx > 1 and idx % 10000 == 0:
                    elapsed = idx / (time.time() - start)
                    elapsed_formatted = "{0:.4g}".format(elapsed)
                    logging.info(  # pylint: disable=logging-fstring-interpolation
                        f"{idx:,} records processed. Recs/sec: {elapsed_formatted} "
                    )
            self.total_records += idx
            logging.info(  # pylint: disable=logging-fstring-interpolation
                f"Done processing {file_name} containing {idx:,} records. "
                f"Total records processed: {self.total_records:,}"
            )

    def process_holding(self, idx, row):
        folio_rec, legacy_id = self.mapper.do_map(
            row, f"row # {idx}", FOLIONamespaces.holdings
        )
        folio_rec["holdingsTypeId"] = self.default_holdings_type
        holdings_from_row = []
        if len(folio_rec.get("instanceId", [])) == 1:  # Normal case.
            folio_rec["instanceId"] = folio_rec["instanceId"][0]
            holdings_from_row.append(folio_rec)
        elif len(folio_rec.get("instanceId", [])) > 1:  # Bound-with.
            holdings_from_row.extend(self.create_bound_with_holdings(folio_rec))
        else:
            raise TransformationRecordFailedError(
                legacy_id, "No instance id in parsed record", ""
            )
        for folio_holding in holdings_from_row:
            self.merge_holding_in(folio_holding)
        self.mapper.report_folio_mapping(folio_holding, self.mapper.schema)

    def create_bound_with_holdings(self, folio_rec):
        # Add former ids
        temp_ids = []
        for former_id in folio_rec.get("formerIds", []):
            if (
                former_id.startswith("[")
                and former_id.endswith("]")
                and "," in former_id
            ):
                ids = (
                    former_id[1:-1]
                    .replace('"', "")
                    .replace(" ", "")
                    .replace("'", "")
                    .split(",")
                )
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
                "In order to locate the other records, make a search for the Class mark, but without brackets."
            ),
            "staffOnly": False,
        }
        if "notes" in folio_rec:
            folio_rec["notes"].append(note)
            folio_rec["notes"].append(note2)
        else:
            folio_rec["notes"] = [note, note2]

        for bwidx, index in enumerate(folio_rec["instanceId"]):
            if not index:
                raise ValueError(f"No ID for record {folio_rec}")
            call_numbers = ast.literal_eval(folio_rec["callNumber"])
            if isinstance(call_numbers, str):
                call_numbers = [call_numbers]
            c = copy.deepcopy(folio_rec)
            c["instanceId"] = index
            c["callNumber"] = call_numbers[bwidx]
            c["holdingsTypeId"] = "7b94034e-ac0d-49c9-9417-0631a35d506b"
            # TODO: Make these UUIDs deterministic as well when moving to the
            # new FOLIO BW model
            c["id"] = str(uuid.uuid4())
            self.mapper.migration_report.add_general_statistics(
                "Bound-with holdings created"
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
            self.mapper.migration_report.add_general_statistics(
                "Unique Holdings created from Items"
            )
            self.holdings[new_holding_key] = folio_holding
        else:
            self.mapper.migration_report.add_general_statistics(
                "Holdings already created from rows in file(s)"
            )
            self.merge_holding(new_holding_key, existing_holding, folio_holding)

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        if any(self.holdings):
            logging.info(
                "Saving holdings created to %s",
                self.folder_structure.created_objects_path,
            )
            with open(
                self.folder_structure.created_objects_path, "w+"
            ) as holdings_file:
                for holding in self.holdings.values():
                    for legacy_id in holding["formerIds"]:
                        # Prevent the first item in a boundwith to be overwritten
                        if legacy_id not in self.legacy_map:
                            self.legacy_map[legacy_id] = {"id": holding["id"]}

                    Helper.write_to_file(holdings_file, holding)
                    self.mapper.migration_report.add_general_statistics(
                        "Holdings Records written to disk"
                    )
            with open(
                self.folder_structure.holdings_id_map_path, "w"
            ) as legacy_map_path_file:
                json.dump(self.legacy_map, legacy_map_path_file)
                logging.info("Wrote %s id:s to legacy map", len(self.legacy_map))
        with open(
            self.folder_structure.migration_reports_file, "w"
        ) as migration_report_file:
            logging.info(
                "Writing migration- and mapping report to %s",
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
        except Exception as exception:
            logging.error(json.dumps(holding, indent=4))
            raise exception

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
    parser = PromptParser()
    parser.add_argument("base_folder", help="Base folder of the client.")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("--password", help="the api users password", secure=True)

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
    parser.add_argument(
        "--time_stamp",
        "-ts",
        help="Time Stamp String (YYYYMMDD-HHMMSS) from Instance transformation. Required",
    )
    args = parser.parse_args()
    logging.info("Okapi URL:\t%s", args.okapi_url)
    logging.info("Tenant Id:\t%s", args.tenant_id)
    logging.info("Username:   \t%s", args.username)
    logging.info("Password:   \tSecret")
    return args


def dedupe(list_of_dicts):
    # TODO: Move to interface or parent class
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]


def main():
    """Main Method. Used for bootstrapping."""
    csv.register_dialect("tsv", delimiter="\t")
    args = parse_args()
    folder_structure = FolderStructure(
        args.base_folder, args.time_stamp  # pylint: disable=no-member
    )  # pylint: disable=no-member
    folder_structure.setup_migration_file_structure("holdingsrecord", "item")
    folder_structure.log_folder_structure()
    try:
        folio_client = FolioClient(
            args.okapi_url, args.tenant_id, args.username, args.password
        )
    except requests.exceptions.SSLError as sslerror:
        logging.error(sslerror)
        logging.error(
            "Network Error. Are you connected to the Internet? Do you need VPN? {}"
        )
        sys.exit()

    # Source data files
    files = [
        os.path.join(folder_structure.legacy_records_folder, f)
        for f in listdir(folder_structure.legacy_records_folder)
        if isfile(os.path.join(folder_structure.legacy_records_folder, f))
    ]
    logging.info("Files to process:")
    for filename in files:
        logging.info("\t%s", filename)

    # All the paths...
    try:
        with open(
            folder_structure.call_number_type_map_path, "r"
        ) as callnumber_type_map_f, open(
            folder_structure.instance_id_map_path, "r"
        ) as instance_id_map_file, open(
            folder_structure.holdings_map_path
        ) as holdings_mapper_f, open(
            folder_structure.locations_map_path
        ) as location_map_f:
            instance_id_map = MainBase.load_instance_id_map(instance_id_map_file)
            holdings_map = json.load(holdings_mapper_f)
            logging.info(
                "%s fields in holdings mapping file map", len(holdings_map["data"])
            )
            mapped_fields = MappingFileMapperBase.get_mapped_folio_properties_from_map(
                holdings_map
            )
            logging.info(
                "%s mapped fields in holdings mapping file map",
                len(list(mapped_fields)),
            )
            location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
            call_number_type_map = list(
                csv.DictReader(callnumber_type_map_f, dialect="tsv")
            )
            logging.info("Found %s rows in location map", len(location_map))

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
        logging.critical(process_error)
        logging.critical("Halting.")
        sys.exit()
    except Exception as exception:
        logging.info("\n=======ERROR===========")
        logging.info(exception)
        logging.info("\n=======Stack Trace===========")
        traceback.print_exc()


if __name__ == "__main__":
    main()
