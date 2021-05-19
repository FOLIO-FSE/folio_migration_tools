'''Main "script."'''
import argparse
import csv
import json
import logging
import os
from os import listdir
from os.path import isfile, join

import pymarc
from argparse_prompt import PromptParser
from folioclient.FolioClient import FolioClient
from pymarc.reader import MARCReader

from marc_to_folio.custom_exceptions import TransformationCriticalDataError
from marc_to_folio.holdings_processor import HoldingsProcessor
from marc_to_folio.main_base import MainBase
from marc_to_folio.rules_mapper_holdings import RulesMapperHoldings


def parse_args():
    """Parse CLI Arguments"""
    # parser = argparse.ArgumentParser()
    parser = PromptParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    parser.add_argument("result_folder", help="path to results folder")
    parser.add_argument("map_path", help=("path to mapping files"))
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
    args = parser.parse_args()
    logging.info(f"\tresults are stored at:\t{args.result_folder}")
    logging.info(f"\tOkapi URL:\t{args.okapi_url}")
    logging.info(f"\tTenanti Id:\t{args.tenant_id}")
    logging.info(f"\tUsername:\t{args.username}")
    logging.info(f"\tPassword:\tSecret")
    return args


def main():
    """Main method. Magic starts here."""
    args = parse_args()
    MainBase.setup_logging(
        os.path.join(args.result_folder, "holdings_transformation.log")
    )
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    csv.register_dialect("tsv", delimiter="\t")
    files = [
        os.path.join(args.source_folder, f)
        for f in listdir(args.source_folder)
        if isfile(os.path.join(args.source_folder, f))
    ]
    with open(
        os.path.join(args.result_folder, "instance_id_map.json"), "r"
    ) as json_file, open(
        os.path.join(args.map_path, "locations.tsv")
    ) as location_map_f, open(
        os.path.join(args.map_path, "mfhd_rules.json")
    ) as mapping_rules_file, open(
        os.path.join(args.result_folder, "folio_holdings.json"), "w+"
    ) as results_file:
        instance_id_map = {}
        for index, json_string in enumerate(json_file):
            # {"legacy_id", "folio_id","instanceLevelCallNumber"}
            map_object = json.loads(json_string)
            if index % 50000 == 0:
                print(
                    f"{index} instance ids loaded to map {map_object['legacy_id']}",
                    end="\r",
                )
            instance_id_map[map_object["legacy_id"]] = map_object
        logging.info(f"loaded {index} migrated instance IDs")

        location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
        rules_file = json.load(mapping_rules_file)

        logging.info(f"Locations in map: {len(location_map)}")
        logging.info(any(location_map))
        logging.info(f'Default location code {rules_file["defaultLocationCode"]}')
        logging.info(f"{len(instance_id_map)} Instance ids in map")
        mapper = RulesMapperHoldings(
            folio_client,
            instance_id_map,
            location_map,
            rules_file["defaultLocationCode"],
            args,
        )
        mapper.mappings = rules_file["rules"]

        processor = HoldingsProcessor(mapper, folio_client, results_file, args)
        for records_file in files:
            try:
                with open(records_file, "rb") as marc_file:
                    reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                    reader.hide_utf8_warnings = True
                    reader.force_utf8 = True
                    logging.info(f"Running {records_file}")
                    read_records(reader, processor)
            except Exception:
                logging.exception(f"Failure in Main: {records_file}", stack_info=True)


def read_records(reader, processor: HoldingsProcessor):
    for idx, record in enumerate(reader):
        try:
            if record is None:
                raise TransformationCriticalDataError(
                    f"Index in file:{idx}",
                    f"MARC parsing error: " f"{reader.current_exception}",
                    reader.current_chunk,
                )
            else:
                processor.process_record(record)
        except TransformationCriticalDataError as error:
            logging.error(error)
        except ValueError as error:
            logging.error(error)
    processor.wrap_up()


if __name__ == "__main__":
    main()
