'''Main "script."'''
import argparse
from marc_to_folio.rules_mapper_holdings import RulesMapperHoldings
import os
import csv
import logging
import json
from os import listdir
from os.path import isfile, join
import pymarc
from folioclient.FolioClient import FolioClient
from marc_to_folio.holdings_processor import HoldingsProcessor


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    parser.add_argument("result_folder", help="path to results folder")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("ils_flavour", help=("The ILS migrating from"))

    parser.add_argument(
        "-postgres_dump",
        "-p",
        help=("results will be written out for Postgres" "ingestion. Default is JSON"),
        action="store_true",
    )
    parser.add_argument("-map_path", "-m", help=("path to mapping files"))
    parser.add_argument(
        "-marcxml", "-x", help=("DATA is in MARCXML format"), action="store_true"
    )
    parser.add_argument(
        "-suppress",
        "-ds",
        help=("This batch of records are to be suppressed in FOLIO."),
        action="store_true",
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
    logging.basicConfig(
        filename=os.path.join(args.result_folder, "holdings_transform_log.log"),
        filemode="w",
    )
    log = logging.getLogger()
    log.setLevel(logging.CRITICAL)
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
            if index % 100000 == 0:
                print(f"{index} instance ids loaded to map", end='\r')            
            map_object = json.loads(json_string)
            instance_id_map[map_object["legacy_id"]] = map_object
        print(f"loaded {index} migrated instance IDs")
        
        location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
        rules_file = json.load(mapping_rules_file)

        print(f"Locations in map: {len(location_map)}")
        print(any(location_map))
        print(f'Default location code {rules_file["defaultLocationCode"]}')
        print(f"{len(instance_id_map)} Instance ids in map")
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
            if args.marcxml:
                pymarc.map_xml(processor.process_record, records_file)
            else:
                with open(records_file, "rb") as marc_file:
                    pymarc.map_records(processor.process_record, marc_file)

    processor.wrap_up()


if __name__ == "__main__":
    main()
