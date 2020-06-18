'''Main "script."'''
import argparse
import csv

import traceback
import json
import logging
import os
import pymarc
from os import listdir
from os.path import isfile, join
from folioclient.FolioClient import FolioClient
from marc_to_folio.items_default_mapper import ItemsDefaultMapper
from marc_to_folio.items_processor import ItemsProcessor


class Worker:
    """Class that is responsible for the acutal work"""

    def __init__(self, folio_client, results_file, processor, file_name):
        print("Init done")
        self.processor = processor
        self.failed_files = list()
        self.file_names = file_names

    def work(self):
        print("Starting....")
        i = 0
        for file_name in self.file_names:
            f = 0
            for rec in self.processor.mapper.get_records(file_name):
                i += 1
                f += 1
                processor.process_record(rec)
                if i % 1000 == 0:
                    print(i)
            print(f"Done processing {file_name} containing {f} records")

    def wrap_up(self):
        print("Done. Wrapping up...")
        self.processor.wrap_up()
        print("Failed files:")
        print(json.dumps(self.failed_files, sort_keys=True, indent=4))
        print("done")
        print(f"processt {i} records in {f} files")


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("records_file", help="path to items file")
    parser.add_argument("result_path", help="path to Instance results file")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("-holdings_id_dict_path", "-ih", help=(""))
    parser.add_argument("-location_map_path", "-l", help=("path of location map"))
    parser.add_argument("-items_mapper_file", "-it", help=(""))
    parser.add_argument(
        "-postgres_dump",
        "-p",
        help=("results will be written out for Postgres" "ingestion. Default is JSON"),
        action="store_true",
    )
    parser.add_argument(
        "-validate",
        "-v",
        help=("Validate JSON data against JSON Schema"),
        action="store_true",
    )
    args = parser.parse_args()
    print(f"File to process: {args.records_file}")

    return args


def main():
    """Main Method. Used for bootstrapping. """
    csv.register_dialect("tsv", delimiter="\t")
    args = parse_args()

    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    files = [
        join(args.records_file, f)
        for f in listdir(args.records_file)
        if isfile(join(args.records_file, f))
    ]
    print(f"File to process: {args.records_file}")

    with open(args.holdings_id_dict_path, "r") as json_file, open(
        args.items_mapper_file
    ) as items_mapper_file, open(args.location_map_path) as location_map_f, open(
        os.path.join(args.result_path, "folio_items.json"), "w+"
    ) as results_file:
        holdings_id_map = json.load(json_file)
        items_map = json.load(items_mapper_file)
        location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
        mapper = ItemsDefaultMapper(
            folio_client, items_map, holdings_id_map, location_map
        )
        processor = ItemsProcessor(mapper, folio_client, results_file, args)
        worker = Worker(folio_client, processor, items_files)
        worker.work()
        worker.wrap_up()


if __name__ == "__main__":
    main()
