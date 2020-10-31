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
from typing import Dict, List


class Worker:
    """Class that is responsible for the acutal work"""

    def __init__(
        self,
        folio_client: FolioClient,
        results_file,
        processor: ItemsProcessor,
        file_names: List[str],
    ):
        self.processor = processor
        self.stats: Dict[str, int] = {}
        self.migration_report: Dict[str, List[str]] = {}
        self.failed_files: List[str] = list()
        self.file_names = file_names
        print("Init done")

    def work(self):
        print("Starting....")
        i = 0
        for file_name in self.file_names:
            print(f"Processing {file_name}")
            try:
                with open(file_name, encoding="utf-8-sig") as records_file:
                    add_stats(self.stats, "Number of files processed")
                    f = 0
                    for rec in self.processor.mapper.get_records(records_file):
                        i += 1
                        add_stats(self.stats, "Number of Legacy items in file")
                        f += 1
                        self.processor.process_record(rec)
                    print(f"Done processing {file_name} containing {f} records")
            except Exception as ee:
                print(f"processing of {file_name} failed: {ee}")
            # print_dict_to_md_table(self.processor.mapper.stats)

        print(f"processed {i} records {len(self.file_names)} files")

    def wrap_up(self):
        print("Done. Wrapping up...")
        print_dict_to_md_table(self.stats)
        self.processor.wrap_up()
        self.write_migration_report(self.processor.migration_report)
        print("done")

    def write_migration_report(self, other_report=None):
        if other_report:
            for a in other_report:
                print(f"## {a} - {len(other_report[a])} things")
                for b in other_report[a]:
                    print(f"{b}\\")
        else:
            for a in self.migration_report:
                print(f"## {a} - {len(self.migration_report[a])} things")
                for b in self.migration_report[a]:
                    print(f"{b}\\")

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("records_path", help="path to items file")
    parser.add_argument("result_path", help="path to Instance results file")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("-map_path", "-it", help=(""))
    parser.add_argument(
        "-loan_type_from_mat_type",
        "-l",
        help="Map loan type to material type field",
        action="store_true",
    )
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

    return args


def main():
    """Main Method. Used for bootstrapping. """
    csv.register_dialect("tsv", delimiter="\t")
    args = parse_args()

    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    files = [
        join(args.records_path, f)
        for f in listdir(args.records_path)
        if isfile(join(args.records_path, f))
    ]
    item_type_map = None
    material_type_map = None
    loan_type_map = None
    print(f"Files to process: {files}")
    holdings_id_dict_path = os.path.join(args.result_path, "holdings_id_map.json")
    items_map_path = os.path.join(args.map_path, "item_to_item.json")
    location_map_path = os.path.join(args.map_path, "locations.tsv")
    items_type_map_path = os.path.join(args.map_path, "item_types.tsv")
    loans_type_map_path = os.path.join(args.map_path, "loan_types.tsv")
    material_type_map_path = os.path.join(args.map_path, "material_types.tsv")
    # Item Type map trumps the others. That has mappings to both LT and MT
    if isfile(items_type_map_path):
        print("Item type map found. leaning on this file for mapping")
        with open(items_type_map_path) as item_types_file:
            item_type_map = list(csv.DictReader(item_types_file, dialect="tsv"))
    elif isfile(loans_type_map_path) and isfile(material_type_map_path):
        print(
            "Material type mapping- and Loan type mapping files found. Relying on these for mapping"
        )
        with open(material_type_map_path) as material_type_file:
            material_type_map = list(csv.DictReader(material_type_file, dialect="tsv"))
            print(f"Found {len(material_type_map)} rows in material type map")
        with open(loans_type_map_path) as loans_type_file:
            loan_type_map = list(csv.DictReader(loans_type_file, dialect="tsv"))
            print(f"Found {len(loan_type_map)} rows in loan type map")
            print(
                f'{",".join(loan_type_map[0].keys())} will be used for determinig loan type'
            )
    else:
        raise Exception(
            "Not enough mapping files present for mapping to be performed. Check documentation"
        )

    with open(holdings_id_dict_path, "r") as holdings_id_map_file, open(
        items_map_path
    ) as items_mapper_f, open(location_map_path) as location_map_f, open(
        os.path.join(args.result_path, "folio_items.json"), "w+"
    ) as results_f:
        holdings_id_map = json.load(holdings_id_map_file)
        items_map = json.load(items_mapper_f)
        print(f'{len(items_map["fields"])} fields in item to item map')
        location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
        print(f"Found {len(location_map)} rows in location map")
        mapper = ItemsDefaultMapper(
            folio_client,
            items_map,
            holdings_id_map,
            location_map,
            [item_type_map, material_type_map, loan_type_map],
            args,
        )
        processor = ItemsProcessor(mapper, folio_client, results_f, args)
        worker = Worker(folio_client, results_f, processor, files)
        worker.work()
        worker.wrap_up()


def add_stats(stats, a):
    if a not in stats:
        stats[a] = 1
    else:
        stats[a] += 1


def print_dict_to_md_table(my_dict, h1="Measure", h2="Number"):
    # TODO: Move to interface or parent class
    d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
    print(f"{h1} | {h2}")
    print("--- | ---:")
    for k, v in d_sorted.items():
        print(f"{k} | {v:,}")


if __name__ == "__main__":
    main()
