'''Main "script."'''
import argparse
import json
import logging
import csv
import copy
import sys
import traceback
from os import listdir
from os.path import isfile, join
from datetime import datetime as dt
import time

from folioclient.FolioClient import FolioClient
from pymarc import MARCReader
from marc_to_folio import BibsRulesMapper

from marc_to_folio.bibs_processor import BibsProcessor


class Worker:
    """Class that is responsible for the acutal work"""

    def __init__(self, folio_client, results_file, args):
        # msu special case
        self.args = args
        self.stats = {}
        self.migration_report = {}
        self.results_file_path = results_file

        self.files = [
            f
            for f in listdir(args.source_folder)
            if isfile(join(args.source_folder, f))
        ]
        self.folio_client = folio_client
        print(f"Files to process: {len(self.files)}")
        print(json.dumps(self.files, sort_keys=True, indent=4))
        self.mapper = BibsRulesMapper(self.folio_client, args)
        self.processor = None
        self.failed_files = list()
        self.bibids = set()
        print("Init done")

    def work(self):
        print("Starting....")
        with open(self.results_file_path, "w+") as results_file:
            self.processor = BibsProcessor(
                self.mapper, self.folio_client, results_file, self.args,
            )
            self.start = time.time()
            for file_name in self.files:
                try:
                    with open(join(sys.argv[1], file_name), "rb") as marc_file:
                        reader = MARCReader(marc_file, "rb", permissive=True)
                        reader.hide_utf8_warnings = True
                        if self.args.force_utf_8:
                            print("FORCE UTF-8 is set to TRUE")
                            reader.force_utf8 = True
                        print(f"running {file_name}")
                        self.read_records(reader)
                except Exception as exception:
                    print(exception)
                    traceback.print_exc()
                    print(file_name)
            # wrap up
            self.wrap_up()

    def read_records(self, reader):
        for record in reader:
            add_stats(self.stats, "MARC21 records in file before parsing")
            if record is None:
                self.add_to_migration_report(
                    "Bib records that failed to parse. -",
                    f"{reader.current_exception} {reader.current_chunk}",
                )
                add_stats(
                    self.stats, "MARC21 Records with encoding errors - parsing failed"
                )
            else:
                add_stats(self.stats, "MARC21 Records successfully parsed")
                self.processor.process_record(record, False)
            add_stats(self.stats, "Bibs processed")
            self.print_progress()

    def print_progress(self):
        i = self.stats["Bibs processed"]
        if i % 1000 == 0:
            elapsed = i / (time.time() - self.start)
            elapsed_formatted = int(elapsed)
            print(
                f"{elapsed_formatted}\t{i}", flush=True,
            )

    def wrap_up(self):
        print("Done. Wrapping up...")
        self.processor.wrap_up()
        print("Failed files:")
        self.stats = {**self.stats, **self.mapper.stats, **self.processor.stats}

        print("# Bibliographic records migration")
        print(f"Time Run: {dt.isoformat(dt.now())}")
        print("## Bibliographic records migration counters")
        print_dict_to_md_table(self.stats, "    ", "Count")
        print("## Unmapped MARC tags")
        print_dict_to_md_table(
            self.mapper.unmapped_tags, "Tag", "Count",
        )
        print("## Mapped FOLIO fields")
        print_dict_to_md_table(
            self.mapper.mapped_folio_fields, "Tag", "Count",
        )
        print("## Unmapped FOLIO fields")
        print_dict_to_md_table(
            self.mapper.unmapped_folio_fields, "Tag", "Count",
        )
        print("## Unmapped conditions in rules")
        print_dict_to_md_table(self.mapper.unmapped_conditions)

        self.write_migration_report(self.mapper.migration_report)
        self.write_migration_report(self.processor.migration_report)
        self.write_migration_report()
        print("done")

    def add_to_migration_report(self, header, messageString):
        # TODO: Move to interface or parent class
        if header not in self.migration_report:
            self.migration_report[header] = list()
        self.migration_report[header].append(messageString)

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


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    parser.add_argument("results_folder", help="path to Instance results folder")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument(
        "ils_flavour", help="The kind of ILS the records are created in"
    )
    parser.add_argument(
        "-holdings_records",
        "-hold",
        help="Create holdings records based on relevant MARC fields",
        action="store_true",
    )
    parser.add_argument(
        "-force_utf_8",
        "-utf8",
        help=("forcing UTF8 when pasing marc records"),
        action="store_true",
    )
    parser.add_argument(
        "-msu_locations_path", "-f", help=("filter records based on MSU rules")
    )
    parser.add_argument(
        "-postgres_dump",
        "-p",
        help=("results will be written out for Postgres" "ingestion. Default is JSON"),
        action="store_true",
    )
    parser.add_argument(
        "-marcxml", "-x", help=("DATA is in MARCXML format"), action="store_true"
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
    # Parse CLI Arguments
    args = parse_args()

    logging.basicConfig(level=logging.CRITICAL)

    results_file = join(args.results_folder, "folio_instances.json")
    print("\tresults will be saved at:\t", args.results_folder)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    # Iniiate Worker
    worker = Worker(folio_client, results_file, args)
    worker.work()


def get_subfield_contents(record, marc_tag, subfield_code):
    fields = record.get_fields(marc_tag)
    res = []
    for f in fields:
        for sf in f.get_subfields(subfield_code):
            res.append(sf)
    return res


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
