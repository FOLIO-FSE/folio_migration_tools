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

    def __init__(self, folio_client, results_file, migration_report_file, args):
        # msu special case
        self.args = args
        self.migration_report_file = migration_report_file
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
            self.mapper.add_stats(
                self.mapper.stats, "MARC21 records in file before parsing"
            )
            if record is None:
                self.mapper.add_to_migration_report(
                    "Bib records that failed to parse. -",
                    f"{reader.current_exception} {reader.current_chunk}",
                )
                self.mapper.add_stats(
                    self.mapper.stats,
                    "MARC21 Records with encoding errors - parsing failed",
                )
            else:
                self.mapper.add_stats(
                    self.mapper.stats, "MARC21 Records successfully parsed"
                )
                self.processor.process_record(record, False)

    def wrap_up(self):
        print("Done. Wrapping up...")
        self.processor.wrap_up()
        with open(self.migration_report_file, "w+") as report_file:
            report_file.write(f"# Bibliographic records transformation results   \n")

            report_file.write(f"Time Run: {dt.isoformat(dt.utcnow())}   \n")
            report_file.write(f"## Bibliographic records transformation counters   \n")
            self.mapper.print_dict_to_md_table(
                self.mapper.stats, report_file, "  Measure  ", "Count   \n",
            )
            self.mapper.write_migration_report(report_file)
            self.mapper.print_mapping_report(report_file)
        print(f"Done. Transformation report written to {self.migration_report_file}")


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
        "-suppress",
        "-ds",
        help=("This batch of records are to be suppressed in FOLIO."),
        action="store_true",
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
    print("Bootstrapping", flush=True)
    args = parse_args()

    logging.basicConfig(level=logging.CRITICAL)

    results_file = join(args.results_folder, "folio_instances.json")
    migration_report_file = join(
        args.results_folder, "instance_transformation_report.md"
    )
    print("\tresults will be saved at:\t", args.results_folder)
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    # Iniiate Worker
    worker = Worker(folio_client, results_file, migration_report_file, args)
    worker.work()


def get_subfield_contents(record, marc_tag, subfield_code):
    fields = record.get_fields(marc_tag)
    res = []
    for f in fields:
        for sf in f.get_subfields(subfield_code):
            res.append(sf)
    return res


if __name__ == "__main__":
    main()
