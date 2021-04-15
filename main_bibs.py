'''Main "script."'''
import argparse
import json
import logging
import csv
import copy
from marc_to_folio.custom_exceptions import TransformationCriticalDataError
import sys
import traceback
from os import listdir
from os.path import isfile, join, dirname
from datetime import datetime as dt
import time

from folioclient.FolioClient import FolioClient
from pymarc import MARCReader
from pymarc.record import Record
from marc_to_folio import BibsRulesMapper, main_base

from marc_to_folio.bibs_processor import BibsProcessor


class Worker(main_base.MainBase):
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
        logging.info(f"Files to process: {len(self.files)}")
        logging.info(json.dumps(self.files, sort_keys=True, indent=4))
        self.mapper = BibsRulesMapper(self.folio_client, args)
        self.processor = None
        self.failed_files = list()
        self.bib_ids = set()
        logging.info("Init done")

    def work(self):
        logging.info("Starting....")
        with open(self.results_file_path, "w+") as results_file:
            self.processor = BibsProcessor(
                self.mapper,
                self.folio_client,
                results_file,
                self.args,
            )
            for file_name in self.files:
                try:
                    with open(join(sys.argv[1], file_name), "rb") as marc_file:
                        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                        reader.hide_utf8_warnings = True
                        if self.args.force_utf_8:
                            logging.info("FORCE UTF-8 is set to TRUE")
                            reader.force_utf8 = True
                        logging.info(f"running {file_name}")
                        self.read_records(reader)
                except Exception:
                    logging.exception(file_name, stack_info=True)
            # wrap up
            self.wrap_up()

    def read_records(self, reader):
        for idx, record in enumerate(reader):
            self.mapper.add_stats(
                self.mapper.stats, "MARC21 records in file before parsing"
            )
            try:
                if record is None:
                    self.mapper.add_to_migration_report(
                        "Bib records that failed to parse",
                        f"{reader.current_exception} {reader.current_chunk}",
                    )
                    self.mapper.add_stats(
                        self.mapper.stats,
                        "MARC21 Records with encoding errors - parsing failed",
                    )
                    raise TransformationCriticalDataError(
                        f"Index in file:{idx}",
                        f"MARC parsing error: "
                        f"{reader.current_exception}",
                        reader.current_chunk,
                    )
                else:
                    self.set_leader(record)
                    self.mapper.add_stats(
                        self.mapper.stats, "MARC21 Records successfully parsed"
                    )
                    self.processor.process_record(idx, record, False)
            except TransformationCriticalDataError as error:
                logging.error(error)

    @staticmethod
    def set_leader(marc_record: Record):
        new_leader = marc_record.leader
        marc_record.leader = new_leader[:9] + "a" + new_leader[10:]

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        self.processor.wrap_up()
        with open(self.migration_report_file, "w+") as report_file:
            report_file.write(f"# Bibliographic records transformation results   \n")
            report_file.write(f"Time Run: {dt.isoformat(dt.utcnow())}   \n")
            report_file.write(f"## Bibliographic records transformation counters   \n")
            self.mapper.print_dict_to_md_table(
                self.mapper.stats,
                report_file,
                "Measure",
                "Count",
            )
            self.mapper.write_migration_report(report_file)
            self.mapper.print_mapping_report(report_file)

        logging.info(
            f"Done. Transformation report written to {self.migration_report_file}"
        )


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("source_folder", help="path to marc records folder")
    parser.add_argument("results_folder", help="path to Instance results folder")
    parser.add_argument("okapi_url", help="OKAPI base url")
    parser.add_argument("tenant_id", help="id of the FOLIO tenant.")
    parser.add_argument("username", help="the api user")
    parser.add_argument("password", help="the api users password")
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
        help="forcing UTF8 when pasing marc records",
        action="store_true",
    )
    parser.add_argument(
        "-msu_locations_path", "-f", help="filter records based on MSU rules"
    )
    parser.add_argument(
        "-suppress",
        "-ds",
        help="This batch of records are to be suppressed in FOLIO.",
        action="store_true",
    )
    parser.add_argument(
        "-postgres_dump",
        "-p",
        help="results will be written out for Postgres" "ingestion. Default is JSON",
        action="store_true",
    )
    parser.add_argument(
        "-marcxml", "-x", help="DATA is in MARCXML format", action="store_true"
    )
    parser.add_argument(
        "-validate",
        "-v",
        help="Validate JSON data against JSON Schema",
        action="store_true",
    )
    args = parser.parse_args()
    return args


def main():
    """Main Method. Used for bootstrapping. """
    try:
        # Parse CLI Arguments
        args = parse_args()
        Worker.setup_logging(join(args.results_folder, "bib_transformation.log"))
        results_file = join(args.results_folder, "folio_instances.json")
        migration_report_file = join(
            args.results_folder, "instance_transformation_report.md"
        )

        logging.info(f"Results will be saved at:\t{args.results_folder}")
        logging.info(f"Okapi URL:\t{args.okapi_url}")
        logging.info(f"Tenant Id:\t{args.tenant_id}")
        logging.info(f"Username:   \t{args.username}")
        logging.info(f"Password:   \tSecret")
        folio_client = FolioClient(
            args.okapi_url, args.tenant_id, args.username, args.password
        )
        # Initiate Worker
        worker = Worker(folio_client, results_file, migration_report_file, args)
        worker.work()
    except FileNotFoundError as fne:
        print(f"{fne}")


if __name__ == "__main__":
    main()
