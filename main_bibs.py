'''Main "script."'''
import argparse
import json
import logging
import csv
import copy
from marc_to_folio.rules_mapper_bibs import BibsRulesMapper
from marc_to_folio.folder_structure import FolderStructure
import os
from marc_to_folio.custom_exceptions import TransformationCriticalDataError
import sys
from argparse_prompt import PromptParser
import traceback
from os import listdir
from os.path import isfile, dirname
from datetime import datetime as dt
import time

from folioclient.FolioClient import FolioClient
from pymarc import MARCReader
from pymarc.record import Record
from marc_to_folio import main_base

from marc_to_folio.bibs_processor import BibsProcessor


class Worker(main_base.MainBase):
    """Class that is responsible for the actual work"""

    def __init__(self, folio_client, folder_structure: FolderStructure, args):
        # msu special case
        self.args = args
        self.folder_structure = folder_structure
        self.files = [
            f
            for f in listdir(folder_structure.legacy_records_folder)
            if isfile(os.path.join(folder_structure.legacy_records_folder, f))
        ]
        self.folio_client = folio_client
        logging.info(f"# of files to process: {len(self.files)}")
        logging.info(json.dumps(self.files, sort_keys=True, indent=4))
        self.mapper = BibsRulesMapper(self.folio_client, args)
        self.processor = None
        self.bib_ids = set()
        logging.info("Init done")

    def work(self):
        logging.info("Starting....")
        with open(
            self.folder_structure.created_objects_path, "w+"
        ) as created_records_file:
            self.processor = BibsProcessor(
                self.mapper,
                self.folio_client,
                created_records_file,
                self.folder_structure,
                self.args,
            )
            for file_name in self.files:
                try:
                    with open(
                        self.folder_structure.legacy_records_folder / file_name,
                        "rb",
                    ) as marc_file:
                        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                        reader.hide_utf8_warnings = True
                        if self.args.force_utf_8 == "True":
                            logging.info("FORCE UTF-8 is set to TRUE")
                            reader.force_utf8 = True
                        else:
                            logging.info("FORCE UTF-8 is set to FALSE")
                            reader.force_utf8 = False
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
                        f"MARC parsing error: " f"{reader.current_exception}",
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
        logging.info(f"Done reading {idx} records from file")

    @staticmethod
    def set_leader(marc_record: Record):
        new_leader = marc_record.leader
        marc_record.leader = new_leader[:9] + "a" + new_leader[10:]

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        self.processor.wrap_up()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
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
            f"Done. Transformation report written to {self.folder_structure.migration_reports_file}"
        )


def parse_args():
    """Parse CLI Arguments"""
    # parser = argparse.ArgumentParser()
    parser = PromptParser()
    parser.add_argument("base_folder", help="path base folder", type=str)
    parser.add_argument("okapi_url", help="OKAPI base url")
    parser.add_argument("tenant_id", help="id of the FOLIO tenant.")
    parser.add_argument("username", help="the api user")
    parser.add_argument("--password", help="the api users password", secure=True)
    flavourhelp = (
        "The kind of ILS the records are coming from and how legacy bibliographic "
        "IDs are to be handled\nOptions:\n"
        "\taleph   \t- bib id in either 998$b or 001\n"
        "\tvoyager \t- bib id in 001\n"
        "\tsierra  \t- bib id in 907 $a\n"
        "\tmillennium \t- bib id in 907 $a\n"
        "\tkoha \t- bib id in 999 $c "
        "\t907y    \t- bib id in 907 $y\n"
        "\t001      \t- bib id in 001\n"
        "\t990a \t- bib id in 990 $a and 001\n "
        "\tnone      \t- Use for ebooks and related records that will not need any legacy id:s\n"
    )
    parser.add_argument("--ils_flavour", default="001", help=flavourhelp)
    parser.add_argument(
        "--holdings_records",
        "-hold",
        help="Create holdings records based on relevant MARC fields",
        default=False,
        type=bool,
    )
    hrid_handling = (
        "HRID Handling\n"
        "This overrides any HRID/001 setting from the mapping rules\n"
        "\tdefault\tFOLIO Default. Current 001 will be placed in a 035, and The "
        "FOLIO-generated HRID will be put in 001. FOLIO HRID prefix will be honored\n"
        "\t001\tHonor current 001:s. 001 will be used in the HRID field on the "
        "Instance, and the current 001 will be maintained"
    )
    parser.add_argument(
        "--force_utf_8",
        "-utf8",
        help=(
            "forcing UTF8 when parsing marc records. If you get a lot of encoding issues, test "
            "changing this setting to False"
        ),
        default="True",
    )
    parser.add_argument("--hrid_handling", "-hh", help=hrid_handling, default="default")
    parser.add_argument(
        "--suppress",
        "-ds",
        help="This batch of records are to be suppressed in FOLIO.",
        default=False,
        type=bool,
    )
    return parser.parse_args()


def main():
    """Main Method. Used for bootstrapping. """
    try:
        # Parse CLI Arguments
        args = parse_args()
        time_stamp = time.strftime("%Y%m%d-%H%M%S")
        folder_structure = FolderStructure(args.base_folder, time_stamp)
        folder_structure.setup_migration_file_structure("instance")
        Worker.setup_logging(folder_structure)
        folder_structure.log_folder_structure()

        logging.info(f"Okapi URL:\t{args.okapi_url}")
        logging.info(f"Tenant Id:\t{args.tenant_id}")
        logging.info(f"Username:   \t{args.username}")
        logging.info(f"Password:   \tSecret")
        folio_client = FolioClient(
            args.okapi_url, args.tenant_id, args.username, args.password
        )
        # Initiate Worker
        worker = Worker(folio_client, folder_structure, args)
        worker.work()
    except FileNotFoundError as fne:
        print(f"{fne}")


if __name__ == "__main__":
    main()
