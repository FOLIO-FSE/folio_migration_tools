'''Main "script."'''
import logging
import os
import sys
import time
from datetime import datetime as dt
from os import listdir
from os.path import isfile

import requests
from argparse_prompt import PromptParser
from folioclient.FolioClient import FolioClient
from pymarc import MARCReader
from pymarc.record import Record

from migration_tools import main_base
from migration_tools.colors import Bcolors
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper


class Worker(main_base.MainBase):
    """Class that is responsible for the actual work"""

    def __init__(self, folio_client, folder_structure: FolderStructure, args):
        # msu special case
        super().__init__()
        self.args = args
        self.folder_structure = folder_structure
        self.files = [
            f
            for f in listdir(folder_structure.legacy_records_folder)
            if isfile(os.path.join(folder_structure.legacy_records_folder, f))
        ]
        self.folio_client = folio_client
        logging.info("# of files to process: %s", len(self.files))
        for file_path in self.files:
            logging.info("\t%s", file_path)
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
                        logging.info("running %s", file_name)
                        self.read_records(reader, file_name)
                except TransformationProcessError as tpe:
                    logging.critical(tpe)
                    exit()
                except Exception:
                    logging.exception(file_name, stack_info=True)
            # wrap up
            self.wrap_up()

    def read_records(self, reader, file_name):
        for idx, record in enumerate(reader):
            self.mapper.migration_report.add_general_statistics(
                "Records in file before parsing"
            )
            try:
                if record is None:
                    self.mapper.migration_report.add_general_statistics(
                        "Records with encoding errors - parsing failed",
                    )
                    raise TransformationRecordFailedError(
                        f"Index in {file_name}:{idx}",
                        f"MARC parsing error: {reader.current_exception}",
                        reader.current_chunk,
                    )
                else:
                    self.set_leader(record)
                    self.mapper.migration_report.add_general_statistics(
                        "Records successfully parsed from MARC21",
                    )
                    self.processor.process_record(idx, record, False)
            except TransformationRecordFailedError as error:
                error.log_it()
        logging.info("Done reading %s records from file", idx + 1)

    @staticmethod
    def set_leader(marc_record: Record):
        new_leader = marc_record.leader
        marc_record.leader = new_leader[:9] + "a" + new_leader[10:]

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        self.processor.wrap_up()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            report_file.write("# Bibliographic records transformation results   \n")
            report_file.write(f"Time Run: {dt.isoformat(dt.utcnow())}   \n")
            Helper.write_migration_report(report_file, self.mapper.migration_report)
            Helper.print_mapping_report(
                report_file,
                self.mapper.parsed_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )

        logging.info(
            "Done. Transformation report written to %s",
            self.folder_structure.migration_reports_file.name,
        )


def parse_args():
    """Parse CLI Arguments"""
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
    version_help = "The FOLIO release you are targeting. Valid values include:\n\t->iris\n\t->juniper\n"
    parser.add_argument("--folio_version", default="juniper", help=version_help)
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
        "Instance, and the current 001 will be maintained\n"
        "\t\t In the absence of a 001 to derive the HRID from, the script will fall "
        "back on the default HRID handling."
    )
    parser.add_argument(
        "--force_utf_8",
        "-utf8",
        help=(
            "forcing UTF8 when parsing marc records. If you get a lot of encoding issues, test "
            "changing this setting to False \n"
            f"\n⚠ {Bcolors.WARNING}WARNING!{Bcolors.ENDC} ⚠ \nEven though setting this to False might make your migrations run smoother, it might lead to data loss in individual fields"
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
    """Main Method. Used for bootstrapping."""
    try:
        # Parse CLI Arguments
        args = parse_args()
        time_stamp = time.strftime("%Y%m%d-%H%M%S")
        folder_structure = FolderStructure(args.base_folder, time_stamp)
        folder_structure.setup_migration_file_structure("instance")
        Worker.setup_logging(folder_structure)
        folder_structure.log_folder_structure()

        logging.info("Okapi URL:\t%s", args.okapi_url)
        logging.info("Tenant Id:\t%s", args.tenant_id)
        logging.info("Username:   \t%s", args.username)
        logging.info("Password:   \tSecret")
        try:
            folio_client = FolioClient(
                args.okapi_url, args.tenant_id, args.username, args.password
            )
        except requests.exceptions.SSLError:
            logging.critical(
                "SSL error. Check your VPN or Internet connection. Exiting"
            )
            sys.exit()
        # Initiate Worker
        worker = Worker(folio_client, folder_structure, args)
        worker.work()
    except FileNotFoundError as fne:
        logging.error(fne)
    except TransformationProcessError as process_error:
        logging.critical(process_error)
        logging.critical("Halting...")
        sys.exit()


if __name__ == "__main__":
    main()
