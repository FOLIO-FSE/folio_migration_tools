import logging
import os
import sys
from datetime import datetime as dt
from os import listdir
from os.path import isfile

from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.colors import Bcolors
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.helper import Helper
from migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper
from migration_tools.migration_configuration import MigrationConfiguration
from pymarc import MARCReader
from pymarc.record import Record

from migration_tasks.migration_task_base import MigrationTaskBase


class BibsTransformer(MigrationTaskBase):
    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.instances

    def __init__(self, configuration: MigrationConfiguration):
        configuration.object_type = FOLIONamespaces.instances
        super().__init__(configuration)
        try:
            self.folder_structure.setup_migration_file_structure()
            # Initiate Worker
        except FileNotFoundError as fne:
            logging.error(fne)
        except TransformationProcessError as process_error:
            logging.critical(process_error)
            logging.critical("Halting...")
            sys.exit()

        # Old init
        self.files = [
            f
            for f in listdir(self.folder_structure.legacy_records_folder)
            if isfile(os.path.join(self.folder_structure.legacy_records_folder, f))
        ]
        logging.info("# of files to process: %s", len(self.files))
        for file_path in self.files:
            logging.info("\t%s", file_path)
        self.mapper = BibsRulesMapper(self.folio_client, self.configuration.args)
        self.processor = None
        self.bib_ids = set()
        logging.info("Init done")

    def do_work(self):
        logging.info("Starting....")
        with open(
            self.folder_structure.created_objects_path, "w+"
        ) as created_records_file:
            self.processor = BibsProcessor(
                self.mapper,
                self.folio_client,
                created_records_file,
                self.folder_structure,
                self.configuration.args,
            )
            for file_name in self.files:
                try:
                    with open(
                        self.folder_structure.legacy_records_folder / file_name,
                        "rb",
                    ) as marc_file:
                        reader = MARCReader(marc_file, to_unicode=True, permissive=True)
                        reader.hide_utf8_warnings = True
                        if self.configuration.args.force_utf_8 == "True":
                            logging.info("FORCE UTF-8 is set to TRUE")
                            reader.force_utf8 = True
                        else:
                            logging.info("FORCE UTF-8 is set to FALSE")
                            reader.force_utf8 = False
                        logging.info("running %s", file_name)
                        self.read_records(reader, file_name)
                except TransformationProcessError as tpe:
                    logging.critical(tpe)
                    sys.exit()
                except Exception:
                    logging.exception(file_name, stack_info=True)
                    logging.critical(
                        "File %s failed for unknown reason. Halting", file_name
                    )
                    sys.exit()
            # wrap up
            self.wrap_up()

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
    def add_arguments(parser):
        MigrationTaskBase.add_common_arguments(parser)
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
        parser.add_argument(
            "--hrid_handling", "-hh", help=hrid_handling, default="default"
        )
        parser.add_argument(
            "--suppress",
            "-ds",
            help="This batch of records are to be suppressed in FOLIO.",
            default=False,
            type=bool,
        )

    @staticmethod
    def set_leader(marc_record: Record):
        new_leader = marc_record.leader
        marc_record.leader = new_leader[:9] + "a" + new_leader[10:]
