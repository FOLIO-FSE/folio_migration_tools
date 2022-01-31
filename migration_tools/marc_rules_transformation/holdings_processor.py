""" Class that processes each MARC record """
import logging
import sys
import time
import traceback
from datetime import datetime as dt

from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.library_configuration import FileDefinition
from migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)


class HoldingsProcessor:
    """the processor"""

    def __init__(self, mapper, folder_structure: FolderStructure):
        self.folder_structure: FolderStructure = folder_structure
        self.records_count = 0
        self.failed_records_count = 0
        self.mapper: RulesMapperHoldings = mapper
        self.start = time.time()
        self.created_objects_file = open(
            self.folder_structure.created_objects_path, "w+"
        )

    def exit_on_too_many_exceptions(self):
        if (
            self.records_count > 10000
            and self.records_count % 10000 == 0
            and self.failed_records_count / (self.records_count + 1) > 0.2
        ):
            logging.critical("More than 20 percent of the records have failed. Halting")
            sys.exit()

    def process_record(self, marc_record, file_def: FileDefinition):
        """processes a marc holdings record and saves it"""
        success = True
        try:
            self.records_count += 1
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.mapper.parse_hold(marc_record, self.records_count)
            if not folio_rec.get("instanceId", ""):
                raise TransformationRecordFailedError(
                    "".join(folio_rec.get("formerIds", [])),
                    "Missing instance ids. Something is wrong.",
                    "",
                )
            folio_rec["discoverySuppress"] = file_def.suppressed
            Helper.write_to_file(self.created_objects_file, folio_rec)
            self.mapper.migration_report.add_general_statistics(
                "Holdings records written to disk"
            )
            self.exit_on_too_many_exceptions()
        except TransformationRecordFailedError as error:
            success = False
            error.log_it()
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
        except TransformationProcessError as tpe:
            raise tpe  # Raise, since it should be handled higher up
        except Exception as inst:
            success = False
            traceback.print_exc()
            logging.error(type(inst))
            logging.error(inst.args)
            logging.error(inst)
            logging.error(marc_record)
            raise inst
        finally:
            if not success:
                self.failed_records_count += 1
                remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
                if (
                    callable(remove_from_id_map)
                    and "folio_rec" in locals()
                    and folio_rec.get("formerIds", "")
                ):
                    self.mapper.remove_from_id_map(folio_rec["formerIds"])

    def wrap_up(self):
        """Finalizes the mapping by writing things out."""
        self.created_objects_file.close()
        logging.info(
            "Saving map of %s old and new IDs to %s",
            len(self.mapper.holdings_id_map),
            self.folder_structure.holdings_id_map_path,
        )
        self.mapper.save_id_map_file(
            self.folder_structure.holdings_id_map_path, self.mapper.holdings_id_map
        )
        logging.info("%s records processed", self.records_count)
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            report_file.write("# MFHD records transformation results   \n")
            report_file.write(f"Time Finished: {dt.isoformat(dt.utcnow())}   \n")
            Helper.write_migration_report(report_file, self.mapper.migration_report)
            Helper.print_mapping_report(
                report_file,
                self.mapper.parsed_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )

        logging.info("Done. Transformation report written to %s", report_file.name)
        logging.info("Done.")
