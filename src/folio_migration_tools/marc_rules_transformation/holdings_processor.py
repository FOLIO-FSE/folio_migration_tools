""" Class that processes each MARC record """
import logging
import sys
import time
import traceback
from datetime import datetime as dt
from folio_uuid.folio_namespaces import FOLIONamespaces
from pymarc import Record
from folio_migration_tools import library_configuration
from pymarc import Field
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.helper import Helper
from folio_migration_tools.holdings_helper import HoldingsHelper
from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    HridHandling,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.report_blurbs import Blurbs


class HoldingsProcessor:
    """the processor"""

    def __init__(self, mapper, folder_structure: FolderStructure):
        self.folder_structure: FolderStructure = folder_structure
        self.records_count = 0
        self.unique_001s = set()
        self.failed_records_count = 0
        self.mapper: RulesMapperHoldings = mapper
        self.start = time.time()
        self.created_objects_file = open(
            self.folder_structure.created_objects_path, "w+"
        )
        self.srs_records_file = open(self.folder_structure.srs_records_path, "w+")
        self.setup_holdings_sources()

    def setup_holdings_sources(self):
        if library_configuration.FolioRelease != FolioRelease.juniper:
            holdings_sources = list(
                self.mapper.folio_client.folio_get_all(
                    "/holdings-sources", "holdingsRecordsSources"
                )
            )
            logging.info(
                "Fetched %s holdingsRecordsSources from tenant", len(holdings_sources)
            )
            self.holdingssources = {
                n["name"].upper(): n["id"] for n in holdings_sources
            }
            if "FOLIO" not in self.holdingssources:
                raise TransformationProcessError(
                    "", "No holdings source with name FOLIO in tenant"
                )
            if "MARC" not in self.holdingssources:
                raise TransformationProcessError(
                    "", "No holdings source with name MARC in tenant"
                )
        else:
            self.holdingssources = {}

    def exit_on_too_many_exceptions(self):
        if (
            self.failed_records_count / (self.records_count + 1)
            > (self.mapper.library_configuration.failed_percentage_threshold / 100)
            and self.failed_records_count
            > self.mapper.library_configuration.failed_records_threshold
        ):
            logging.critical("More than 20 percent of the records have failed. Halting")
            sys.exit()

    def process_record(self, marc_record: Record, file_def: FileDefinition):
        """processes a marc holdings record and saves it"""
        success = True
        folio_rec = {}
        try:
            self.records_count += 1
            # Transform the MARC21 to a FOLIO record
            legacy_id = self.get_legacy_id(
                self.mapper.task_configuration.legacy_id_marc_path, marc_record
            )
            if not legacy_id:
                raise TransformationRecordFailedError(
                    self.records_count,
                    "legacy_id was empty",
                    self.mapper.task_configuration.legacy_id_marc_path,
                )
            folio_rec = self.mapper.parse_hold(marc_record, legacy_id)
            HoldingsHelper.handle_notes(folio_rec)
            if not folio_rec.get("instanceId", ""):
                raise TransformationRecordFailedError(
                    "".join(folio_rec.get("formerIds", [])),
                    "Missing instance ids. Something is wrong.",
                    "",
                )
            folio_rec["discoverySuppress"] = file_def.suppressed
            self.set_source_id(
                self.mapper.task_configuration, folio_rec, self.holdingssources
            )
            self.set_hrid(marc_record, folio_rec)
            self.save_srs_record(marc_record, file_def, folio_rec, legacy_id)
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
            raise tpe from tpe
        except Exception as inst:
            success = False
            traceback.print_exc()
            logging.error(type(inst))
            logging.error(inst.args)
            logging.error(inst)
            logging.error(marc_record)
            logging.error(folio_rec)
            raise inst from inst
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

    @staticmethod
    def get_legacy_id(marc_path: str, marc_record: Record):
        split = marc_path.split("$", maxsplit=1)
        if not (split[0].isnumeric() and len(split[0]) == 3):
            raise TransformationProcessError(
                "",
                (
                    "the marc field used for determining the legacy id is not numeric "
                    "or does not have the stipulated lenght of 3."
                    "Make sure the task configuration setting for 'legacyIdMarcPath' "
                    "is correct or make this piece of code more allowing"
                ),
                marc_path,
            )
        elif len(split) == 1:
            return marc_record[split[0]].value()
        elif len(split) == 2 and len(split[1]) == 1:
            for field in marc_record.get_fields(split[0]):
                if sf := field.get_subfields(split[1]):
                    return sf[0]
            raise TransformationRecordFailedError(
                "", "Subfield not found in record", split[1]
            )

        else:
            raise TransformationProcessError(
                "",
                ("Something is wrong with 'legacyIdMarcPath' property in the settings"),
                marc_path,
            )

    def save_srs_record(self, marc_record, file_def, folio_rec, legacy_id: str):
        if self.mapper.task_configuration.create_source_records:
            self.add_hrid_to_records(folio_rec, marc_record)
            if "008" in marc_record and len(marc_record["008"].data) > 32:
                remain, rest = (
                    marc_record["008"].data[:32],
                    marc_record["008"].data[32:],
                )
                marc_record["008"].data = remain
                self.mapper.migration_report.add(
                    Blurbs.MarcValidation,
                    f"008 lenght invalid. '{rest}' was stripped out",
                )
            for former_id in folio_rec["formerIds"]:
                if map_entity := self.mapper.instance_id_map.get(former_id, ""):
                    new_004 = Field(tag="004", data=map_entity["instance_hrid"])
                    marc_record.remove_fields("004")
                    marc_record.add_ordered_field(new_004)
                if self.mapper.task_configuration.hrid_handling == HridHandling.default:
                    new_035 = Field(
                        tag="035",
                        indicators=[" ", " "],
                        subfields=["a", former_id],
                    )
                    marc_record.add_ordered_field(new_035)
            self.mapper.save_source_record(
                self.srs_records_file,
                FOLIONamespaces.holdings,
                self.mapper.folio_client,
                marc_record,
                folio_rec,
                legacy_id,
                file_def.suppressed,
            )
            self.mapper.migration_report.add_general_statistics(
                "SRS records written to disk"
            )

    def set_hrid(self, marc_record, folio_rec):
        if self.mapper.task_configuration.hrid_handling == HridHandling.preserve001:
            value = marc_record["001"].value()
            if value in self.unique_001s:
                self.mapper.migration_report.add(
                    Blurbs.HridHandling, "Duplicate 001. Creating HRID instead"
                )
                Helper.log_data_issue(
                    folio_rec["formerIds"],
                    "Duplicate 001 for record. HRID created for record",
                    value,
                )
                num_part = str(self.mapper.instance_hrid_counter).zfill(11)
                folio_rec["hrid"] = f"{self.mapper.instance_hrid_prefix}{num_part}"
                new_001 = Field(tag="001", data=folio_rec["hrid"])
                marc_record.add_ordered_field(new_001)
                self.mapper.instance_hrid_counter += 1
            else:
                self.unique_001s.add(value)
                folio_rec["hrid"] = value
                self.mapper.migration_report.add(
                    Blurbs.HridHandling, "Took HRID from 001"
                )

    def add_hrid_to_records(self, folio_record: dict, marc_record: Record):
        if (
            "hrid" in folio_record
            and "001" in marc_record
            and marc_record["001"].value() == folio_record["hrid"]
        ):
            return
        num_part = str(self.mapper.holdings_hrid_counter).zfill(11)
        folio_record["hrid"] = f"{self.mapper.holdings_hrid_prefix}{num_part}"
        new_001 = Field(tag="001", data=folio_record["hrid"])
        marc_record.remove_fields("001")
        marc_record.add_ordered_field(new_001)
        self.mapper.holdings_hrid_counter += 1

    @staticmethod
    def set_source_id(task_configuration, folio_rec, holdingssources):
        if library_configuration.FolioRelease != FolioRelease.juniper:
            if task_configuration.create_source_records:
                folio_rec["sourceId"] = holdingssources.get("MARC")
            else:
                folio_rec["sourceId"] = holdingssources.get("FOLIO")

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
            self.mapper.migration_report.write_migration_report(report_file)
            Helper.print_mapping_report(
                report_file,
                self.mapper.parsed_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        self.srs_records_file.close()
        self.mapper.wrap_up()

        logging.info("Done. Transformation report written to %s", report_file.name)
        logging.info("Done.")
