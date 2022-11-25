import json
import logging
import sys
import time
import traceback
from pathlib import Path

from folio_uuid.folio_namespaces import FOLIONamespaces
from pymarc import Field
from pymarc import Record

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_blurbs import Blurbs


class MarcFileProcessor:
    def __init__(
        self, mapper: RulesMapperBase, folder_structure: FolderStructure, id_map_path: Path
    ):
        self.id_map_file = open(id_map_path, "w+")
        self.object_type: FOLIONamespaces = folder_structure.object_type
        self.folder_structure: FolderStructure = folder_structure
        self.mapper: RulesMapperBase = mapper
        self.created_objects_file = open(self.folder_structure.created_objects_path, "w+")
        self.srs_records_file = open(self.folder_structure.srs_records_path, "w+")
        self.unique_001s: set = set()
        self.failed_records_count: int = 0
        self.start: float = time.time()
        self.legacy_ids: set = set()

    def process_record(self, idx: int, marc_record: Record, file_def: FileDefinition):
        """processes a marc holdings record and saves it

        Args:
            idx (int): Index in file being parsed
            marc_record (Record): _description_
            file_def (FileDefinition): _description_

        Raises:
            TransformationProcessError: _description_
            Exception: _description_
        """
        success = True
        folio_rec = {}
        try:
            # Transform the MARC21 to a FOLIO record
            legacy_ids = self.mapper.get_legacy_ids(marc_record, idx)
            if not legacy_ids:
                raise TransformationRecordFailedError(
                    f"Index in file: {idx}", "No legacy id found", idx
                )
            folio_rec = self.mapper.parse_record(marc_record, file_def, legacy_ids)
            filtered_legacy_ids = self.get_valid_folio_record_ids(
                legacy_ids,
                self.legacy_ids,
                self.mapper.migration_report,
            )
            self.save_id_map_to_file(file_def, folio_rec, filtered_legacy_ids)

            self.save_srs_record(marc_record, file_def, folio_rec, legacy_ids)
            Helper.write_to_file(self.created_objects_file, folio_rec)
            self.mapper.migration_report.add_general_statistics(
                "Inventory records written to disk"
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
                self.object_type,
                self.mapper.folio_client,
                marc_record,
                folio_rec,
                legacy_id,
                file_def.suppressed,
            )
            self.mapper.migration_report.add_general_statistics("SRS records written to disk")

    def add_hrid_to_records(self, folio_record: dict, marc_record: Record):
        if (
            "hrid" in folio_record
            and "001" in marc_record
            and marc_record["001"].value() == folio_record["hrid"]
        ):
            return
        num_part = self.generate_num_part()
        folio_record["hrid"] = f"{self.mapper.holdings_hrid_prefix}{num_part}"
        new_001 = Field(tag="001", data=folio_record["hrid"])
        marc_record.remove_fields("001")
        marc_record.add_ordered_field(new_001)
        self.mapper.holdings_hrid_counter += 1

    def exit_on_too_many_exceptions(self):
        if (
            self.failed_records_count / (self.records_count + 1)
            > (self.mapper.library_configuration.failed_percentage_threshold / 100)
            and self.failed_records_count
            > self.mapper.library_configuration.failed_records_threshold
        ):
            logging.critical("More than 20 percent of the records have failed. Halting")
            sys.exit(1)

    @staticmethod
    def get_valid_folio_record_ids(
        legacy_ids, folio_record_identifiers, migration_report: MigrationReport
    ):
        new_ids = set()
        for legacy_id in legacy_ids:
            if legacy_id not in folio_record_identifiers:
                new_ids.add(legacy_id)
            else:
                s = "Duplicate MARC record identifiers "
                migration_report.add_general_statistics(s)
                Helper.log_data_issue(legacy_id, s, "-".join(legacy_ids))
                logging.error(s)
        if not any(new_ids):
            s = "Failed records. No unique MARC record identifiers in legacy record"
            migration_report.add_general_statistics(s)
            raise TransformationRecordFailedError(
                "-".join(legacy_ids),
                "Duplicate recod identifier(s). See logs. Record Failed",
                "-".join(legacy_ids),
            )
        return list(new_ids)

    def generate_num_part(self):
        return (
            str(self.mapper.holdings_hrid_counter).zfill(11)
            if self.mapper.common_retain_leading_zeroes
            else str(self.mapper.holdings_hrid_counter)
        )

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
            self.mapper.migration_report.write_migration_report(
                "MFHD records transformation report", report_file, self.mapper.start_datetime
            )
            Helper.print_mapping_report(
                report_file,
                self.mapper.parsed_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        self.srs_records_file.close()
        self.mapper.wrap_up()
        self.id_map_file.close()

        logging.info("Done. Transformation report written to %s", report_file.name)
        logging.info("Done.")

    def save_id_map_to_file(self, file_def: FileDefinition, folio_rec, filtered_legacy_ids):
        for legacy_id in filtered_legacy_ids:
            self.legacy_ids.add(legacy_id)
            s = json.dumps(
                {
                    "legacy_id": legacy_id,
                    "folio_id": folio_rec["id"],
                    "suppressed": file_def.suppressed,
                }
            )
            self.id_map_file.write(f"{s}\n")
            self.mapper.migration_report.add_general_statistics("Lines written to identifier map")
