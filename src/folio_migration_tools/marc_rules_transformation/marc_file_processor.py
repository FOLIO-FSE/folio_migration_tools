import logging
import sys
import time
import traceback
from typing import List
import i18n

from folio_uuid.folio_namespaces import FOLIONamespaces
from pymarc import Field
from pymarc import Record
from pymarc import Subfield

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


class MarcFileProcessor:
    def __init__(
        self, mapper: RulesMapperBase, folder_structure: FolderStructure, created_objects_file
    ):
        self.object_type: FOLIONamespaces = folder_structure.object_type
        self.folder_structure: FolderStructure = folder_structure
        self.mapper: RulesMapperBase = mapper
        self.created_objects_file = created_objects_file
        self.srs_records_file = open(self.folder_structure.srs_records_path, "w+")
        self.unique_001s: set = set()
        self.failed_records_count: int = 0
        self.records_count: int = 0
        self.start: float = time.time()
        self.legacy_ids: set = set()
        if (
            self.object_type == FOLIONamespaces.holdings
            and self.mapper.task_configuration.create_source_records
        ):
            logging.info("Loading Parent HRID map for SRS creation")
            self.parent_hrids = {entity[1]: entity[2] for entity in mapper.parent_id_map.values()}

    def process_record(self, idx: int, marc_record: Record, file_def: FileDefinition):
        """processes a marc holdings record and saves it

        Args:
            idx (int): Index in file being parsed
            marc_record (Record): _description_
            file_def (FileDefinition): _description_

        Raises:
            TransformationProcessError: _description_
            TransformationRecordFailedError: _description_
        """
        success = True
        folio_recs = []
        self.records_count += 1
        try:
            # Transform the MARC21 to a FOLIO record
            legacy_ids = self.mapper.get_legacy_ids(marc_record, idx)
            if not legacy_ids:
                raise TransformationRecordFailedError(
                    f"Index in file: {idx}", "No legacy id found", idx
                )
            folio_recs = self.mapper.parse_record(marc_record, file_def, legacy_ids)
            for idx, folio_rec in enumerate(folio_recs):
                if idx == 0:
                    filtered_legacy_ids = self.get_valid_folio_record_ids(
                        legacy_ids, self.legacy_ids, self.mapper.migration_report
                    )
                    self.add_legacy_ids_to_map(folio_rec, filtered_legacy_ids)

                    self.save_srs_record(
                        marc_record,
                        file_def,
                        folio_rec,
                        legacy_ids,
                        self.object_type,
                    )
                Helper.write_to_file(self.created_objects_file, folio_rec)
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("Inventory records written to disk")
                )
                self.exit_on_too_many_exceptions()

        except TransformationRecordFailedError as error:
            success = False
            raise TransformationRecordFailedError(
                f"{error.index_or_id} in {file_def.file_name}", error.message, error.data_value
            ) from error
        except TransformationProcessError as tpe:
            raise TransformationProcessError(
                f"{tpe.index_or_id} in {file_def.file_name}", tpe.message, tpe.data_value
            ) from tpe
        except Exception as inst:
            success = False
            traceback.print_exc()
            logging.error(type(inst))
            logging.error(inst.args)
            logging.error(inst)
            logging.error(marc_record)
            logging.error(folio_recs)
            raise TransformationProcessError("", inst.args, "") from inst
        finally:
            if not success:
                self.failed_records_count += 1
                remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
                for folio_rec in folio_recs:
                    if (
                        callable(remove_from_id_map)
                        and "folio_rec" in locals()
                        and folio_rec.get("formerIds", "")
                    ):
                        self.mapper.remove_from_id_map(folio_rec.get("formerIds", []))

    def save_srs_record(
        self,
        marc_record: Record,
        file_def: FileDefinition,
        folio_rec,
        legacy_ids: List[str],
        object_type: FOLIONamespaces,
    ):
        if not self.mapper.task_configuration.create_source_records:
            return
        if object_type in [FOLIONamespaces.holdings]:
            if "008" in marc_record and len(marc_record["008"].data) > 32:
                remain, rest = (
                    marc_record["008"].data[:32],
                    marc_record["008"].data[32:],
                )
                marc_record["008"].data = remain
                self.mapper.migration_report.add(
                    "MarcValidation",
                    i18n.t("008 length invalid. '%{rest}' was stripped out", rest=rest),
                )
            self.add_mapped_location_code_to_record(marc_record, folio_rec)
            new_004 = Field(tag="004", data=self.parent_hrids[folio_rec["instanceId"]])
            marc_record.remove_fields("004")
            marc_record.add_ordered_field(new_004)
            for former_id in legacy_ids:
                if self.mapper.task_configuration.hrid_handling == HridHandling.default:
                    new_035 = Field(
                        tag="035",
                        indicators=[" ", " "],
                        subfields=[Subfield(code="a", value=former_id)],
                    )
                    marc_record.add_ordered_field(new_035)
        self.mapper.save_source_record(
            self.srs_records_file,
            self.object_type,
            self.mapper.folio_client,
            marc_record,
            folio_rec,
            legacy_ids,
            file_def.discovery_suppressed,
        )
        self.mapper.migration_report.add_general_statistics(i18n.t("SRS records written to disk"))

    def add_mapped_location_code_to_record(self, marc_record, folio_rec):
        location_code = next(
            (
                location["code"]
                for location in self.mapper.folio_client.locations
                if location["id"] == folio_rec["permanentLocationId"]
            ),
            None,
        )
        if "852" not in marc_record:
            raise TransformationRecordFailedError(
                "", "No 852 in record when storing new location code", ""
            )
        first_852 = marc_record.get_fields("852")[0]
        first_852.delete_subfield("b")
        while old_b := first_852.delete_subfield("b"):
            first_852.add_subfield("x", old_b, 0)
            self.mapper.migration_report.add(
                "LocationMapping", i18n.t("Additional 852$b was moved to 852$x")
            )
        first_852.add_subfield("b", location_code, 0)
        self.mapper.migration_report.add(
            "LocationMapping", i18n.t("Set 852 to FOLIO location code")
        )

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
                migration_report.add_general_statistics(
                    i18n.t("Duplicate MARC record identifiers ")
                )
        if not any(new_ids):
            s = i18n.t("Failed records. No unique record identifiers in legacy record")
            migration_report.add_general_statistics(s)
            raise TransformationRecordFailedError(
                "-".join(legacy_ids),
                "Duplicate recod identifier(s). See logs. Record Failed",
                "-".join(legacy_ids),
            )
        return list(new_ids)

    def wrap_up(self):
        """Finalizes the mapping by writing things out."""
        logging.info(
            "Saving map of %s old and new IDs to %s",
            len(self.mapper.id_map),
            self.folder_structure.id_map_path,
        )
        self.mapper.save_id_map_file(self.folder_structure.id_map_path, self.mapper.id_map)
        logging.info("%s records processed", self.records_count)
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.mapper.migration_report.write_migration_report(
                i18n.t("MFHD records transformation report"),
                report_file,
                self.mapper.start_datetime,
            )
            Helper.print_mapping_report(
                report_file,
                self.mapper.parsed_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        self.srs_records_file.close()
        self.mapper.wrap_up()

        logging.info("Transformation report written to %s", report_file.name)
        logging.info("Processor is done.")

    def add_legacy_ids_to_map(self, folio_rec, filtered_legacy_ids):
        for legacy_id in filtered_legacy_ids:
            self.legacy_ids.add(legacy_id)
            if legacy_id not in self.mapper.id_map:
                self.mapper.id_map[legacy_id] = self.mapper.get_id_map_tuple(
                    legacy_id, folio_rec, self.object_type
                )

            else:
                raise TransformationRecordFailedError(
                    legacy_id,
                    "Legacy ID already added to Legacy Id map.",
                    ",".join(filtered_legacy_ids),
                )
