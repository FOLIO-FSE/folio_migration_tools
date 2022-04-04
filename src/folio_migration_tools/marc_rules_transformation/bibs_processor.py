""" Class that processes each MARC record """
import json
import logging

from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.helper import Helper
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import (
    BibsRulesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_blurbs import Blurbs
from pymarc.record import Record


class BibsProcessor:
    """the processor"""

    def __init__(
        self,
        mapper: BibsRulesMapper,
        folio_client: FolioClient,
        results_file,
        folder_structure: FolderStructure,
    ):
        self.results_file = results_file
        self.folio_client = folio_client
        self.instance_schema = folio_client.get_instance_json_schema()
        self.mapper: BibsRulesMapper = mapper
        self.folders = folder_structure
        self.srs_records_file = open(self.folders.srs_records_path, "w+")
        self.instance_id_map_file = open(self.folders.instance_id_map_path, "w+")
        self.instance_identifiers = set()

    def process_record(self, idx, marc_record: Record, suppressed: bool):
        """processes a marc record and saves it"""
        try:
            legacy_ids = self.mapper.get_legacy_ids(
                marc_record, self.mapper.task_configuration.ils_flavour, idx
            )
        except TransformationRecordFailedError as trf:
            trf.log_it()
        except Exception as ee:
            index_or_legacy_id = [
                f"Index in file: {idx}"
            ]  # Only used for reporting purposes
            Helper.log_data_issue(index_or_legacy_id, "001 nor legacy id found", ee)
        folio_rec = None
        try:
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.mapper.parse_bib(legacy_ids, marc_record, suppressed)
            if prec_titles := folio_rec.get("precedingTitles", []):
                self.mapper.migration_report.add(
                    Blurbs.PrecedingSuccedingTitles, f"{len(prec_titles)}"
                )
                del folio_rec["precedingTitles"]
            if succ_titles := folio_rec.get("succeedingTitles", []):
                del folio_rec["succeedingTitles"]
                self.mapper.migration_report.add(
                    Blurbs.PrecedingSuccedingTitles, f"{len(succ_titles)}"
                )
            filtered_legacy_ids = self.get_valid_instance_ids(
                folio_rec,
                legacy_ids,
                self.instance_identifiers,
                self.mapper.migration_report,
            )
            self.save_instance_ids_to_file(suppressed, folio_rec, filtered_legacy_ids)
            Helper.write_to_file(self.results_file, folio_rec)
            self.mapper.save_source_record(
                self.srs_records_file,
                FOLIONamespaces.instances,
                self.folio_client,
                marc_record,
                folio_rec,
                legacy_ids[0],
                suppressed,
            )
            self.mapper.migration_report.add_general_statistics(
                "Records successfully transformed into FOLIO objects"
            )

        except ValueError as value_error:
            self.mapper.migration_report.add(
                Blurbs.FieldMappingErrors,
                f"{value_error} for {legacy_ids} ",
            )
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
        except TransformationRecordFailedError as error:
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
            error.id = legacy_ids
            error.log_it()

        except Exception as inst:
            self.mapper.migration_report.add_general_statistics(
                "Records that failed Due to a unhandled exception",
            )
            self.mapper.migration_report.add_general_statistics(
                f"Transformation exceptions: {inst.__class__.__name__}",
            )
            logging.error(type(inst))
            logging.error(inst.args)
            logging.error(inst)
            logging.error(marc_record)
            if folio_rec:
                logging.error(folio_rec)
            raise inst from inst

    def save_instance_ids_to_file(self, suppressed, folio_rec, filtered_legacy_ids):
        for legacy_id in filtered_legacy_ids:
            self.instance_identifiers.add(legacy_id)
            s = json.dumps(
                {
                    "legacy_id": legacy_id,
                    "folio_id": folio_rec["id"],
                    "instance_hrid": folio_rec["hrid"],
                    "suppressed": suppressed,
                }
            )
            self.instance_id_map_file.write(f"{s}\n")
            self.mapper.migration_report.add_general_statistics(
                "Lines written to identifier map"
            )

    @staticmethod
    def get_valid_instance_ids(
        folio_rec, legacy_ids, instance_identifiers, migration_report: MigrationReport
    ):
        new_ids = set()
        for legacy_id in legacy_ids:
            if legacy_id not in instance_identifiers:
                new_ids.add(legacy_id)
            else:
                s = "Duplicate Instance identifiers "
                migration_report.add_general_statistics(s)
                Helper.log_data_issue(legacy_id, s, "-".join(legacy_ids))
                logging.error(s)
        if not any(new_ids):
            s = "Failed records. No unique bib identifiers in legacy record"
            migration_report.add_general_statistics(s)
            raise TransformationRecordFailedError(
                "-".join(legacy_ids),
                "Duplicate recod identifier(s). See logs. Record Failed",
                "-".join(legacy_ids),
            )
        return list(new_ids)

    def wrap_up(self):  # sourcery skip: remove-redundant-fstring
        """Finalizes the mapping by writing things out."""
        try:
            self.mapper.wrap_up()
        except Exception:
            logging.exception("error during wrap up")
        if any(self.mapper.holdings_map):
            logging.info("Saving holdings created from bibs")
            holdings_path = self.folders.data_folder / "folio_holdings_from_bibs.json"
            with open(holdings_path, "w+") as holdings_file:
                for key, holding in self.mapper.holdings_map.items():
                    Helper.write_to_file(holdings_file, holding)
        self.srs_records_file.close()
        self.instance_id_map_file.close()
