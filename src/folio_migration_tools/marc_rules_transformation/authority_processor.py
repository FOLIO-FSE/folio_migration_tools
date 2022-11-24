import json
import logging

from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient
from pymarc import Record

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.marc_rules_transformation.bibs_processor import BibsProcessor
from folio_migration_tools.marc_rules_transformation.rules_mapper_autorities import (
    AuthorityMapper,
)
from folio_migration_tools.report_blurbs import Blurbs


class AuthorityProcessor:
    def __init__(
        self,
        mapper: AuthorityMapper,
        folio_client: FolioClient,
        results_file,
        folder_structure: FolderStructure,
    ):
        self.results_file = results_file
        self.folio_client = folio_client
        self.mapper: AuthorityMapper = mapper
        self.folder_structure = folder_structure
        self.srs_records_file = open(self.folder_structure.srs_records_path, "w+")
        self.auth_id_map_file = open(self.folder_structure.auth_id_map_path, "w+")
        self.auth_identifiers: set = set()

    def get_autority_json_schema(self, latest_release=True):
        """Fetches the JSON Schema for autorities"""
        return self.folio_client.get_latest_from_github(
            "folio-org", "mod-inventory-storage", "/ramls/authorities/authority.json"
        )

    def process_record(self, idx, marc_record: Record, file_definition: FileDefinition):
        """processes a marc record and saves it


        Args:
            idx (_type_): _description_
            marc_record (Record): _description_
            file_definition (FileDefinition): _description_

        Raises:
            Exception: _description_
        """
        folio_rec = None
        legacy_ids = []
        try:
            legacy_ids = self.mapper.get_legacy_ids(
                marc_record, self.mapper.task_configuration.ils_flavour, idx
            )
            if not legacy_ids:
                raise TransformationRecordFailedError(
                    f"Index in file: {idx}", "No legacy id found", idx
                )
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.mapper.parse_auth(legacy_ids, marc_record, file_definition)
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
            filtered_legacy_ids = BibsProcessor.get_valid_folio_record_ids(
                legacy_ids,
                self.auth_identifiers,
                self.mapper.migration_report,
            )
            self.save_autority_ids_to_file(file_definition, folio_rec, filtered_legacy_ids)
            Helper.write_to_file(self.results_file, folio_rec)
            self.mapper.save_source_record(
                self.srs_records_file,
                FOLIONamespaces.athorities,
                self.folio_client,
                marc_record,
                folio_rec,
                legacy_ids[0],
                file_definition.suppressed,
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
            error.index_or_id = legacy_ids
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

    def wrap_up(self):
        logging.info("Processor wrapping up...")

    def save_autority_ids_to_file(self, file_def: FileDefinition, folio_rec, filtered_legacy_ids):
        for legacy_id in filtered_legacy_ids:
            self.auth_identifiers.add(legacy_id)
            s = json.dumps(
                {
                    "legacy_id": legacy_id,
                    "folio_id": folio_rec["id"],
                    "suppressed": file_def.suppressed,
                }
            )
            self.auth_id_map_file.write(f"{s}\n")
            self.mapper.migration_report.add_general_statistics("Lines written to identifier map")
