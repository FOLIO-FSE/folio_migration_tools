'''Main "script."'''
import csv
import ctypes
import json
import logging
import sys
import time
import traceback
import uuid
from os.path import isfile
from pathlib import Path
from typing import List, Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.item_mapper import ItemMapper
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.report_blurbs import Blurbs
from pydantic.main import BaseModel

from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


class ItemsTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        migration_task_type: str
        hrid_handling: HridHandling
        files: List[FileDefinition]
        items_mapping_file_name: str
        location_map_file_name: str
        default_call_number_type_name: str
        temp_location_map_file_name: Optional[str] = ""
        material_types_map_file_name: str
        loan_types_map_file_name: str
        temp_loan_types_map_file_name: Optional[str] = ""
        statistical_codes_map_file_name: Optional[str] = ""
        item_statuses_map_file_name: str
        call_number_type_map_file_name: str

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.items

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(library_config, task_config, use_logging)
        self.task_config = task_config
        self.files = [
            f
            for f in self.task_config.files
            if isfile(self.folder_structure.legacy_records_folder / f.file_name)
        ]
        if not any(self.files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                "",
                f"Files {ret_str} not found in {self.folder_structure.data_folder / 'items'}",
            )
        logging.info("Files to process:")
        for filename in self.files:
            logging.info("\t%s", filename.file_name)

        self.total_records = 0
        self.folio_keys = []
        self.items_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder
            / self.task_config.items_mapping_file_name
        )
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.items_map
        )
        if any(k for k in self.folio_keys if k.startswith("statisticalCodeIds")):
            statcode_mapping = self.load_ref_data_mapping_file(
                "statisticalCodeIds",
                self.folder_structure.mapping_files_folder
                / self.task_config.statistical_codes_map_file_name,
                self.folio_keys,
                False,
            )
        else:
            statcode_mapping = None

        if (
            self.folder_structure.mapping_files_folder
            / self.task_config.temp_loan_types_map_file_name
        ).is_file():
            temporary_loan_type_mapping = self.load_ref_data_mapping_file(
                "temporaryLoanTypeId",
                self.folder_structure.temp_loan_type_map_path,
                self.folio_keys,
            )
        else:
            logging.info(
                "%s not found. No temporary loan type mapping will be performed",
                self.folder_structure.temp_loan_type_map_path,
            )
            temporary_loan_type_mapping = None

        if (
            self.folder_structure.mapping_files_folder
            / self.task_config.temp_location_map_file_name
        ).is_file():
            temporary_location_mapping = self.load_ref_data_mapping_file(
                "temporaryLocationId",
                self.folder_structure.temp_locations_map_path,
                self.folio_keys,
            )
        else:
            logging.info(
                "%s not found. No temporary location mapping will be performed",
                self.folder_structure.temp_locations_map_path,
            )
            temporary_location_mapping = None
        self.mapper = ItemMapper(
            self.folio_client,
            self.items_map,
            self.load_ref_data_mapping_file(
                "materialTypeId",
                self.folder_structure.mapping_files_folder
                / self.task_config.material_types_map_file_name,
                self.folio_keys,
            ),
            self.load_ref_data_mapping_file(
                "permanentLoanTypeId",
                self.folder_structure.mapping_files_folder
                / self.task_config.loan_types_map_file_name,
                self.folio_keys,
            ),
            self.load_ref_data_mapping_file(
                "permanentLocationId",
                self.folder_structure.mapping_files_folder
                / self.task_config.location_map_file_name,
                self.folio_keys,
            ),
            self.load_ref_data_mapping_file(
                "itemLevelCallNumberTypeId",
                self.folder_structure.mapping_files_folder
                / self.task_config.call_number_type_map_file_name,
                self.folio_keys,
                False,
            ),
            self.load_id_map(self.folder_structure.holdings_id_map_path),
            statcode_mapping,
            self.load_ref_data_mapping_file(
                "status.name",
                self.folder_structure.item_statuses_map_path,
                self.folio_keys,
                False,
            ),
            temporary_loan_type_mapping,
            temporary_location_mapping,
            self.library_configuration,
        )
        logging.info("Init done")

    def do_work(self):
        logging.info("Starting....")
        with open(self.folder_structure.created_objects_path, "w+") as results_file:
            for file_name in self.files:
                try:
                    self.process_single_file(file_name, results_file)
                except Exception as exception:
                    error_str = f"\n\nProcessing of {file_name} failed:\n{exception}."
                    logging.exception(error_str, stack_info=True)
                    logging.fatal(
                        "Check source files for empty lines or missing reference data. Halting"
                    )
                    self.mapper.migration_report.add(
                        Blurbs.FailedFiles, f"{file_name} - {exception}"
                    )
                    logging.fatal(error_str)
                    sys.exit()
        logging.info(  # pylint: disable=logging-fstring-interpolation
            f"processed {self.total_records:,} records in {len(self.files)} files"
        )

    def process_single_file(self, file_name: FileDefinition, results_file):
        full_path = self.folder_structure.legacy_records_folder / file_name.file_name
        logging.info("Processing %s", full_path)
        records_in_file = 0
        with open(full_path, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                "Number of files processed"
            )
            start = time.time()
            for idx, record in enumerate(
                self.mapper.get_objects(records_file, full_path)
            ):
                try:
                    if idx == 0:
                        logging.info("First legacy record:")
                        logging.info(json.dumps(record, indent=4))
                    folio_rec, legacy_id = self.mapper.do_map(
                        record, f"row {idx}", FOLIONamespaces.items
                    )

                    self.handle_circiulation_notes(
                        folio_rec, self.folio_client.current_user
                    )
                    self.handle_notes(folio_rec)
                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(folio_rec, indent=4))
                    # TODO: turn this into a asynchrounous task
                    Helper.write_to_file(results_file, folio_rec)
                    self.mapper.migration_report.add_general_statistics(
                        "Number of records written to disk"
                    )
                    self.mapper.report_folio_mapping(folio_rec, self.mapper.schema)
                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as data_error:
                    self.mapper.handle_transformation_record_failed_error(
                        idx, data_error
                    )
                except AttributeError as attribute_error:
                    traceback.print_exc()
                    logging.fatal(attribute_error)
                    logging.info("Quitting...")
                    sys.exit()
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)
                self.mapper.migration_report.add(
                    Blurbs.GeneralStatistics,
                    f"Number of Legacy items in {file_name}",
                )
                self.mapper.migration_report.add_general_statistics(
                    "Number of legacy items in total"
                )
                self.print_progress(idx, start)
                records_in_file = idx + 1

            logging.info(  # pylint: disable=logging-fstring-interpolation
                f"Done processing {file_name} containing {records_in_file:,} records. "
                f"Total records processed: {records_in_file:,}"
            )
        self.total_records += records_in_file

    @staticmethod
    def handle_notes(folio_object):
        if folio_object.get("notes", []):
            filtered_notes = []
            for note_obj in folio_object.get("notes", []):
                if not note_obj.get("itemNoteTypeId", ""):
                    raise TransformationProcessError(
                        folio_object.get("legacyIds", ""),
                        "Missing note type id mapping",
                        json.dumps(note_obj),
                    )
                elif note_obj.get("note", "") and note_obj.get("itemNoteTypeId", ""):
                    filtered_notes.append(note_obj)
            if filtered_notes:
                folio_object["notes"] = filtered_notes
            else:
                del folio_object["notes"]

    @staticmethod
    def handle_circiulation_notes(folio_rec, current_user_uuid):
        if not folio_rec.get("circulationNotes", []):
            return
        filtered_notes = []
        for circ_note in folio_rec.get("circulationNotes", []):
            if circ_note.get("noteType", "") not in ["Check in", "Check out"]:
                raise TransformationProcessError(
                    "", "Circulation Note types are not mapped correclty"
                )
            if circ_note.get("note", ""):
                circ_note["id"] = str(uuid.uuid4())
                circ_note["source"] = {
                    "id": current_user_uuid,
                    "personal": {"lastName": "Data", "firstName": "Migration"},
                }
                filtered_notes.append(circ_note)
        if filtered_notes:
            folio_rec["circulationNotes"] = filtered_notes
        else:
            del folio_rec["circulationNotes"]

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        with open(
            self.folder_structure.migration_reports_file, "w"
        ) as migration_report_file:
            logging.info(
                "Writing migration and mapping report to %s",
                self.folder_structure.migration_reports_file,
            )
            self.mapper.migration_report.write_migration_report(migration_report_file)
            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        logging.info("All done!")
