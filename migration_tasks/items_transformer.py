'''Main "script."'''
import csv
import ctypes
import json
import logging
import sys
import time
import traceback
import uuid
from os import listdir
from os.path import isfile, join
from pathlib import Path
from typing import List, Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic.main import BaseModel
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)

from migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
from migration_tools.folder_structure import FolderStructure
from migration_tools.helper import Helper
from migration_tools.library_configuration import FileDefinition, HridHandling
from migration_tools.mapping_file_transformation.item_mapper import ItemMapper
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.migration_configuration import MigrationConfiguration
from migration_tools.report_blurbs import Blurbs

from migration_tasks.migration_task_base import MigrationTaskBase

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
        statistical_codes_map_file_name: str
        item_statuses_map_file_name: str
        call_number_type_map_file_name: str

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.items

    def __init__(
        self,
        # configuration: MigrationConfiguration,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(library_config, task_config)
        self.task_config = task_config
        self.files = [
            f
            for f in self.task_config.files
            if isfile(self.folder_structure.legacy_records_folder / f.file_name)
        ]
        if not any(self.files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                f"Files {ret_str} not found in {self.folder_structure.data_folder / 'items'}"
            )
        logging.info("Files to process:")
        for filename in self.files:
            logging.info("\t%s", filename.file_name)

        self.total_records = 0
        self.folio_keys = []
        self.items_map = self.setup_records_map()
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.items_map
        )
        self.failed_files: List[str] = list()
        if "statisticalCodes" in self.folio_keys:
            statcode_mapping = self.load_ref_data_mapping_file(
                "statisticalCodeIds",
                self.folder_structure.mapping_files_folder
                / self.task_config.statistical_codes_map_file_name,
                False,
            )
        else:
            statcode_mapping = None

        if (
            self.folder_structure.mapping_files_folder
            / self.task_config.temp_loan_types_map_file_name
        ).is_file():
            temporary_loan_type_mapping = self.load_ref_data_mapping_file(
                "temporaryLoanTypeId", self.folder_structure.temp_loan_type_map_path
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
                "temporaryLocationId", self.folder_structure.temp_locations_map_path
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
            ),
            self.load_ref_data_mapping_file(
                "permanentLoanTypeId",
                self.folder_structure.mapping_files_folder
                / self.task_config.loan_types_map_file_name,
            ),
            self.load_ref_data_mapping_file(
                "permanentLocationId",
                self.folder_structure.mapping_files_folder
                / self.task_config.location_map_file_name,
            ),
            self.load_ref_data_mapping_file(
                "itemLevelCallNumberTypeId",
                self.folder_structure.mapping_files_folder
                / self.task_config.call_number_type_map_file_name,
                False,
            ),
            setup_holdings_id_map(self.folder_structure),
            statcode_mapping,
            self.load_ref_data_mapping_file(
                "status.name", self.folder_structure.item_statuses_map_path, False
            ),
            temporary_loan_type_mapping,
            temporary_location_mapping,
        )
        logging.info("Init done")

    @staticmethod
    def add_arguments(sub_parser):
        MigrationTaskBase.add_common_arguments(sub_parser)
        sub_parser.add_argument(
            "timestamp",
            help=(
                "timestamp or migration identifier. "
                "Used to chain multiple runs together"
            ),
            secure=False,
        )
        sub_parser.add_argument(
            "--default_call_number_type_name",
            help=(
                "Name of the default callnumber type. Needs to exist "
                " in the tenant verbatim"
            ),
            default="Other scheme",
        )
        sub_parser.add_argument(
            "--suppress",
            "-ds",
            help="This batch of records are to be suppressed in FOLIO.",
            default=False,
            type=bool,
        )

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
            f"processed {self.total_records:,} records " f"in {len(self.files)} files"
        )

    def setup_records_map(self):
        with open(
            self.folder_structure.mapping_files_folder
            / self.task_config.items_mapping_file_name
        ) as items_mapper_f:
            items_map = json.load(items_mapper_f)
            logging.info("%s fields in item mapping file map", len(items_map["data"]))
            mapped_fields = (
                f
                for f in items_map["data"]
                if f["legacy_field"] and f["legacy_field"] != "Not mapped"
            )
            logging.info(
                "%s Mapped fields in item mapping file map", len(list(mapped_fields))
            )
            return items_map

    def load_ref_data_mapping_file(
        self, folio_property_name: str, map_file_path: Path, required: bool = True
    ):
        if (
            folio_property_name in self.folio_keys
            or required
            or folio_property_name.startswith("statisticalCodeIds")
        ):
            try:
                with open(map_file_path) as map_file:
                    ref_data_map = list(csv.DictReader(map_file, dialect="tsv"))
                    logging.info(
                        "Found %s rows in %s map",
                        len(ref_data_map),
                        folio_property_name,
                    )
                    logging.info(
                        "%s will be used for determinig %s",
                        ",".join(ref_data_map[0].keys()),
                        folio_property_name,
                    )
                    return ref_data_map
            except Exception as exception:
                raise TransformationProcessError(
                    f"{folio_property_name} not mapped in legacy->folio mapping file "
                    f"({map_file_path}) ({exception}). Did you map this field, "
                    "but forgot to add a mapping file?"
                )
        else:
            logging.info("No mapping setup for %s", folio_property_name)
            logging.info("%s will have default mapping if any ", folio_property_name)
            logging.info(
                "Add a file named %s and add the field to "
                "the item.mapping.json file.",
                map_file_path,
            )
            return None

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
                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(folio_rec, indent=4))
                    self.handle_circiulation_notes(folio_rec)
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

            total_records = 0
            total_records += records_in_file
            logging.info(  # pylint: disable=logging-fstring-interpolation
                f"Done processing {file_name} containing {records_in_file:,} records. "
                f"Total records processed: {total_records:,}"
            )
        self.total_records = total_records

    def handle_circiulation_notes(self, folio_rec):
        for circ_note in folio_rec.get("circulationNotes", []):
            circ_note["id"] = str(uuid.uuid4())
            circ_note["source"] = {
                "id": self.folio_client.current_user,
                "personal": {"lastName": "Data", "firstName": "Migration"},
            }

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        with open(
            self.folder_structure.migration_reports_file, "w"
        ) as migration_report_file:
            logging.info(
                "Writing migration and mapping report to %s",
                self.folder_structure.migration_reports_file,
            )
            Helper.write_migration_report(
                migration_report_file, self.mapper.migration_report
            )
            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        logging.info("All done!")


def setup_holdings_id_map(folder_structure: FolderStructure):
    logging.info("Loading holdings id map. This can take a while...")
    with open(folder_structure.holdings_id_map_path, "r") as holdings_id_map_file:
        holdings_id_map = json.load(holdings_id_map_file)
        logging.info("Loaded %s holdings ids", len(holdings_id_map))
        return holdings_id_map
