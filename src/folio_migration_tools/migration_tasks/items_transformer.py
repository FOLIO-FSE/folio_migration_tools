"""Item records transformation task.

Transforms item records from CSV/TSV files to FOLIO Item records using mapping
files. Handles material types, loan types, location mapping, and statistical codes.
"""

import csv
import ctypes
import json
import logging
import sys
import time
import traceback
import uuid
from typing import Annotated, List, Optional

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.item_mapper import ItemMapper
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.marc_rules_transformation.hrid_handler import HRIDHandler
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration

logger = logging.getLogger(__name__)

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


class ItemsTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        """Task configuration for ItemsTransformer."""

        name: Annotated[
            str,
            Field(
                title="Task name",
                description="Name of the task.",
            ),
        ]
        migration_task_type: Annotated[
            str,
            Field(
                title="Migration task type",
                description="Type of migration task.",
            ),
        ]
        hrid_handling: Annotated[
            HridHandling,
            Field(
                title="HRID handling",
                description=("Determining how the HRID generation should be handled."),
            ),
        ]
        files: Annotated[
            List[FileDefinition],
            Field(
                title="Files",
                description="List of files.",
            ),
        ]
        items_mapping_file_name: Annotated[
            str,
            Field(
                title="Items mapping file name",
                description="File name for items mapping.",
            ),
        ]
        location_map_file_name: Annotated[
            str,
            Field(
                title="Location map file name",
                description="File name for location map.",
            ),
        ]
        default_call_number_type_name: Annotated[
            str,
            Field(
                title="Default call number type name",
                description="Default name for call number type.",
            ),
        ]
        temp_location_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Temporary location map file name",
                description=("Temporary file name for location map. Empty string by default."),
            ),
        ] = ""
        material_types_map_file_name: Annotated[
            str,
            Field(
                title="Material types map file name",
                description="File name for material types map.",
            ),
        ]
        loan_types_map_file_name: Annotated[
            str,
            Field(
                title="Loan types map file name",
                description="File name for loan types map.",
            ),
        ]
        temp_loan_types_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Temporary loan types map file name",
                description=("File name for temporary loan types map. Empty string by default."),
            ),
        ] = ""
        statistical_codes_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Statistical code map file name",
                description=(
                    "Path to the file containing the mapping of statistical codes. "
                    "The file should be in TSV format with legacy_stat_code "
                    "and folio_code columns."
                ),
            ),
        ] = ""
        item_statuses_map_file_name: Annotated[
            str,
            Field(
                title="Item statuses map file name",
                description="File name for item statuses map.",
            ),
        ]
        call_number_type_map_file_name: Annotated[
            str,
            Field(
                title="Call number type map file name",
                description="File name for call number type map.",
            ),
        ]
        reset_hrid_settings: Annotated[
            Optional[bool],
            Field(
                title="Reset HRID settings",
                description=(
                    "At the end of the run "
                    "reset FOLIO with the HRID settings. "
                    "By default is False."
                ),
            ),
        ] = False
        update_hrid_settings: Annotated[
            bool,
            Field(
                title="Update HRID settings",
                description=(
                    "At the end of the run "
                    "update FOLIO with the HRID settings. "
                    "By default is True."
                ),
            ),
        ] = True
        boundwith_flavor: Annotated[
            IlsFlavour,
            Field(
                title="Boundwith flavor",
                description=(
                    "If boundwith relationships are present, this setting determines "
                    "the flavor of ILS-specific boundwith handling to be applied. "
                    "The default is 'voyager', meaning Voyager-specific handling will be applied. "
                    "Supported values are 'voyager' and 'aleph', and requires a properly "
                    "formatted boundwith relationship file to be provided."
                ),
            ),
        ] = "voyager"
        boundwith_relationship_file_path: Annotated[
            str,
            Field(
                title="Boundwith relationship file path",
                description=(
                    "Path to a file outlining Boundwith relationships, "
                    "in the style of Voyager. "
                    "A TSV file with MFHD_ID and BIB_ID headers and values (voyager-style) or "
                    "LKR_HOL and ITEM_REC_KEY headers and values (aleph-style). "
                    "Default is empty string."
                ),
            ),
        ] = ""
        prevent_permanent_location_map_default: Annotated[
            bool,
            Field(
                title="Prevent permanent location map default",
                description=(
                    "Prevent the default mapping of permanent location "
                    "to the default location. "
                    "Default is False."
                ),
            ),
        ] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.items

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        """Initialize ItemsTransformer for transforming item records.

        Args:
            task_config (TaskConfiguration): Items transformation configuration.
            library_config (LibraryConfiguration): Library configuration.
            folio_client: FOLIO API client.
            use_logging (bool): Whether to set up task logging.
        """
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.task_config = task_config
        self.task_configuration = self.task_config
        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_config.files
        )
        self.total_records = 0
        self.folio_keys = []
        self.items_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder / self.task_config.items_mapping_file_name
        )
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.items_map
        )
        if any(k for k in self.folio_keys if k.startswith("statisticalCodeIds")) or any(
            getattr(k, "statistical_code", "") for k in self.task_configuration.files
        ):
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
                self.folder_structure.mapping_files_folder
                / self.task_config.temp_loan_types_map_file_name,
                self.folio_keys,
            )
        else:
            logger.info(
                "%s not found. No temporary loan type mapping will be performed",
                self.folder_structure.temp_loan_type_map_path,
            )
            temporary_loan_type_mapping = None
        # Load Boundwith relationship map
        self.boundwith_relationship_map = {}
        if self.task_config.boundwith_relationship_file_path:
            self.load_boundwith_relationships()
        if (
            self.folder_structure.mapping_files_folder
            / self.task_config.temp_location_map_file_name
        ).is_file():
            temporary_location_mapping = self.load_ref_data_mapping_file(
                "temporaryLocationId",
                self.folder_structure.mapping_files_folder
                / self.task_config.temp_location_map_file_name,
                self.folio_keys,
            )
        else:
            logger.info(
                "%s not found. No temporary location mapping will be performed",
                self.task_config.temp_location_map_file_name,
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
            self.task_configuration,
        )
        self._validate_boundwith_relationships()
        if (
            self.task_configuration.reset_hrid_settings
            and self.task_configuration.update_hrid_settings
        ):
            hrid_handler = HRIDHandler(
                self.folio_client, HridHandling.default, self.mapper.migration_report, True
            )
            hrid_handler.reset_item_hrid_counter()

        logger.info("Init done")

    def do_work(self):
        logger.info("Starting....")
        with open(self.folder_structure.created_objects_path, "w+") as results_file:
            for file_def in self.task_config.files:
                try:
                    self.process_single_file(file_def, results_file)
                except Exception as exception:
                    error_str = f"\n\nProcessing of {file_def.file_name} failed:\n{exception}."
                    logger.exception(error_str, stack_info=True)
                    logger.fatal("Check source files for empty rows or missing reference data.")
                    self.mapper.migration_report.add(
                        "FailedFiles", f"{file_def.file_name} - {exception}"
                    )
                    logger.fatal(error_str)
                    sys.exit(1)
        logger.info(
            f"processed {self.total_records:,} records in {len(self.task_config.files)} files"
        )

    def handle_boundwith_parts(self, folio_rec, legacy_id):
        if (
            self.task_configuration.boundwith_flavor == IlsFlavour.voyager
            and self.boundwith_relationship_map
            and folio_rec["holdingsRecordId"] in self.boundwith_relationship_map
        ):
            for idx_, instance_id in enumerate(
                self.boundwith_relationship_map.get(folio_rec["holdingsRecordId"])
            ):
                if idx_ == 0:
                    bw_id = folio_rec["holdingsRecordId"]
                else:
                    bw_id = self.mapper.generate_boundwith_holding_uuid(
                        folio_rec["holdingsRecordId"], instance_id
                    )
                self.mapper.create_and_write_boundwith_part(legacy_id, bw_id)
        elif (
            self.task_configuration.boundwith_flavor == IlsFlavour.aleph
            and self.boundwith_relationship_map
            and legacy_id in self.boundwith_relationship_map
        ):
            for holdings_id in self.boundwith_relationship_map.get(legacy_id):
                self.mapper.create_and_write_boundwith_part(
                    legacy_id, self.mapper.holdings_id_map.get(holdings_id)[1]
                )

    def process_single_file(self, file_def: FileDefinition, results_file):
        full_path = self.folder_structure.legacy_records_folder / file_def.file_name
        logger.info("Processing %s", full_path)
        records_in_file = 0
        with open(full_path, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                i18n_t("Number of files processed")
            )
            start = time.time()
            for idx, record in enumerate(self.mapper.get_objects(records_file, full_path)):
                try:
                    if idx == 0:
                        logger.info("First legacy record:")
                        logger.info(json.dumps(record, indent=4))
                        self.mapper.verify_legacy_record(record, idx)
                    folio_rec, legacy_id = self.mapper.do_map(
                        record, f"row {idx}", FOLIONamespaces.items
                    )

                    self.mapper.perform_additional_mappings(legacy_id, folio_rec, file_def)
                    self.handle_circulation_notes(folio_rec, self.folio_client.current_user)
                    self.handle_notes(folio_rec)
                    self.handle_boundwith_parts(folio_rec, legacy_id)

                    if idx == 0:
                        logger.info("First FOLIO record:")
                        logger.info(json.dumps(folio_rec, indent=4))
                    # TODO: turn this into a asynchronous task
                    Helper.write_to_file(results_file, folio_rec)
                    self.mapper.migration_report.add_general_statistics(
                        i18n_t("Number of records written to disk")
                    )
                    self.mapper.report_folio_mapping(folio_rec, self.mapper.schema)
                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as data_error:
                    self.mapper.handle_transformation_record_failed_error(idx, data_error)
                except AttributeError as attribute_error:
                    traceback.print_exc()
                    logger.fatal(attribute_error)
                    logger.info("Quitting...")
                    sys.exit(1)
                except Exception as exception:
                    self.mapper.handle_generic_exception(idx, exception)
                self.mapper.migration_report.add(
                    "GeneralStatistics",
                    i18n.t("Number of Legacy items in %{container}", container=file_def),
                )
                self.mapper.migration_report.add_general_statistics(
                    i18n_t("Number of Legacy items in total")
                )
                self.print_progress(idx, start)
                records_in_file = idx + 1

            logger.info(
                f"Done processing {file_def} containing {records_in_file:,} records. "
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
    def handle_circulation_notes(folio_rec, current_user_uuid):
        if not folio_rec.get("circulationNotes", []):
            return
        filtered_notes = []
        for circ_note in folio_rec.get("circulationNotes", []):
            if circ_note.get("noteType", "") not in ["Check in", "Check out"]:
                raise TransformationProcessError(
                    "", "Circulation Note types are not mapped correctly"
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

    def load_boundwith_relationships(self):
        if self.task_configuration.boundwith_flavor == IlsFlavour.voyager:
            self._load_voyager_boundwith_relationships()
        elif self.task_configuration.boundwith_flavor == IlsFlavour.aleph:
            self._load_aleph_boundwith_relationships()
        else:
            raise TransformationProcessError(
                "",
                f"Unsupported boundwith flavor: {self.task_configuration.boundwith_flavor}. "
                f"Supported flavors are 'voyager' and 'aleph'.",
            )

    def _load_voyager_boundwith_relationships(self):
        try:
            with open(
                self.folder_structure.boundwith_relationships_map_path
            ) as boundwith_relationship_file:
                self.boundwith_relationship_map = dict(
                    json.loads(x) for x in boundwith_relationship_file
                )
            logger.info(
                "Rows in Bound with relationship map: %s", len(self.boundwith_relationship_map)
            )
        except FileNotFoundError as fnfe:
            raise TransformationProcessError(
                "",
                "Boundwith relationship file specified, but relationships file "
                "from holdings transformation not found.",
                self.folder_structure.boundwith_relationships_map_path,
            ) from fnfe
        except ValueError as ve:
            raise TransformationProcessError(
                "",
                "Boundwith relationship file specified, but relationships file "
                "from holdings transformation is not a valid line JSON.",
                self.folder_structure.boundwith_relationships_map_path,
            ) from ve

    def _load_aleph_boundwith_relationships(self):
        """Load Aleph-style boundwith relationships from TSV file.

        Loads raw relationship data into boundwith_relationship_map without
        validating against the holdings ID map. Validation is deferred to
        _validate_aleph_boundwith_relationships, which must be called after
        the mapper (and its holdings_id_map) is available.
        """
        try:
            with open(
                self.folder_structure.legacy_records_folder
                / self.task_configuration.boundwith_relationship_file_path
            ) as boundwith_relationship_file:
                for line in csv.DictReader(boundwith_relationship_file, delimiter="\t"):
                    mfhd_id = line.get("LKR_HOL", "").strip()
                    item_legacy_id = line.get("ITEM_REC_KEY", "").strip()
                    holdings_ids = self.boundwith_relationship_map.get(item_legacy_id, set())
                    holdings_ids.add(mfhd_id)
                    self.boundwith_relationship_map[item_legacy_id] = holdings_ids
        except FileNotFoundError as fnfe:
            raise TransformationProcessError(
                "",
                "Boundwith relationship file specified, but file not found.",
                self.task_config.boundwith_relationship_file_path,
            ) from fnfe
        except Exception as exception:
            raise TransformationProcessError(
                "",
                f"An error occurred while loading the boundwith relationship file. {exception}",
                self.task_config.boundwith_relationship_file_path,
            ) from exception

    def _validate_boundwith_relationships(self):
        """Validate boundwith relationships against the holdings ID map.

        For Aleph-flavor boundwiths, removes any holdings IDs from the
        boundwith_relationship_map that are not present in the mapper's
        holdings_id_map, and logs data issues for each missing ID.
        No-op for other flavors. Must be called after the mapper is instantiated.
        """
        if self.task_configuration.boundwith_flavor != IlsFlavour.aleph:
            return
        for item_legacy_id, holdings_ids in self.boundwith_relationship_map.items():
            valid_ids = set()
            for mfhd_id in holdings_ids:
                if mfhd_id not in self.mapper.holdings_id_map:
                    Helper.log_data_issue_failed(
                        item_legacy_id,
                        "Holdings for boundwith relationship not found in holdings id map",
                        mfhd_id,
                    )
                else:
                    valid_ids.add(mfhd_id)
            if valid_ids:
                self.boundwith_relationship_map[item_legacy_id] = valid_ids
            else:
                del self.boundwith_relationship_map[item_legacy_id]

    def wrap_up(self):
        logger.info("Done. Transformer wrapping up...")
        self.extradata_writer.flush()
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            self.mapper.migration_report.write_migration_report(
                i18n_t("Item transformation report"),
                migration_report_file,
                self.mapper.start_datetime,
            )
            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        with open(self.folder_structure.migration_reports_raw_file, "w") as raw_report_file:
            self.mapper.migration_report.write_json_report(raw_report_file)
        self.clean_out_empty_logs()
        logger.info("All done!")
