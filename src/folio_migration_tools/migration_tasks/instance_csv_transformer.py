"""Delimited instance records transformation task.

Transforms instance records from CSV/TSV files to FOLIO Instance records using a
mapping file. Produces instances, an instance id map and a migration report. No
SRS/MARC records are created.
"""

import csv
import ctypes
import json
import logging
import sys
import time
import traceback
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
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.instance_mapper import InstanceMapper
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration

logger = logging.getLogger(__name__)

csv.field_size_limit((ctypes.c_ulong(-1).value // 2))


class InstanceCsvTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        """Task configuration for InstanceCsvTransformer."""

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
        files: Annotated[
            List[FileDefinition],
            Field(
                title="Files",
                description="List of delimited files containing the legacy instances.",
            ),
        ]
        instances_mapping_file_name: Annotated[
            str,
            Field(
                title="Instances mapping file name",
                description=(
                    "File name for the instances mapping. Must map legacyIdentifier and a title."
                ),
            ),
        ]
        instance_types_map_file_name: Annotated[
            str,
            Field(
                title="Instance types map file name",
                description=(
                    "File name for the instance type map. A TSV file with a legacy value "
                    "column and a folio_name column."
                ),
            ),
        ]
        nature_of_content_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Nature of content map file name",
                description="Optional file name for the nature of content term map.",
            ),
        ] = ""
        instance_formats_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Instance formats map file name",
                description="Optional file name for the instance format map.",
            ),
        ] = ""
        modes_of_issuance_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Modes of issuance map file name",
                description="Optional file name for the mode of issuance map.",
            ),
        ] = ""
        contributor_types_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Contributor types map file name",
                description="Optional file name for the contributor type map.",
            ),
        ] = ""
        identifier_types_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Identifier types map file name",
                description=(
                    "Optional file name for an identifier type mapping table. A TSV with "
                    "legacy_identifier_type and folio_name columns. When absent, values are "
                    "resolved directly as FOLIO names or UUIDs."
                ),
            ),
        ] = ""
        contributor_name_types_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Contributor name types map file name",
                description=(
                    "Optional file name for a contributor name type mapping table. A TSV with "
                    "legacy_contributor_name_type and folio_name columns. When absent, values "
                    "are resolved directly as FOLIO names or UUIDs."
                ),
            ),
        ] = ""
        instance_note_types_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Instance note types map file name",
                description=(
                    "Optional file name for an instance note type mapping table. A TSV with "
                    "legacy_note_type and folio_name columns. When absent, values are "
                    "resolved directly as FOLIO names or UUIDs."
                ),
            ),
        ] = ""
        statistical_codes_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Statistical code map file name",
                description=(
                    "Optional file name for the statistical code map. The file should be "
                    "in TSV format with legacy_stat_code and folio_code columns."
                ),
            ),
        ] = ""
        add_administrative_notes_with_legacy_ids: Annotated[
            bool,
            Field(
                title="Add administrative notes with legacy IDs",
                description=(
                    "If set to true, an administrative note containing the legacy ID is "
                    "added to each instance, protecting the value from overlays."
                ),
            ),
        ] = True

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.instances

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        """Initialize InstanceCsvTransformer for transforming delimited instances.

        Args:
            task_config (TaskConfiguration): Instance transformation configuration.
            library_config (LibraryConfiguration): Library configuration.
            folio_client: FOLIO API client.
            use_logging (bool): Whether to set up task logging.
        """
        csv.register_dialect("tsv", delimiter="\t")
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.task_config = task_config
        self.check_source_files(
            self.folder_structure.legacy_records_folder, self.task_config.files
        )
        self.total_records = 0
        self.instance_id_map: dict = {}
        self.instances_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder
            / self.task_config.instances_mapping_file_name
        )
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.instances_map
        )
        if any(k for k in self.folio_keys if k.startswith("statisticalCodeIds")) or any(
            getattr(k, "statistical_code", "") for k in self.task_config.files
        ):
            statcode_mapping = self.load_ref_data_mapping_file(
                "statisticalCodeIds",
                self.folder_structure.mapping_files_folder
                / (self.task_config.statistical_codes_map_file_name or ""),
                self.folio_keys,
                False,
            )
        else:
            statcode_mapping = None
        self.mapper = InstanceMapper(
            self.folio_client,
            self.instances_map,
            self.load_ref_data_mapping_file(
                "instanceTypeId",
                self.folder_structure.mapping_files_folder
                / self.task_config.instance_types_map_file_name,
                self.folio_keys,
            ),
            self.library_configuration,
            self.task_config,
            statcode_mapping,
            self._load_optional_map(
                "natureOfContentTermIds", self.task_config.nature_of_content_map_file_name
            ),
            self._load_optional_map(
                "instanceFormatIds", self.task_config.instance_formats_map_file_name
            ),
            self._load_optional_map(
                "modeOfIssuanceId", self.task_config.modes_of_issuance_map_file_name
            ),
            self._load_optional_map(
                "contributors.contributorTypeId", self.task_config.contributor_types_map_file_name
            ),
            self._load_optional_map(
                "identifiers.identifierTypeId", self.task_config.identifier_types_map_file_name
            ),
            self._load_optional_map(
                "contributors.contributorNameTypeId",
                self.task_config.contributor_name_types_map_file_name,
            ),
            self._load_optional_map(
                "notes.instanceNoteTypeId", self.task_config.instance_note_types_map_file_name
            ),
        )
        logger.info("Init done")

    def _load_optional_map(self, folio_property_name: str, file_name: Optional[str]):
        if not file_name:
            return None
        return self.load_ref_data_mapping_file(
            folio_property_name,
            self.folder_structure.mapping_files_folder / file_name,
            self.folio_keys,
        )

    async def do_work(self):
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
                        record, f"row {idx}", FOLIONamespaces.instances
                    )
                    self.mapper.perform_additional_mappings(legacy_id, folio_rec, file_def)
                    self.handle_notes(folio_rec)
                    if idx == 0:
                        logger.info("First FOLIO record:")
                        logger.info(json.dumps(folio_rec, indent=4))
                    self.instance_id_map[legacy_id] = self.get_id_map_entry(legacy_id, folio_rec)
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
                    i18n.t(
                        "Number of Legacy items in %{container}",
                        container=file_def.file_name,
                    ),
                )
                self.mapper.migration_report.add_general_statistics(
                    i18n_t("Number of Legacy items in total")
                )
                self.print_progress(idx, start)
                records_in_file = idx + 1
            logger.info(
                f"Done processing {file_def.file_name} containing {records_in_file:,} records. "
                f"Total records processed: {records_in_file:,}"
            )
        self.total_records += records_in_file

    @staticmethod
    def get_id_map_entry(legacy_id: str, folio_rec: dict) -> tuple:
        """Build the id map entry; the HRID is included only when it has been mapped."""
        if folio_rec.get("hrid"):
            return (legacy_id, folio_rec["id"], folio_rec["hrid"])
        return (legacy_id, folio_rec["id"])

    @staticmethod
    def handle_notes(folio_object):
        """Drop empty notes and fail records with notes lacking a note type."""
        if not folio_object.get("notes"):
            return
        filtered_notes = []
        for note_obj in folio_object["notes"]:
            if not note_obj.get("instanceNoteTypeId", ""):
                raise TransformationProcessError(
                    folio_object.get("id", ""),
                    "Missing note type id mapping",
                    json.dumps(note_obj),
                )
            if note_obj.get("note", ""):
                filtered_notes.append(note_obj)
        if filtered_notes:
            folio_object["notes"] = filtered_notes
        else:
            del folio_object["notes"]

    async def wrap_up(self):
        logger.info("Done. Transformer wrapping up...")
        self.extradata_writer.flush()
        self.mapper.save_id_map_file(
            self.folder_structure.instance_id_map_path, self.instance_id_map
        )
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            self.mapper.migration_report.write_migration_report(
                i18n_t("Instance transformation report"),
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
