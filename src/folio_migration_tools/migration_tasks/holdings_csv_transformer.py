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
from httpx import HTTPError
from pydantic import Field

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.holdings_helper import HoldingsHelper
from folio_migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.holdings_mapper import (
    HoldingsMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.marc_rules_transformation.hrid_handler import HRIDHandler
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
csv.register_dialect("tsv", delimiter="\t")


class HoldingsCsvTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: Annotated[
            str,
            Field(
                title="Task name",
                description="Name of the task",
            ),
        ]
        migration_task_type: Annotated[
            str,
            Field(
                title="Migration task type",
                description="Type of migration task",
            ),
        ]
        hrid_handling: Annotated[
            HridHandling,
            Field(
                title="HRID handling",
                description=(
                    "Determining how the HRID generation "
                    "should be handled."
                ),
            ),
        ]
        files: Annotated[
            List[FileDefinition],
            Field(
                title="Files",
                description="List of files",
            ),
        ]
        holdings_map_file_name: Annotated[
            str,
            Field(
                title="Holdings map file name",
                description="File name for holdings map",
            ),
        ]
        location_map_file_name: Annotated[
            str,
            Field(
                title="Location map file name",
                description="File name for location map",
            ),
        ]
        default_call_number_type_name: Annotated[
            str,
            Field(
                title="Default call number type name",
                description="Default name for call number type",
            ),
        ]
        previously_generated_holdings_files: Annotated[
            Optional[list[str]],
            Field(
                title="Previously generated holdings files",
                description=(
                    "List of previously generated holdings files. "
                    "By default is empty list."
                ),
            ),
        ] = []
        fallback_holdings_type_id: Annotated[
            str,
            Field(
                title="Fallback holdings type ID",
                description="ID for fallback holdings type",
            ),
        ]
        holdings_type_uuid_for_boundwiths: Annotated[
            str,
            Field(
                title="Holdings Type for Boundwith Holdings",
                description=(
                    "UUID for a Holdings type (set in Settings->Inventory) "
                    "for Bound-with Holdings. Default is empty string."
                ),
            ),
        ] = ""
        call_number_type_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Call number type map file name",
                description="File name for call number type map",
            ),
        ]
        holdings_merge_criteria: Annotated[
            Optional[list[str]],
            Field(
                title="Holdings merge criteria",
                description=(
                    "List of holdings merge criteria. "
                    "Default value is "
                    "['instanceId', 'permanentLocationId', 'callNumber']."
                ),
            ),
        ] = [
            "instanceId",
            "permanentLocationId",
            "callNumber",
        ]
        reset_hrid_settings: Annotated[
            Optional[bool],
            Field(
                title="Reset HRID settings",
                description=(
                    "At the end of the run reset "
                    "FOLIO with the HRID settings. Default is FALSE."
                ),
            ),
        ] = False
        update_hrid_settings: Annotated[
            bool,
            Field(
                title="Update HRID settings",
                description=(
                    "At the end of the run update "
                    "FOLIO with the HRID settings. Default is TRUE."
                ),
            ),
        ] = True
        statistical_codes_map_file_name: Annotated[
            Optional[str],
            Field(
                title="Statistical code map file name",
                description=(
                    "Path to the file containing the mapping of statistical codes. "
                    "The file should be in TSV format with legacy_stat_code and folio_code columns."
                ),
            ),
        ] = ""

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.holdings

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.fallback_holdings_type = None
        self.folio_keys, self.holdings_field_map = self.load_mapped_fields()
        if any(k for k in self.folio_keys if k.startswith("statisticalCodeIds")):
            statcode_mapping = self.load_ref_data_mapping_file(
                "statisticalCodeIds",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.statistical_codes_map_file_name,
                self.folio_keys,
                False,
            )
        else:
            statcode_mapping = None
        try:
            self.bound_with_keys = set()
            self.mapper = HoldingsMapper(
                self.folio_client,
                self.holdings_field_map,
                self.load_location_map(),
                self.load_call_number_type_map(),
                self.load_instance_id_map(True),
                library_config,
                statcode_mapping,
            )
            self.holdings = {}
            self.total_records = 0
            self.holdings_id_map = self.load_id_map(self.folder_structure.holdings_id_map_path)
            self.results_path = self.folder_structure.created_objects_path
            self.holdings_types = list(
                self.folio_client.folio_get_all("/holdings-types", "holdingsTypes")
            )
            logging.info("%s\tholdings types in tenant", len(self.holdings_types))
            self.validate_merge_criterias()
            self.check_source_files(
                self.folder_structure.data_folder / "items", self.task_configuration.files
            )
            self.fallback_holdings_type = next(
                h
                for h in self.holdings_types
                if h["id"] == self.task_configuration.fallback_holdings_type_id
            )
            if not self.fallback_holdings_type:
                raise TransformationProcessError(
                    "",
                    (
                        "Holdings type with ID "
                        f"{self.task_configuration.fallback_holdings_type_id} "
                        "not found in FOLIO."
                    ),
                )
            logging.info(
                "%s will be used as default holdings type",
                self.fallback_holdings_type["name"],
            )
            if any(self.task_configuration.previously_generated_holdings_files):
                for file_name in self.task_configuration.previously_generated_holdings_files:
                    logging.info("Processing %s", file_name)
                    self.holdings.update(
                        HoldingsHelper.load_previously_generated_holdings(
                            self.folder_structure.results_folder / file_name,
                            self.task_configuration.holdings_merge_criteria,
                            self.mapper.migration_report,
                            self.task_configuration.holdings_type_uuid_for_boundwiths,
                        )
                    )

            else:
                logging.info("No file of legacy holdings setup.")

            if (
                self.task_configuration.reset_hrid_settings
                and self.task_configuration.update_hrid_settings
            ):
                hrid_handler = HRIDHandler(
                    self.folio_client, HridHandling.default, self.mapper.migration_report, True
                )
                hrid_handler.reset_holdings_hrid_counter()

        except HTTPError as http_error:
            logging.critical(http_error)
            sys.exit(1)
        except (FileNotFoundError, TransformationProcessError) as process_error:
            logging.critical(process_error)
            logging.critical("Halting.")
            sys.exit(1)
        except json.JSONDecodeError as jde:
            raise jde
        except Exception as exception:
            logging.info("\n=======ERROR===========")
            logging.info(exception)
            logging.info("\n=======Stack Trace===========")
            traceback.print_exc()
            sys.exit(1)
        logging.info("Init done")

    def load_call_number_type_map(self):
        with open(
            self.folder_structure.mapping_files_folder
            / self.task_configuration.call_number_type_map_file_name,
            "r",
        ) as callnumber_type_map_f:
            return self.load_ref_data_map_from_file(
                callnumber_type_map_f, "Found %s rows in call number type map"
            )

    def load_location_map(self):
        with open(
            self.folder_structure.mapping_files_folder / self.task_configuration.location_map_file_name
        ) as location_map_f:
            return self.load_ref_data_map_from_file(
                location_map_f, "Found %s rows in location map"
            )

    # TODO Rename this here and in `load_call_number_type_map` and `load_location_map`
    def load_ref_data_map_from_file(self, file, message):
        ref_dat_map = list(csv.DictReader(file, dialect="tsv"))
        logging.info(message, len(ref_dat_map))
        return ref_dat_map

    def load_mapped_fields(self):
        with open(
            self.folder_structure.mapping_files_folder / self.task_configuration.holdings_map_file_name
        ) as holdings_mapper_f:
            holdings_map = json.load(holdings_mapper_f)
            logging.info("%s fields in holdings mapping file map", len(holdings_map["data"]))
            mapped_fields = MappingFileMapperBase.get_mapped_folio_properties_from_map(
                holdings_map
            )
            logging.info(
                "%s mapped fields in holdings mapping file map",
                len(list(mapped_fields)),
            )
            return mapped_fields, holdings_map

    def do_work(self):
        logging.info("Starting....")
        for file_def in self.task_configuration.files:
            logging.info("Processing %s", file_def.file_name)
            try:
                self.process_single_file(file_def)
            except Exception as ee:
                error_str = (
                    f"Processing of {file_def.file_name} failed:\n{ee}."
                    "\nCheck source files for empty rows or missing reference data"
                )
                logging.critical(error_str)
                print(f"\n{error_str}\nHalting")
                sys.exit(1)
        logging.info(
            f"processed {self.total_records:,} records in {len(self.task_configuration.files)} files"
        )

    def wrap_up(self):
        logging.info("Done. Transformer wrapping up...")
        self.extradata_writer.flush()
        if any(self.holdings):
            logging.info(
                "Saving holdings created to %s",
                self.folder_structure.created_objects_path,
            )
            with open(self.folder_structure.created_objects_path, "w+") as holdings_file:
                for holding in self.holdings.values():
                    for legacy_id in holding["formerIds"]:
                        # Prevent the first item in a boundwith to be overwritten
                        # TODO: Find out why not
                        # if legacy_id not in self.holdings_id_map:
                        self.holdings_id_map[legacy_id] = self.mapper.get_id_map_tuple(
                            legacy_id, holding, self.object_type
                        )
                    Helper.write_to_file(holdings_file, holding)
                    self.mapper.migration_report.add_general_statistics(
                        i18n.t("Holdings Records Written to disk")
                    )
            self.mapper.save_id_map_file(
                self.folder_structure.holdings_id_map_path, self.holdings_id_map
            )
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            self.mapper.migration_report.write_migration_report(
                i18n.t("Holdings transformation report"),
                migration_report_file,
                self.mapper.start_datetime,
            )
            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        logging.info("All done!")
        self.clean_out_empty_logs()

    def validate_merge_criterias(self):
        holdings_schema = self.folio_client.get_holdings_schema()
        properties = holdings_schema["properties"].keys()
        logging.info(properties)
        logging.info(self.task_configuration.holdings_merge_criteria)
        res = [mc for mc in self.task_configuration.holdings_merge_criteria if mc not in properties]
        if any(res):
            logging.critical(
                (
                    "Merge criteria(s) is not a property of a holdingsrecord: %s"
                    "check the merge criteria names and try again"
                ),
                ", ".join(res),
            )
            sys.exit(1)

    def process_single_file(self, file_def: FileDefinition):
        full_path = self.folder_structure.data_folder / "items" / file_def.file_name
        with open(full_path, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                i18n.t("Number of files processed")
            )
            start = time.time()
            records_processed = 0
            for idx, legacy_record in enumerate(self.mapper.get_objects(records_file, full_path)):
                records_processed = idx + 1
                try:
                    self.mapper.verify_legacy_record(legacy_record, idx)
                    folio_rec, legacy_id = self.mapper.do_map(
                        legacy_record, f"row # {idx}", FOLIONamespaces.holdings
                    )
                    self.post_process_holding(folio_rec, legacy_id, file_def)
                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as error:
                    self.mapper.handle_transformation_record_failed_error(idx, error)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("Number of Legacy items in file")
                )
                if idx > 1 and idx % 10000 == 0:
                    elapsed = idx / (time.time() - start)
                    elapsed_formatted = "{0:.4g}".format(elapsed)
                    logging.info(f"{idx:,} records processed. Recs/sec: {elapsed_formatted} ")
            self.total_records = records_processed
            logging.info(
                f"Done processing {file_def.file_name} containing {self.total_records:,} records. "
                f"Total records processed: {self.total_records:,}"
            )

    def post_process_holding(self, folio_rec: dict, legacy_id: str, file_def: FileDefinition):
        HoldingsHelper.handle_notes(folio_rec)
        HoldingsHelper.remove_empty_holdings_statements(folio_rec)

        if not folio_rec.get("holdingsTypeId", ""):
            folio_rec["holdingsTypeId"] = self.fallback_holdings_type["id"]

        holdings_from_row = []
        all_instance_ids = folio_rec.get("instanceId", [])
        if len(all_instance_ids) == 1:
            # Normal case.
            folio_rec["instanceId"] = folio_rec["instanceId"][0]
            holdings_from_row.append(folio_rec)

        elif len(folio_rec.get("instanceId", [])) > 1:  # Bound-with.
            holdings_from_row.extend(self.create_bound_with_holdings(folio_rec, legacy_id))
        else:
            raise TransformationRecordFailedError(legacy_id, "No instance id in parsed record", "")

        for folio_holding in holdings_from_row:
            self.mapper.perform_additional_mappings(legacy_id, folio_holding, file_def)
            self.merge_holding_in(folio_holding, all_instance_ids, legacy_id)
        self.mapper.report_folio_mapping(folio_holding, self.mapper.schema)

    def create_bound_with_holdings(self, folio_holding, legacy_id: str):
        folio_holding["formerIds"] = explode_former_ids(folio_holding)
        return list(
            self.mapper.create_bound_with_holdings(
                folio_holding,
                folio_holding["instanceId"],
                self.task_configuration.holdings_type_uuid_for_boundwiths,
            )
        )

    def merge_holding_in(
        self, incoming_holding: dict, instance_ids: list[str], legacy_item_id: str
    ) -> None:
        """Determines what newly generated holdingsrecords are to be merged with
        previously created ones. When that is done, it generates the correct boundwith
        parts needed.

        Args:
            incoming_holding (dict): The newly created FOLIO Holding
            instance_ids (list): the instance IDs tied to the current item
            legacy_item_id (str): Id of the Item the holding was generated from
        """
        if len(instance_ids) > 1:
            # Is boundwith
            bw_key = (
                f"bw_{incoming_holding['instanceId']}_{incoming_holding['permanentLocationId']}_"
                f"{incoming_holding.get('callNumber', '')}_{'_'.join(sorted(instance_ids))}"
            )
            if bw_key not in self.bound_with_keys:
                self.bound_with_keys.add(bw_key)
                self.holdings[bw_key] = incoming_holding
                self.mapper.create_and_write_boundwith_part(legacy_item_id, incoming_holding["id"])
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("Unique BW Holdings created from Items")
                )
            else:
                self.merge_holding(bw_key, incoming_holding)
                self.mapper.create_and_write_boundwith_part(
                    legacy_item_id, self.holdings[bw_key]["id"]
                )
                self.holdings_id_map[legacy_item_id] = self.mapper.get_id_map_tuple(
                    legacy_item_id, self.holdings[bw_key], self.object_type
                )
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("BW Items found tied to previously created BW Holdings")
                )
        else:
            # Regular holding. Merge according to criteria
            new_holding_key = HoldingsHelper.to_key(
                incoming_holding,
                self.task_configuration.holdings_merge_criteria,
                self.mapper.migration_report,
                self.task_configuration.holdings_type_uuid_for_boundwiths,
            )
            if self.holdings.get(new_holding_key, None):
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("Holdings already created from Item")
                )
                self.merge_holding(new_holding_key, incoming_holding)
            else:
                self.mapper.migration_report.add_general_statistics(
                    i18n.t("Unique Holdings created from Items")
                )
                self.holdings[new_holding_key] = incoming_holding

    def merge_holding(self, holdings_key: str, new_holdings_record: dict):
        self.holdings[holdings_key] = HoldingsHelper.merge_holding(
            self.holdings[holdings_key], new_holdings_record
        )


def explode_former_ids(folio_holding: dict):
    temp_ids = []
    for former_id in folio_holding.get("formerIds", []):
        if former_id.startswith("[") and former_id.endswith("]") and "," in former_id:
            ids = list(
                former_id[1:-1].replace('"', "").replace(" ", "").replace("'", "").split(",")
            )
            temp_ids.extend(ids)
        else:
            temp_ids.append(former_id)
    return temp_ids
