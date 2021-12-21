'''Main "script."'''
import ast
import copy
import csv
import ctypes
import json
import logging
import os
import sys
import time
import traceback
import uuid
from os import listdir
from os.path import isfile
from typing import List, Optional
from folio_uuid import FolioUUID

from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.helper import Helper
from migration_tools.library_configuration import (
    FileDefinition,
    HridHandling,
    LibraryConfiguration,
)
from migration_tools.mapping_file_transformation.holdings_mapper import HoldingsMapper
from migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from migration_tools.report_blurbs import Blurbs
from pydantic.main import BaseModel

from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
csv.register_dialect("tsv", delimiter="\t")


class HoldingsCsvTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        migration_task_type: str
        hrid_handling: HridHandling
        files: List[FileDefinition]
        holdings_map_file_name: str
        location_map_file_name: str
        default_call_number_type_name: str
        default_holdings_type_id: str
        holdings_type_uuid_for_boundwiths: Optional[str]
        call_number_type_map_file_name: Optional[str]
        holdings_merge_criteria: Optional[str] = "clb"

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.holdings

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
    ):
        super().__init__(library_config, task_config)
        self.default_holdings_type = None
        try:
            self.task_config = task_config
            self.files = self.list_source_files()
            self.mapper = HoldingsMapper(
                self.folio_client,
                self.load_mapped_fields(),
                self.load_location_map(),
                self.load_call_number_type_map(),
                self.load_id_map(self.folder_structure.instance_id_map_path),
            )
            self.holdings = {}
            self.total_records = 0
            self.holdings_id_map = self.load_id_map(
                self.folder_structure.holdings_id_map_path
            )
            if "_" in self.task_config.holdings_merge_criteria:
                self.excluded_hold_type_id = (
                    self.task_config.holdings_merge_criteria.split("_")[-1]
                )
                logging.info(self.excluded_hold_type_id)

            self.results_path = self.folder_structure.created_objects_path
            self.failed_files: List[str] = list()
            self.holdings_types = list(
                self.folio_client.folio_get_all("/holdings-types", "holdingsTypes")
            )
            logging.info("%s\tholdings types in tenant", len(self.holdings_types))

            self.default_holdings_type = next(
                h
                for h in self.holdings_types
                if h["id"] == self.task_config.default_holdings_type_id
            )
            if not self.default_holdings_type:
                raise TransformationProcessError(
                    (
                        "Holdings type with ID "
                        f"{self.task_config.default_holdings_type_id} "
                        "not found in FOLIO."
                    )
                )
            logging.info(
                "%s will be used as default holdings type",
                self.default_holdings_type["name"],
            )
        except TransformationProcessError as process_error:
            logging.critical(process_error)
            logging.critical("Halting.")
            sys.exit()
        except Exception as exception:
            logging.info("\n=======ERROR===========")
            logging.info(exception)
            logging.info("\n=======Stack Trace===========")
            traceback.print_exc()
        logging.info("Init done")

    def load_call_number_type_map(self):
        with open(
            self.folder_structure.mapping_files_folder
            / self.task_config.call_number_type_map_file_name,
            "r",
        ) as callnumber_type_map_f:
            return self.load_ref_data_map_from_file(
                callnumber_type_map_f, "Found %s rows in call number type map"
            )

    def load_location_map(self):
        with open(
            self.folder_structure.mapping_files_folder
            / self.task_config.location_map_file_name
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
            self.folder_structure.mapping_files_folder
            / self.task_config.holdings_map_file_name
        ) as holdings_mapper_f:
            holdings_map = json.load(holdings_mapper_f)
            logging.info(
                "%s fields in holdings mapping file map", len(holdings_map["data"])
            )
            mapped_fields = MappingFileMapperBase.get_mapped_folio_properties_from_map(
                holdings_map
            )
            logging.info(
                "%s mapped fields in holdings mapping file map",
                len(list(mapped_fields)),
            )
            return holdings_map

    def list_source_files(self):
        # Source data files
        files = [
            self.folder_structure.data_folder / "items" / f.file_name
            for f in self.task_config.files
            if isfile(self.folder_structure.data_folder / "items" / f.file_name)
        ]
        if not any(files):
            ret_str = ",".join(f.file_name for f in self.task_config.files)
            raise TransformationProcessError(
                f"Files {ret_str} not found in {self.folder_structure.data_folder / 'items'}"
            )
        logging.info("Files to process:")
        for filename in files:
            logging.info("\t%s", filename)
        return files

    def load_instance_id_map(self):
        res = {}
        with open(
            self.folder_structure.instance_id_map_path, "r"
        ) as instance_id_map_file:
            for index, json_string in enumerate(instance_id_map_file):
                # Format:{"legacy_id", "folio_id","instanceLevelCallNumber"}
                if index % 100000 == 0:
                    print(f"{index} instance ids loaded to map", end="\r")
                map_object = json.loads(json_string)
                res[map_object["legacy_id"]] = map_object
        logging.info("Loaded %s migrated instance IDs", (index + 1))
        return res

    def do_work(self):
        logging.info("Starting....")
        for file_name in self.files:
            logging.info("Processing %s", file_name)
            try:
                self.process_single_file(file_name)
            except Exception as ee:
                error_str = (
                    f"Processing of {file_name} failed:\n{ee}."
                    "Check source files for empty lines or missing reference data"
                )
                logging.exception(error_str)
                self.mapper.migration_report.add(
                    Blurbs.FailedFiles, f"{file_name} - {ee}"
                )
                sys.exit()
        logging.info(  # pylint: disable=logging-fstring-interpolation
            f"processed {self.total_records:,} records in {len(self.files)} files"
        )

    def process_single_file(self, file_name):
        with open(file_name, encoding="utf-8-sig") as records_file:
            self.mapper.migration_report.add_general_statistics(
                "Number of files processed"
            )
            start = time.time()
            records_processed = 0
            for idx, record in enumerate(
                self.mapper.get_objects(records_file, file_name)
            ):
                records_processed = idx + 1
                try:
                    self.process_holding(idx, record)

                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as error:
                    self.mapper.handle_transformation_record_failed_error(idx, error)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)
                self.mapper.migration_report.add_general_statistics(
                    "Number of Legacy items in file"
                )
                if idx > 1 and idx % 10000 == 0:
                    elapsed = idx / (time.time() - start)
                    elapsed_formatted = "{0:.4g}".format(elapsed)
                    logging.info(  # pylint: disable=logging-fstring-interpolation
                        f"{idx:,} records processed. Recs/sec: {elapsed_formatted} "
                    )
            self.total_records = records_processed
            logging.info(  # pylint: disable=logging-fstring-interpolation
                f"Done processing {file_name} containing {self.total_records:,} records. "
                f"Total records processed: {self.total_records:,}"
            )

    def process_holding(self, idx, row):
        folio_rec, legacy_id = self.mapper.do_map(
            row, f"row # {idx}", FOLIONamespaces.holdings
        )
        folio_rec["holdingsTypeId"] = self.default_holdings_type["id"]
        holdings_from_row = []
        if len(folio_rec.get("instanceId", [])) == 1:  # Normal case.
            folio_rec["instanceId"] = folio_rec["instanceId"][0]
            holdings_from_row.append(folio_rec)
        elif len(folio_rec.get("instanceId", [])) > 1:  # Bound-with.
            holdings_from_row.extend(
                self.create_bound_with_holdings(folio_rec, legacy_id)
            )
        else:
            raise TransformationRecordFailedError(
                legacy_id, "No instance id in parsed record", ""
            )
        for folio_holding in holdings_from_row:
            self.merge_holding_in(folio_holding)
        self.mapper.report_folio_mapping(folio_holding, self.mapper.schema)

    def create_bound_with_holdings(self, folio_holding, legacy_id: str):
        # Add former ids
        temp_ids = []
        for former_id in folio_holding.get("formerIds", []):
            if (
                former_id.startswith("[")
                and former_id.endswith("]")
                and "," in former_id
            ):
                ids = (
                    former_id[1:-1]
                    .replace('"', "")
                    .replace(" ", "")
                    .replace("'", "")
                    .split(",")
                )
                temp_ids.extend(ids)
            else:
                temp_ids.append(former_id)
        folio_holding["formerIds"] = temp_ids
        for bwidx, instance_id in enumerate(folio_holding["instanceId"]):
            if not instance_id:
                raise ValueError(f"No ID for record {folio_holding}")
            call_numbers = ast.literal_eval(folio_holding["callNumber"])
            if isinstance(call_numbers, str):
                call_numbers = [call_numbers]
            bound_with_holding = copy.deepcopy(folio_holding)
            bound_with_holding["instanceId"] = instance_id
            bound_with_holding["callNumber"] = call_numbers[bwidx]
            if not self.task_config.holdings_type_uuid_for_boundwiths:
                raise TransformationProcessError(
                    "",
                    (
                        "Boundwith UUID not added to task configuration."
                        "Add a property to holdingsTypeUuidForBoundwiths to "
                        "the task configuration"
                    ),
                    "",
                )
            bound_with_holding[
                "holdingsTypeId"
            ] = self.task_config.holdings_type_uuid_for_boundwiths
            bound_with_holding["id"] = str(
                FolioUUID(
                    FOLIONamespaces.holdings, f'{folio_holding["id"]}-{instance_id}'
                )
            )
            self.generate_boundwith_part(legacy_id, bound_with_holding)
            self.mapper.migration_report.add_general_statistics(
                "Bound-with holdings created"
            )
            yield bound_with_holding

    @staticmethod
    def generate_boundwith_part(self, legacy_id, bound_with_holding):
        part = {
            "id": str(uuid.uuid4()),
            "holdingsRecordId": bound_with_holding["id"],
            "itemId": FolioUUID(
                self.folio_client.okapi_url,
                FOLIONamespaces.items,
                legacy_id,
            ),
        }
        logging.log(25, f"boundwithPart\t{json.dumps(part)}")

    def wrap_up(self):
        logging.info("Done. Wrapping up...")
        if any(self.holdings):
            logging.info(
                "Saving holdings created to %s",
                self.folder_structure.created_objects_path,
            )
            with open(
                self.folder_structure.created_objects_path, "w+"
            ) as holdings_file:
                for holding in self.holdings.values():
                    for legacy_id in holding["formerIds"]:
                        # Prevent the first item in a boundwith to be overwritten
                        if legacy_id not in self.holdings_id_map:
                            self.holdings_id_map[
                                legacy_id
                            ] = self.mapper.get_id_map_string(legacy_id, holding)

                    Helper.write_to_file(holdings_file, holding)
                    self.mapper.migration_report.add_general_statistics(
                        "Holdings Records Written to disk"
                    )
            self.mapper.save_id_map_file(
                self.folder_structure.holdings_id_map_path, self.holdings_id_map
            )
        with open(
            self.folder_structure.migration_reports_file, "w"
        ) as migration_report_file:
            logging.info(
                "Writing migration- and mapping report to %s",
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

    def merge_holding_in(self, folio_holding):
        new_holding_key = self.to_key(
            folio_holding, self.task_config.holdings_merge_criteria
        )
        existing_holding = self.holdings.get(new_holding_key, None)
        exclude = (
            self.task_config.holdings_merge_criteria.startswith("u_")
            and folio_holding["holdingsTypeId"] == self.excluded_hold_type_id
        )
        if exclude or not existing_holding:
            self.mapper.migration_report.add_general_statistics(
                "Unique Holdings created from Items"
            )
            self.holdings[new_holding_key] = folio_holding
        else:
            self.mapper.migration_report.add_general_statistics(
                "Holdings already created from Item"
            )
            self.merge_holding(new_holding_key, existing_holding, folio_holding)

    @staticmethod
    def to_key(holding, fields_criteria):
        """creates a key if key values in holding record
        to determine uniquenes"""
        try:
            # creates a key of key values in holding record to determine uniquenes
            call_number = (
                "".join(holding.get("callNumber", "").split())
                if "c" in fields_criteria
                else ""
            )
            instance_id = holding["instanceId"] if "b" in fields_criteria else ""
            location_id = (
                holding["permanentLocationId"] if "l" in fields_criteria else ""
            )
            return "-".join([instance_id, call_number, location_id, ""])
        except Exception as exception:
            logging.error(json.dumps(holding, indent=4))
            raise exception

    def merge_holding(self, key, old_holdings_record, new_holdings_record):
        # TODO: Move to interface or parent class and make more generic
        if self.holdings[key].get("notes", None):
            self.holdings[key]["notes"].extend(new_holdings_record.get("notes", []))
            self.holdings[key]["notes"] = dedupe(self.holdings[key].get("notes", []))
        if self.holdings[key].get("holdingsStatements", None):
            self.holdings[key]["holdingsStatements"].extend(
                new_holdings_record.get("holdingsStatements", [])
            )
            self.holdings[key]["holdingsStatements"] = dedupe(
                self.holdings[key]["holdingsStatements"]
            )
        if self.holdings[key].get("formerIds", None):
            self.holdings[key]["formerIds"].extend(
                new_holdings_record.get("formerIds", [])
            )
            self.holdings[key]["formerIds"] = list(set(self.holdings[key]["formerIds"]))

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
            "--suppress",
            "-ds",
            help="This batch of records are to be suppressed in FOLIO.",
            default=False,
            type=bool,
        )
        flavourhelp = (
            "What criterias do you want to use when merging holdings?\t "
            "All these parameters need to be the same in order to become "
            "the same Holdings record in FOLIO. \n"
            "\tclb\t-\tCallNumber, Location, Bib ID\n"
            "\tlb\t-\tLocation and Bib ID only\n"
            "\tclb_7b94034e-ac0d-49c9-9417-0631a35d506b\t-\t "
            "Exclude bound-with holdings from merging. Requires a "
            "Holdings type in the tenant with this Id"
        )
        sub_parser.add_argument(
            "--holdings_merge_criteria", "-hmc", default="clb", help=flavourhelp
        )


def dedupe(list_of_dicts):
    # TODO: Move to interface or parent class
    return [dict(t) for t in {tuple(d.items()) for d in list_of_dicts}]
