import csv
import json
import logging
from abc import abstractmethod
import sys
from typing import Dict, List, Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_migration_tools import migration_report
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.user_mapper import UserMapper
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from pydantic import BaseModel


class UserTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        migration_task_type: str
        group_map_path: str
        departments_map_path: Optional[str] = ""
        use_group_map: Optional[bool] = True
        user_mapping_file_name: str
        user_file: FileDefinition

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.users

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        super().__init__(library_config, task_config, use_logging)
        self.task_config = task_config
        self.total_records = 0

        self.user_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder
            / self.task_config.user_mapping_file_name
        )
        self.folio_keys = []
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.user_map
        )
        # Properties
        self.failed_ids = []
        self.failed_objects = []
        if (
            self.folder_structure.mapping_files_folder / self.task_config.group_map_path
        ).is_file():
            group_mapping = self.load_ref_data_mapping_file(
                "patronGroup",
                self.folder_structure.mapping_files_folder
                / self.task_config.group_map_path,
                self.folio_keys,
            )
        else:
            logging.info(
                "%s not found. No patronGroup mapping will be performed",
                self.folder_structure.mapping_files_folder
                / self.task_config.group_map_path,
            )
            group_mapping = []

        if (
            self.folder_structure.mapping_files_folder
            / self.task_config.departments_map_path
        ).is_file():
            departments_mapping = self.load_ref_data_mapping_file(
                "departments",
                self.folder_structure.mapping_files_folder
                / self.task_config.departments_map_path,
                self.folio_keys,
            )
        else:
            logging.info(
                "%s not found. No departments mapping will be performed",
                self.folder_structure.mapping_files_folder
                / self.task_config.departments_map_path,
            )
            departments_mapping = []
        self.mapper = UserMapper(
            self.folio_client,
            task_config,
            library_config,
            departments_mapping,
            group_mapping,
        )
        print("UserTransformer init done")

    def do_work(self):
        logging.info("Starting....")
        source_path = (
            self.folder_structure.legacy_records_folder
            / self.task_config.user_file.file_name
        )
        map_path = (
            self.folder_structure.mapping_files_folder
            / self.task_config.user_mapping_file_name
        )
        try:
            with open(
                self.folder_structure.created_objects_path,
                "w+",
                encoding="utf-8",
            ) as results_file:
                with open(source_path, encoding="utf8") as object_file, open(
                    map_path, encoding="utf8"
                ) as mapping_file:
                    logging.info(f"processing {source_path}")
                    user_map = json.load(mapping_file)
                    legacy_property_name = self.get_legacy_id_prop(user_map)

                    file_format = "tsv" if str(source_path).endswith(".tsv") else "csv"
                    for num_users, legacy_user in enumerate(
                        self.mapper.get_users(object_file, file_format), start=1
                    ):
                        try:
                            if num_users == 1:
                                logging.info("First Legacy  user")
                                logging.info(json.dumps(legacy_user, indent=4))
                            folio_user = self.mapper.do_map(
                                legacy_user,
                                user_map,
                                legacy_user.get(legacy_property_name),
                            )
                            self.clean_user(folio_user)
                            results_file.write(f"{json.dumps(folio_user)}\n")
                            if num_users == 1:
                                logging.info("## First FOLIO  user")
                                logging.info(
                                    json.dumps(folio_user, indent=4, sort_keys=True)
                                )
                            self.mapper.migration_report.add_general_statistics(
                                "Successful user transformations"
                            )
                            if num_users % 1000 == 0:
                                logging.info(f"{num_users} users processed.")
                        except TransformationRecordFailedError as tre:
                            self.mapper.migration_report.add_general_statistics(
                                "Records failed"
                            )
                            Helper.log_data_issue(
                                tre.index_or_id, tre.message, tre.data_value
                            )
                            logging.error(tre)
                        except TransformationProcessError as tpe:
                            logging.error(tpe)
                            print(f"\n{tpe.message}")
                            print("Halting")
                            sys.exit()
                        except ValueError as ve:
                            logging.error(ve)
                            raise ve
                        except Exception as ee:
                            logging.error(ee)
                            logging.error(num_users)
                            logging.error(json.dumps(legacy_user))
                            self.mapper.migration_report.add_general_statistics(
                                "Failed user transformations"
                            )
                            logging.error(ee, exc_info=True)
                        finally:
                            if num_users == 1:
                                print_email_warning()
                        self.total_records = num_users
        except FileNotFoundError as fnfe:
            logging.exception("File not found")
            print(f"\n{fnfe}")
            sys.exit()

    @staticmethod
    def get_legacy_id_prop(record_map):
        field_map = {}  # Map of folio_fields and source fields as an array
        for k in record_map["data"]:
            if not field_map.get(k["folio_field"]):
                field_map[k["folio_field"]] = [k["legacy_field"]]
            else:
                field_map[k["folio_field"]].append(k["legacy_field"])
        if "legacyIdentifier" not in field_map:
            raise TransformationProcessError(
                "",
                (
                    "property legacyIdentifier is not in map. Add this property "
                    "to the mapping file as if it was a FOLIO property"
                ),
            )
        try:
            legacy_id_property_name = field_map["legacyIdentifier"][0]
            logging.info(
                "Legacy identifier will be mapped from %s", legacy_id_property_name
            )
            return legacy_id_property_name
        except Exception as exception:
            raise TransformationProcessError(
                "",
                (
                    f"property legacyIdentifier not setup in map: "
                    f"{field_map.get('legacyIdentifier', '') ({exception})}"
                ),
            ) from exception

    def wrap_up(self):
        path = self.folder_structure.results_folder / "user_id_map.json"
        logging.info(
            f"Saving map of {len(self.mapper.legacy_id_map)} old and new IDs to {path}"
        )

        with open(path, "w+") as id_map_file:
            json.dump(self.mapper.legacy_id_map, id_map_file, indent=4)
        with open(
            self.folder_structure.migration_reports_file, "w"
        ) as migration_report_file:
            logging.info(
                "Writing migration- and mapping report to %s",
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

    @staticmethod
    def clean_user(folio_user):
        if addresses := folio_user.get("personal", {}).get("addresses", []):
            primary_address_exists = False

            for address in addresses:
                if "id" in address:
                    del address["id"]

                if address["primaryAddress"] is True:
                    primary_address_exists = True

            if not primary_address_exists:
                addresses[0]["primaryAddress"] = True


def print_email_warning():
    s = (
        "  ______   __  __              _____   _         _____     ___  \n"  # pylint: disable=anomalous-backslash-in-string
        " |  ____| |  \/  |     /\     |_   _| | |       / ____|   |__ \ \n"  # pylint: disable=anomalous-backslash-in-string
        " | |__    | \  / |    /  \      | |   | |      | (___        ) |\n"  # pylint: disable=anomalous-backslash-in-string
        " |  __|   | |\/| |   / /\ \     | |   | |       \___ \      / / \n"  # pylint: disable=anomalous-backslash-in-string
        " | |____  | |  | |  / ____ \   _| |_  | |____   ____) |    |_|  \n"  # pylint: disable=anomalous-backslash-in-string
        " |______| |_|  |_| /_/    \_\ |_____| |______| |_____/     (_)  \n"  # pylint: disable=anomalous-backslash-in-string
        "                                                                \n"  # pylint: disable=anomalous-backslash-in-string
        "                                                       \n"
    )
    print(s)


def get_import_struct(batch) -> Dict:
    return {
        "source_type": "",
        "deactivateMissingUsers": False,
        "users": list(batch),
        "updateOnlyPresentFields": False,
        "totalRecords": len(batch),
    }
