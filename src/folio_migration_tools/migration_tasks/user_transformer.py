import json
import logging
import sys
from typing import Optional

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.user_mapper import UserMapper
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class UserTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        migration_task_type: str
        group_map_path: str
        departments_map_path: Optional[str] = ""
        use_group_map: Optional[bool] = True
        user_mapping_file_name: str
        user_file: FileDefinition
        remove_id_and_request_preferences: Optional[bool] = False
        remove_request_preferences: Optional[bool] = False

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
            self.folder_structure.mapping_files_folder / self.task_config.user_mapping_file_name
        )
        self.folio_keys = []
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(self.user_map)
        # Properties
        self.failed_ids = []
        self.failed_objects = []
        if (
            self.folder_structure.mapping_files_folder / self.task_config.group_map_path
        ).is_file():
            group_mapping = self.load_ref_data_mapping_file(
                "patronGroup",
                self.folder_structure.mapping_files_folder / self.task_config.group_map_path,
                self.folio_keys,
            )
        else:
            logging.info(
                "%s not found. No patronGroup mapping will be performed",
                self.folder_structure.mapping_files_folder / self.task_config.group_map_path,
            )
            group_mapping = []

        if (
            self.folder_structure.mapping_files_folder / self.task_config.departments_map_path
        ).is_file():
            departments_mapping = self.load_ref_data_mapping_file(
                "departments",
                self.folder_structure.mapping_files_folder / self.task_config.departments_map_path,
                self.folio_keys,
            )
        else:
            logging.info(
                "%s not found. No departments mapping will be performed",
                self.folder_structure.mapping_files_folder / self.task_config.departments_map_path,
            )
            departments_mapping = []
        map_path = (
            self.folder_structure.mapping_files_folder / self.task_config.user_mapping_file_name
        )
        with open(map_path, encoding="utf8") as mapping_file:
            user_map = json.load(mapping_file)
            self.mapper = UserMapper(
                self.folio_client,
                task_config,
                library_config,
                user_map,
                departments_mapping,
                group_mapping,
            )

        logging.info("UserTransformer init done")

    def do_work(self):
        logging.info("Starting....")
        source_path = (
            self.folder_structure.legacy_records_folder / self.task_config.user_file.file_name
        )

        try:
            with open(
                self.folder_structure.created_objects_path,
                "w+",
                encoding="utf-8",
            ) as results_file:
                with open(source_path, encoding="utf8") as object_file:
                    logging.info(f"processing {source_path}")
                    file_format = "tsv" if str(source_path).endswith(".tsv") else "csv"
                    for num_users, legacy_user in enumerate(
                        self.mapper.get_users(object_file, file_format), start=1
                    ):
                        try:
                            if num_users == 1:
                                logging.info("First Legacy  user")
                                logging.info(json.dumps(legacy_user, indent=4))
                                print_email_warning()
                            folio_user, index_or_id = self.mapper.do_map(
                                legacy_user,
                                num_users,
                                FOLIONamespaces.users,
                            )
                            folio_user = self.mapper.perform_additional_mapping(
                                legacy_user, folio_user, index_or_id
                            )
                            self.clean_user(folio_user, index_or_id)
                            results_file.write(f"{json.dumps(folio_user)}\n")
                            if num_users == 1:
                                logging.info("## First FOLIO  user")
                                logging.info(json.dumps(folio_user, indent=4, sort_keys=True))
                            self.mapper.migration_report.add_general_statistics(
                                i18n.t("Successful user transformations")
                            )
                            if num_users % 1000 == 0:
                                logging.info(f"{num_users} users processed.")
                        except TransformationRecordFailedError as tre:
                            self.mapper.migration_report.add_general_statistics(
                                i18n.t("Records failed")
                            )
                            Helper.log_data_issue(tre.index_or_id, tre.message, tre.data_value)
                            logging.error(tre)
                        except TransformationProcessError as tpe:
                            logging.critical(tpe)
                            print(f"\n{tpe.message}: {tpe.data_value}")
                            print("\nHalting")
                            sys.exit(1)
                        except ValueError as ve:
                            logging.error(ve)
                            raise ve
                        except Exception as ee:
                            logging.error(ee)
                            logging.error(num_users)
                            logging.error(json.dumps(legacy_user))
                            self.mapper.migration_report.add_general_statistics(
                                i18n.t("Failed user transformations")
                            )
                            logging.error(ee, exc_info=True)

                        self.total_records = num_users
        except FileNotFoundError as fnfe:
            logging.exception("File not found")
            print(f"\n{fnfe}")
            sys.exit(1)

    def wrap_up(self):
        self.extradata_writer.flush()
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            self.mapper.migration_report.write_migration_report(
                i18n.t("Users transformation report"),
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

    @staticmethod
    def clean_user(folio_user, index_or_id):
        # Make sure the user has exactly one primary address
        if addresses := folio_user.get("personal", {}).get("addresses", []):
            primary_true = []
            for address in addresses:
                if "primaryAddress" not in address:
                    address["primaryAddress"] = False
                elif (
                    isinstance(address["primaryAddress"], bool)
                    and address["primaryAddress"] is True
                ):
                    primary_true.append(address)

            if len(primary_true) < 1:
                addresses[0]["primaryAddress"] = True
            elif len(primary_true) > 1:
                logging.log(
                    26,
                    "DATA ISSUE\t%s\t%s\t%s",
                    index_or_id,
                    "Too many addresses mapped as primary. Setting first one to primary.",
                    primary_true,
                )
                for pt in primary_true[1:]:
                    pt["primaryAddress"] = False


def print_email_warning():
    s = (
        "  ______   __  __              _____   _         _____     ___  \n"  # noqa: E501, W605
        " |  ____| |  \/  |     /\     |_   _| | |       / ____|   |__ \ \n"  # noqa: E501, W605
        " | |__    | \  / |    /  \      | |   | |      | (___        ) |\n"  # noqa: E501, W605
        " |  __|   | |\/| |   / /\ \     | |   | |       \___ \      / / \n"  # noqa: E501, W605
        " |______| |_|  |_| /_/    \_\ |_____| |______| |_____/     (_)  \n"  # noqa: E501, W605
        "                                                                \n"  # noqa: E501, W605
        "                                                       \n"
    )
    print(s)
