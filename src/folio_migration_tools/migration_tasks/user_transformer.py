import json
import logging
import sys
from typing import Optional, Annotated
from pydantic import Field

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from art import tprint

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
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class UserTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: Annotated[
            str,
            Field(
                title="Migration task name",
                description=(
                    "Name of this migration task. The name is being used to call "
                    "the specific task, and to distinguish tasks of similar types"
                ),
            ),
        ]
        migration_task_type: Annotated[
            str,
            Field(
                title="Migration task type",
                description="The type of migration task you want to perform",
            ),
        ]
        group_map_path: Annotated[
            str,
            Field(
                title="Group map path",
                description="Define the path for group mapping",
            )
        ]
        departments_map_path: Annotated[
            Optional[str],
            Field(
                title="Departments map path",
                description=(
                    "Define the path for departments mapping. "
                    "Optional, by dfault is empty string"
                ),
            )
        ] = ""
        use_group_map: Annotated[
            Optional[bool],
            Field(
                title="Use group map",
                description=(
                    "Specify whether to use group mapping. "
                    "Optional, by default is True"
                ),
            )
        ] = True
        user_mapping_file_name: Annotated[
            str,
            Field(
                title="User mapping file name",
                description="Specify the user mapping file name",
            )
        ]
        user_file: Annotated[
            FileDefinition,
            Field(
                title="User file",
                description="Select the user data file",
            )
        ]
        remove_id_and_request_preferences: Annotated[
            Optional[bool],
            Field(
                title="Remove ID and request preferences",
                description=(
                    "Specify whether to remove user ID and request preferences. "
                    "Optional, by default is False"
                ),
            )
        ] = False
        remove_request_preferences: Annotated[
            Optional[bool],
            Field(
                title="Remove request preferences",
                description=(
                    "Specify whether to remove user request preferences. "
                    "Optional, by default is False"
                ),
            )
        ] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.users

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.task_config = task_config
        self.task_configuration = self.task_config
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
                                str(num_users),
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
        except FileNotFoundError as fn:
            logging.exception("File not found")
            print(f"\n{fn}")
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
        valid_addresses = remove_empty_addresses(folio_user)
        # Make sure the user has exactly one primary address
        if valid_addresses:
            primary_true = find_primary_addresses(valid_addresses)
            if len(primary_true) < 1:
                valid_addresses[0]["primaryAddress"] = True
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
            folio_user["personal"]["addresses"] = valid_addresses


def print_email_warning():
    tprint("\nEMAILS?\n", space=2)


def remove_empty_addresses(folio_user):
    valid_addresses = []
    # Remove empty addresses
    if addresses := folio_user.get("personal", {}).pop("addresses", []):
        for address in addresses:
            address_fields = [
                x for x in address.keys() if x not in ["primaryAddress", "addressTypeId", "id"]
            ]
            if address_fields:
                valid_addresses.append(address)
    return valid_addresses


def find_primary_addresses(addresses):
    primary_true = []
    for address in addresses:
        if "primaryAddress" not in address:
            address["primaryAddress"] = False
        elif (
            isinstance(address["primaryAddress"], bool)
            and address["primaryAddress"] is True
        ) or (
            isinstance(address["primaryAddress"], str)
            and address["primaryAddress"].lower() == "true"
        ):
            primary_true.append(address)
        else:
            address["primaryAddress"] = False
    return primary_true
