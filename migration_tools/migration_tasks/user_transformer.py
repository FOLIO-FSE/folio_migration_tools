import csv
import json
import logging
from abc import abstractmethod
import sys
from typing import Dict, List, Optional

from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.helper import Helper
from migration_tools.library_configuration import FileDefinition, LibraryConfiguration
from migration_tools.mapping_file_transformation.user_mapper import UserMapper
from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from pydantic import BaseModel


class UserTransformer(MigrationTaskBase):
    class TaskConfiguration(BaseModel):
        name: str
        migration_task_type: str
        group_map_path: str
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
        # Properties
        self.failed_ids = []
        self.failed_objects = []
        csv.register_dialect("tsv", delimiter="\t")
        if self.task_config.use_group_map:
            with open(
                self.folder_structure.mapping_files_folder
                / self.task_config.group_map_path,
                "r",
            ) as group_map_file:
                self.mapper = UserMapper(
                    self.folio_client,
                    task_config,
                    library_config,
                    list(csv.DictReader(group_map_file, dialect="tsv")),
                )
        else:
            self.mapper = UserMapper(
                self.folio_client,
                task_config,
                library_config,
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
                file_format = "tsv" if str(source_path).endswith(".tsv") else "csv"
                for legacy_user in self.mapper.get_users(object_file, file_format):
                    self.total_records += 1
                    try:
                        if self.total_records == 1:
                            logging.info("First Legacy  user")
                            logging.info(json.dumps(legacy_user, indent=4))
                        folio_user = self.mapper.do_map(
                            legacy_user, user_map, self.total_records
                        )
                        self.clean_user(folio_user)
                        results_file.write(f"{json.dumps(folio_user)}\n")
                        if self.total_records == 1:
                            logging.info("## First FOLIO  user")
                            logging.info(
                                json.dumps(folio_user, indent=4, sort_keys=True)
                            )
                        self.mapper.migration_report.add_general_statistics(
                            "Successful user transformations"
                        )
                        if self.total_records % 1000 == 0:
                            logging.info(f"{self.total_records} users processed.")
                    except TransformationRecordFailedError as tre:
                        Helper.log_data_issue(
                            tre.index_or_id, tre.message, tre.data_value
                        )
                        logging.error(tre)
                    except TransformationProcessError as tpe:
                        logging.error(tpe)
                        print("Halting")
                        sys.exit()
                    except ValueError as ve:
                        logging.error(ve)
                    except Exception as ee:
                        logging.error(self.total_records)
                        logging.error(json.dumps(legacy_user))
                        self.mapper.migration_report.add_general_statistics(
                            "Failed user transformations"
                        )
                        raise ee
                    finally:
                        if self.total_records == 1:
                            print_email_warning()

    def wrap_up(self):
        path = self.folder_structure.results_folder / "user_id_map.json"
        logging.info(
            "Saving map of {} old and new IDs to {}".format(
                len(self.mapper.legacy_id_map), path
            )
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
        if "id" in folio_user:
            del folio_user["id"]

        addresses = folio_user.get("personal", {}).get("addresses", [])

        if addresses:
            primary_address_exists = False

            for address in addresses:
                if "id" in address:
                    del address["id"]

                if address["primaryAddress"] is True:
                    primary_address_exists = True

            if not primary_address_exists:
                addresses[0]["primaryAddress"] = True

    @staticmethod
    @abstractmethod
    def add_arguments(parser):
        MigrationTaskBase.add_common_arguments(parser)
        MigrationTaskBase.add_argument(
            parser,
            "client_folder",
            "Client folder for current migration. Assumes a certain folder structure.",
        )


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
