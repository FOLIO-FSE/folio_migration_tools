import csv
from genericpath import isfile
import logging
from pathlib import Path
import sys
import time
from abc import abstractmethod
import json

from argparse_prompt import PromptParser
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient
from folio_migration_tools import library_configuration
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.folder_structure import FolderStructure


class MigrationTaskBase:
    @staticmethod
    @abstractmethod
    def get_object_type() -> FOLIONamespaces:
        raise NotImplementedError()

    def __init__(
        self,
        library_configuration: library_configuration.LibraryConfiguration,
        task_configuration,
        use_logging: bool = True,
    ):

        print("MigrationTaskBase init")
        self.folio_client: FolioClient = FolioClient(
            library_configuration.okapi_url,
            library_configuration.tenant_id,
            library_configuration.okapi_username,
            library_configuration.okapi_password,
        )
        self.folder_structure: FolderStructure = FolderStructure(
            library_configuration.base_folder,
            self.get_object_type(),
            task_configuration.name,
            library_configuration.iteration_identifier,
            library_configuration.add_time_stamp_to_file_names,
        )

        self.library_configuration = library_configuration
        self.object_type = self.get_object_type()
        try:
            self.folder_structure.setup_migration_file_structure()
            # Initiate Worker
        except FileNotFoundError as fne:
            logging.error(fne)
        except TransformationProcessError as process_error:
            logging.critical(process_error)
            logging.critical("Halting...")
            sys.exit()
        self.num_exeptions: int = 0
        if use_logging:
            self.setup_logging()
        self.folder_structure.log_folder_structure()
        print("MigrationTaskBase init done")

    @abstractmethod
    def wrap_up(self):
        raise NotImplementedError()

    @abstractmethod
    def do_work(self):
        raise NotImplementedError

    @staticmethod
    def load_id_map(map_path):
        if not isfile(map_path):
            logging.warn(
                "No legacy id map found at %s. Will build one from scratch", map_path
            )
            return {}
        id_map = {}
        with open(map_path) as id_map_file:
            for index, json_string in enumerate(id_map_file):
                # {"legacy_id", "folio_id","suppressed"}
                map_object = json.loads(json_string)
                if index % 50000 == 0:
                    print(
                        f"{(index+1)} ids loaded to map {map_object['legacy_id']}",
                        end="\r",
                    )
                id_map[map_object["legacy_id"]] = map_object
        logging.info("Loaded %s migrated IDs", (index + 1))
        return id_map

    @staticmethod
    def add_argument(parser, destination, help, **kwargs):
        parser.add_argument(dest=destination, help=help, **kwargs)

    def setup_logging(self):
        debug = self.library_configuration.log_level_debug
        DATA_OUTPUT_LVL_NUM = 25
        logging.addLevelName(DATA_OUTPUT_LVL_NUM, "DATA_OUTPUT")

        def data_output(self, message, *args, **kws):
            if self.isEnabledFor(DATA_OUTPUT_LVL_NUM):
                # Yes, logger takes its '*args' as 'args'.
                self._log(DATA_OUTPUT_LVL_NUM, message, args, **kws)

        logging.Logger.data_output = data_output

        DATA_ISSUE_LVL_NUM = 26
        logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")

        def data_issues(self, message, *args, **kws):
            if self.isEnabledFor(DATA_ISSUE_LVL_NUM):
                # Yes, logger takes its '*args' as 'args'.
                self._log(DATA_ISSUE_LVL_NUM, message, args, **kws)

        logging.Logger.data_issues = data_issues

        logger = logging.getLogger()
        logger.handlers = []
        formatter = logging.Formatter(
            "%(asctime)s\t%(levelname)s\t%(message)s\t%(filename)s:%(lineno)d"
        )
        stream_handler = logging.StreamHandler()
        stream_handler.addFilter(ExcludeLevelFilter(25))
        stream_handler.addFilter(ExcludeLevelFilter(26))

        if debug:
            logger.setLevel(logging.DEBUG)
            stream_handler.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
            stream_handler.setLevel(logging.INFO)
            stream_handler.addFilter(
                ExcludeLevelFilter(30)
            )  # Loose warnings from pymarc
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        file_formatter = logging.Formatter("%(message)s")
        file_handler = logging.FileHandler(
            filename=self.folder_structure.transformation_log_path, mode="w"
        )
        file_handler.addFilter(ExcludeLevelFilter(25))
        file_handler.addFilter(ExcludeLevelFilter(26))
        # file_handler.addFilter(LevelFilter(0, 20))
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(file_handler)

        # Data file formatter
        data_file_formatter = logging.Formatter("%(message)s")
        data_file_handler = logging.FileHandler(
            filename=str(self.folder_structure.transformation_extra_data_path), mode="w"
        )
        data_file_handler.addFilter(LevelFilter(25))
        data_file_handler.setFormatter(data_file_formatter)
        data_file_handler.setLevel(25)
        logging.getLogger().addHandler(data_file_handler)

        # Data issue file formatter
        data_issue_file_formatter = logging.Formatter("%(message)s")
        data_issue_file_handler = logging.FileHandler(
            filename=str(self.folder_structure.data_issue_file_path), mode="w"
        )
        data_issue_file_handler.addFilter(LevelFilter(26))
        data_issue_file_handler.setFormatter(data_issue_file_formatter)
        data_issue_file_handler.setLevel(26)
        logging.getLogger().addHandler(data_issue_file_handler)
        logger.info("Logging set up")

    def setup_records_map(self, mapping_file_path):
        with open(mapping_file_path) as mapping_file:
            field_map = json.load(mapping_file)
            logging.info("%s fields in mapping file map", len(field_map["data"]))
            mapped_fields = (
                f
                for f in field_map["data"]
                if f["legacy_field"] and f["legacy_field"] != "Not mapped"
            )
            logging.info(
                "%s Mapped fields in mapping file map", len(list(mapped_fields))
            )
            return field_map

    @staticmethod
    def add_common_arguments(parser: PromptParser):

        """parser.add_argument("okapi_url", help="OKAPI base url")
        parser.add_argument("tenant_id", help="id of the FOLIO tenant.")
        parser.add_argument("username", help="the api user")
        parser.add_argument("base_folder", help="path base folder", type=str)
        parser.add_argument("--password", help="the api users password", secure=True)"""

    def log_and_exit_if_too_many_errors(
        self, error: TransformationRecordFailedError, idx
    ):
        self.num_exeptions += 1
        error.log_it()
        if self.num_exeptions / (1 + idx) > 0.2 and self.num_exeptions > 5000:
            logging.fatal(
                f"Number of exceptions exceeded limit of "
                f"{self.num_exeptions}. and failure rate is "
                f"{self.num_exeptions / (1 + idx)} Stopping."
            )
            sys.exit()

    @staticmethod
    def print_progress(num_processed, start_time):
        if num_processed > 1 and num_processed % 10000 == 0:
            elapsed = num_processed / (time.time() - start_time)
            elapsed_formatted = "{0:.4g}".format(elapsed)
            logging.info(
                f"{num_processed:,} records processed. Recs/sec: {elapsed_formatted} "
            )

    def load_ref_data_mapping_file(
        self,
        folio_property_name: str,
        map_file_path: Path,
        folio_keys,
        required: bool = True,
    ):
        if (
            folio_property_name in folio_keys
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
                    "",
                    (
                        f"{folio_property_name} not mapped in legacy->folio mapping file "
                        f"({map_file_path}) ({exception}). Did you map this field, "
                        "but forgot to add a mapping file?"
                    ),
                ) from exception
        else:
            logging.info("No mapping setup for %s", folio_property_name)
            logging.info("%s will have default mapping if any ", folio_property_name)
            logging.info(
                "Add a file named %s and add the field to "
                "the item.mapping.json file.",
                map_file_path,
            )
            return None


class ExcludeLevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno != self.level


class LevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno == self.level
