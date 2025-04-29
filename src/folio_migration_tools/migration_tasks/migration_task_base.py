import csv
import io
import json
import logging
import os
import sys
import time
from abc import abstractmethod
from datetime import datetime, timezone
from genericpath import isfile
from pathlib import Path
from typing import Optional

import folioclient
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools import library_configuration, task_configuration
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.folder_structure import FolderStructure
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.marc_reader_wrapper import (
    MARCReaderWrapper,
)


class MigrationTaskBase:
    @staticmethod
    @abstractmethod
    def get_object_type() -> FOLIONamespaces:
        raise NotImplementedError()

    def __init__(
        self,
        library_configuration: library_configuration.LibraryConfiguration,
        task_configuration: task_configuration.AbstractTaskConfiguration,
        folio_client: folioclient.FolioClient,
        use_logging: bool = True,
    ):
        logging.info("MigrationTaskBase init")
        self.start_datetime = datetime.now(timezone.utc)
        self.task_configuration = task_configuration
        logging.info(self.task_configuration.json(indent=4))
        self.folio_client: FolioClient = folio_client
        self.ecs_tenant_id = (
            task_configuration.ecs_tenant_id or library_configuration.ecs_tenant_id
        )
        self.ecs_tenant_header = (
            {"x-okapi-tenant": self.ecs_tenant_id} if self.ecs_tenant_id else {}
        )
        self.folio_client.okapi_headers.update(self.ecs_tenant_header)
        self.central_folder_structure: Optional[FolderStructure] = None
        if library_configuration.is_ecs and library_configuration.ecs_central_iteration_identifier:
            self.central_folder_structure = FolderStructure(
                library_configuration.base_folder,
                FOLIONamespaces.instances,
                task_configuration.name,
                library_configuration.ecs_central_iteration_identifier,
                library_configuration.add_time_stamp_to_file_names,
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
            if self.central_folder_structure:
                self.central_folder_structure.setup_migration_file_structure()
            # Initiate Worker
        except FileNotFoundError as fne:
            logging.error(fne)
        except TransformationProcessError as process_error:
            logging.critical(process_error)
            logging.critical("Halting...")
            sys.exit(1)
        self.num_exeptions: int = 0
        self.extradata_writer = ExtradataWriter(
            self.folder_structure.transformation_extra_data_path
        )
        if use_logging:
            self.setup_logging()
        self.folder_structure.log_folder_structure()
        logging.info("MigrationTaskBase init done")

    @abstractmethod
    def wrap_up(self):
        raise NotImplementedError()

    def clean_out_empty_logs(self):
        if (
            self.folder_structure.data_issue_file_path.is_file()
            and os.stat(self.folder_structure.data_issue_file_path).st_size == 0
        ):
            logging.info("Removing data issues file since it is empty")
            os.remove(self.folder_structure.data_issue_file_path)
            logging.info("Removed data issues file since it was empty")

        if (
            self.folder_structure.failed_marc_recs_file.is_file()
            and os.stat(self.folder_structure.failed_marc_recs_file).st_size == 0
        ):
            os.remove(self.folder_structure.failed_marc_recs_file)
            logging.info("Removed empty failed marc records file since it was empty")

    @abstractmethod
    def do_work(self):
        raise NotImplementedError

    @staticmethod
    def check_source_files(
        source_path: Path, file_defs: list[library_configuration.FileDefinition]
    ) -> None:
        """Lists the source data files. Special case since we use the Items folder for holdings

        Args:
            source_path (Path): _description_
            file_defs (list[library_configuration.FileDefinition]): _description_

        Raises:
            TransformationProcessError: _description_

        """
        files = [
            source_path / f.file_name
            for f in file_defs
            if isfile(source_path / f.file_name)
        ]
        ret_str = ", ".join(f.file_name for f in file_defs)

        if files and len(files) < len(file_defs):
            raise TransformationProcessError(
                "",
                f"Some files listed in task configuration not found in {source_path}."
                f"Listed files: {ret_str}",
            )
        if not any(files):
            raise TransformationProcessError(
                "",
                f"None of the files listed in task configuration found in {source_path}."
                f"Listed files: {ret_str}",
            )
        logging.info("Files to process:")
        for filename in files:
            logging.info("\t%s", filename)

    def load_instance_id_map(self, raise_if_empty=True) -> dict:
        """
        This method handles loading instance id maps for holdings and other transformations that require it.
        This is in the base class because multiple tasks need it. It exists because instances in an ECS environment
        are transformed for the central and data tenants separately, but the data tenants need to know about
        the central tenant instance ids. This is a bit of a hack, but it works for now.
        """
        map_files = []
        instance_id_map = {}
        if self.library_configuration.is_ecs and self.central_folder_structure:
            logging.info(
                "Loading ECS central tenant instance id map from %s", self.central_folder_structure.instance_id_map_path
            )
            instance_id_map = self.load_id_map(
                self.central_folder_structure.instance_id_map_path,
                raise_if_empty=False,
            )
            map_files.append(str(self.central_folder_structure.instance_id_map_path))
            logging.info(
                "Loading member tenant isntance id map from %s",
                self.folder_structure.instance_id_map_path
            )
        instance_id_map = self.load_id_map(
            self.folder_structure.instance_id_map_path,
            raise_if_empty=False,
            existing_id_map=instance_id_map,
        )
        map_files.append(str(self.folder_structure.instance_id_map_path))
        if not any(instance_id_map) and raise_if_empty:
            map_file_paths = ", ".join(map_files)
            raise TransformationProcessError("", "Instance id map is empty", map_file_paths)
        return instance_id_map

    @staticmethod
    def load_id_map(map_path, raise_if_empty=False, existing_id_map={}):
        if not isfile(map_path):
            logging.warning(
                "No legacy id map found at %s. Will build one from scratch", map_path
            )
            return {}
        id_map = existing_id_map
        loaded_rows = len(id_map)
        with open(map_path) as id_map_file:
            for index, json_string in enumerate(id_map_file, start=1):
                loaded_rows = index
                # {"legacy_id", "folio_id","suppressed"}
                map_tuple = json.loads(json_string)
                if loaded_rows % 500000 == 0:
                    print(
                        f"{loaded_rows + 1} ids loaded to map. Last Id: {map_tuple[0]}          ",
                        end="\r",
                    )

                id_map[map_tuple[0]] = map_tuple
        logging.info("Loaded %s migrated IDs from %s", loaded_rows, id_map_file.name)
        if not any(id_map) and raise_if_empty:
            raise TransformationProcessError("", "Legacy id map is empty", map_path)
        return id_map

    @staticmethod
    def add_argument(parser, destination, help, **kwargs):
        parser.add_argument(dest=destination, help=help, **kwargs)

    def setup_logging(self):
        debug = self.library_configuration.log_level_debug

        DATA_ISSUE_LVL_NUM = 26
        logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")

        def data_issues(self, message, *args, **kws):
            if self.isEnabledFor(DATA_ISSUE_LVL_NUM):
                # Yes, logger takes its '*args' as 'args'.
                self._log(DATA_ISSUE_LVL_NUM, message, args, **kws)

        logging.Logger.data_issues = data_issues
        logger = logging.getLogger()
        logger.propogate = True
        logger.handlers = []
        formatter = logging.Formatter(
            "%(asctime)s\t%(levelname)s\t%(message)s\t%(task_configuration_name)s"
        )
        stream_handler = logging.StreamHandler()
        stream_handler.addFilter(ExcludeLevelFilter(26))
        stream_handler.addFilter(TaskNameFilter(self.task_configuration.name))
        if debug:
            logger.setLevel(logging.DEBUG)
            stream_handler.setLevel(logging.DEBUG)
            logging.getLogger("httpx").setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
            stream_handler.setLevel(logging.INFO)
            stream_handler.addFilter(
                ExcludeLevelFilter(30)
            )  # Exclude warnings from pymarc
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        file_formatter = logging.Formatter(
            "%(asctime)s\t%(message)s\t%(task_configuration_name)s\t%(filename)s:%(lineno)d"
        )
        file_handler = logging.FileHandler(
            filename=self.folder_structure.transformation_log_path, mode="w"
        )
        file_handler.addFilter(ExcludeLevelFilter(26))
        file_handler.addFilter(TaskNameFilter(self.task_configuration.name))
        # file_handler.addFilter(LevelFilter(0, 20))
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(file_handler)

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
            logging.info(
                "%s fields present in record mapping file", len(field_map["data"])
            )
            mapped_fields = (
                f
                for f in field_map["data"]
                if f["legacy_field"] and f["legacy_field"] != "Not mapped"
            )
            logging.info(
                "%s fields mapped in record mapping file", len(list(mapped_fields))
            )
            return field_map

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
            sys.exit(1)

    @staticmethod
    def print_progress(num_processed, start_time):
        if num_processed > 1 and num_processed % 10000 == 0:
            elapsed = num_processed / (time.time() - start_time)
            elapsed_formatted = "{0:.4g}".format(elapsed)
            logging.info(
                f"{num_processed:,} records processed. Recs/sec: {elapsed_formatted} "
            )

    def do_work_marc_transformer(
        self,
    ):
        logging.info("Starting....")
        if self.folder_structure.failed_marc_recs_file.is_file():
            os.remove(self.folder_structure.failed_marc_recs_file)
            logging.info("Removed failed marc records file to prevent duplicating data")
        with open(
            self.folder_structure.created_objects_path, "w+"
        ) as created_records_file:
            self.processor = MarcFileProcessor(
                self.mapper, self.folder_structure, created_records_file
            )
            for file_def in self.task_configuration.files:
                MARCReaderWrapper.process_single_file(
                    file_def,
                    self.processor,
                    self.folder_structure.failed_marc_recs_file,
                    self.folder_structure,
                )

    @staticmethod
    def validate_ref_data_mapping_lines(lines, num_of_columns):
        """
        Helper method to validate the structure of individual lines in a mapping file.

        Args:
            lines (list): List of lines in the mapping file
            num_of_columns (int): Number of columns expected in each line

        Returns:
            tuple: A tuple containing a list of invalid lines and a list of valid lines
        """
        invalid_lines = []
        valid_lines = []
        for idx, row in enumerate(lines, start=2):
            if not row.strip():
                if idx == len(lines) + 1:
                    continue
                else:
                    invalid_lines.append(str(idx))
            else:
                line_length = len(row.split("\t"))
                if line_length != num_of_columns:
                    invalid_lines.append(str(idx))
                else:
                    valid_lines.append(str(idx))
        return invalid_lines, valid_lines

    @staticmethod
    def verify_ref_data_mapping_file_structure(map_file: io.TextIOBase):
        """
        Helper method to validate the structure of a mapping file.

        Args:
            map_file (io.TextIOBase): The mapping file to validate

        Raises:
            TransformationProcessError: If the mapping file has rows with different number of columns

        Returns:
            None
        """
        current_pos = map_file.tell()
        try:
            map_file.seek(0)
            num_of_columns = len(map_file.readline().split("\t"))
            lines = map_file.readlines()
            invalid_lines, valid_lines = MigrationTaskBase.validate_ref_data_mapping_lines(
                lines, num_of_columns
            )
            if invalid_lines:
                raise TransformationProcessError(
                    "",
                    (
                        f"Mapping file {map_file.name} has rows with different number "
                        f"of columns ({'Row' if len(invalid_lines) == 1 else 'Rows'} {', '.join(invalid_lines)})"
                    ),
                )
            if not valid_lines:
                raise TransformationProcessError(
                    "", f"Map has no rows: {map_file.name}"
                )
        finally:
            map_file.seek(current_pos)

    @staticmethod
    def load_ref_data_mapping_file(
        folio_property_name: str,
        map_file_path: Path,
        folio_keys,
        required: bool = True,
    ):
        """
        Helper method to load a reference data mapping file.

        Args:
            folio_property_name (str): The name of the property in FOLIO
            map_file_path (Path): The path to the mapping file
            folio_keys (list): A list of FOLIO keys
            required (bool): Whether the property is required or not
        """
        if (
            folio_property_name in folio_keys
            or required
            or folio_property_name.startswith("statisticalCodeIds")
            or folio_property_name.startswith("locationMap")
        ):
            try:
                with open(map_file_path) as map_file:
                    # Validate the structure of the mapping file
                    MigrationTaskBase.verify_ref_data_mapping_file_structure(map_file)
                    ref_data_map = list(csv.DictReader(map_file, dialect="tsv"))
                    logging.info(
                        "Found %s rows in %s map",
                        len(ref_data_map),
                        folio_property_name,
                    )
                    if not any(ref_data_map[0].keys()):
                        raise TransformationProcessError(
                            "",
                            (
                                f"{folio_property_name} not mapped in legacy->folio mapping file "
                                f"({map_file_path}). Did you map this field, "
                                "but forgot to add a mapping file?"
                            ),
                        )
                    logging.info(
                        "%s will be used for determining %s",
                        ", ".join(ref_data_map[0].keys()),
                        folio_property_name,
                    )
                    return ref_data_map
            except Exception as exception:
                raise exception

        else:
            logging.info("No mapping setup for %s", folio_property_name)
            logging.info("%s will have default mapping if any ", folio_property_name)
            logging.info(
                "Add a file named %s and add the field to the item.mapping.json file.",
                map_file_path,
            )
            return None


class ExcludeLevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno != self.level


class TaskNameFilter(logging.Filter):
    def __init__(self, task_configuration_name):
        super().__init__()
        self.task_configuration_name = task_configuration_name

    def filter(self, record):
        record.task_configuration_name = self.task_configuration_name
        return True


class LevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.level = level

    def filter(self, record):
        return record.levelno == self.level
