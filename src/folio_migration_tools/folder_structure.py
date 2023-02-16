import logging
import sys
import time
from pathlib import Path

from folio_uuid.folio_namespaces import FOLIONamespaces


class FolderStructure:
    def __init__(
        self,
        base_path: Path,
        object_type: FOLIONamespaces,
        migration_task_name: str,
        iteration_identifier: str,
        add_time_stamp_to_file_names: bool,
    ):
        logging.info("Validating folder structure")

        self.object_type: FOLIONamespaces = object_type
        self.migration_task_name = migration_task_name
        self.add_time_stamp_to_file_names = add_time_stamp_to_file_names
        self.iteration_identifier = iteration_identifier
        self.base_folder = Path(base_path)
        if not self.base_folder.is_dir():
            logging.critical("Base Folder Path is not a folder. Exiting.")
            sys.exit(1)

        # Basic folders
        self.mapping_files_folder = self.base_folder / "mapping_files"
        self.verify_folder(self.mapping_files_folder)
        gitignore = self.base_folder / ".gitignore"
        verify_git_ignore(gitignore)
        self.verify_folder(self.base_folder / "iterations")

        # Iteration-specific folders
        self.iteration_folder = self.base_folder / "iterations" / self.iteration_identifier
        self.verify_folder(self.iteration_folder)
        self.data_folder = self.iteration_folder / "source_data"
        self.verify_folder(self.data_folder)
        self.results_folder = self.iteration_folder / "results"
        self.verify_folder(self.results_folder)
        self.reports_folder = self.iteration_folder / "reports"
        self.verify_folder(self.reports_folder)

    def log_folder_structure(self):
        logging.info("Mapping files folder is %s", self.mapping_files_folder)
        logging.info("Git ignore is set up correctly")
        logging.info("Base folder is %s", self.base_folder)
        logging.info("Reports and logs folder is %s", self.reports_folder)
        logging.info("Results folder is %s", self.results_folder)
        logging.info("Data folder is %s", self.data_folder)
        logging.info("Source records files folder is %s", self.legacy_records_folder)
        logging.info("Log file will be located at %s", self.transformation_log_path)
        logging.info("Extra data will be stored at%s", self.transformation_extra_data_path)
        logging.info("Data issue reports %s", self.data_issue_file_path)
        logging.info("Created objects will be stored at  %s", self.created_objects_path)
        logging.info("Migration report file will be saved at %s", self.migration_reports_file)

    def setup_migration_file_structure(self, source_file_type: str = ""):
        self.time_stamp = f'_{time.strftime("%Y%m%d-%H%M%S")}'
        self.time_str = self.time_stamp if self.add_time_stamp_to_file_names else ""
        self.file_template = f"{self.time_str}_{self.migration_task_name}"
        object_type_string = str(self.object_type.name).lower()
        if source_file_type:
            self.legacy_records_folder = self.data_folder / source_file_type
        elif self.object_type == FOLIONamespaces.other:
            self.legacy_records_folder = self.data_folder
        else:
            self.legacy_records_folder = self.data_folder / object_type_string
        self.verify_folder(self.legacy_records_folder)

        # Make sure the items are there if the Holdings processor is run
        if self.object_type == FOLIONamespaces.holdings:
            self.verify_folder(self.data_folder / str(FOLIONamespaces.items.name).lower())

        self.transformation_log_path = self.reports_folder / (
            f"log_{object_type_string}{self.file_template}.log"
        )

        self.failed_recs_path = (
            self.results_folder / f"failed_records{self.file_template}{self.time_stamp}.txt"
        )

        self.transformation_extra_data_path = (
            self.results_folder / f"extradata{self.file_template}.extradata"
        )

        self.data_issue_file_path = (
            self.reports_folder / f"data_issues_log{self.file_template}.tsv"
        )
        self.created_objects_path = (
            self.results_folder / f"folio_{object_type_string}{self.file_template}.json"
        )
        self.failed_marc_recs_file = (
            self.results_folder / f"failed_records{self.file_template}.mrc"
        )

        self.migration_reports_file = self.reports_folder / f"report{self.file_template}.md"

        self.srs_records_path = (
            self.results_folder / f"folio_srs_{object_type_string}{self.file_template}.json"
        )
        self.organizations_id_map_path = (
            self.results_folder / f"{str(FOLIONamespaces.organizations.name).lower()}_id_map.json"
        )
        self.instance_id_map_path = (
            self.results_folder / f"{str(FOLIONamespaces.instances.name).lower()}_id_map.json"
        )
        self.auth_id_map_path = (
            self.results_folder / f"{str(FOLIONamespaces.authorities.name).lower()}_id_map.json"
        )

        self.holdings_id_map_path = (
            self.results_folder / f"{str(FOLIONamespaces.holdings.name).lower()}_id_map.json"
        )
        self.id_map_path = (
            self.results_folder / f"{str(self.object_type.name).lower()}_id_map.json"
        )
        # Mapping files
        self.material_type_map_path = self.mapping_files_folder / "material_types.tsv"
        self.loan_type_map_path = self.mapping_files_folder / "loan_types.tsv"
        self.temp_loan_type_map_path = self.mapping_files_folder / "temp_loan_types.tsv"
        self.statistical_codes_map_path = self.mapping_files_folder / "statcodes.tsv"
        self.item_statuses_map_path = self.mapping_files_folder / "item_statuses.tsv"

    def verify_folder(self, folder_path: Path):
        if not folder_path.is_dir():
            logging.critical("There is no folder located at %s. Exiting.", folder_path)
            logging.critical("Create a folder by calling\n\tmkdir %s", folder_path)
            sys.exit(1)
        else:
            logging.info("Located %s", folder_path)


def verify_git_ignore(gitignore: Path):
    with open(gitignore, "r+") as f:
        contents = f.read()
        if "results/" not in contents:
            f.write("results/\n")
        if "archive/" not in contents:
            f.write("archive/\n")
        if "source_data/" not in contents:
            f.write("source_data/\n")
        if "*.data" not in contents:
            f.write("*.data\n")
    logging.info("Made sure there was a valid .gitignore file at %s", gitignore)
