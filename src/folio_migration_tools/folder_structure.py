import logging
import sys
from pathlib import Path
import time

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
        logging.info("Setting up folder structure")

        self.object_type: FOLIONamespaces = object_type
        self.migration_task_name = migration_task_name
        self.add_time_stamp_to_file_names = add_time_stamp_to_file_names
        self.iteration_identifier = iteration_identifier
        self.base_folder = Path(base_path)
        if not self.base_folder.is_dir():
            logging.critical("Base Folder Path is not a folder. Exiting.")
            sys.exit()

        self.data_folder = self.base_folder / "data"
        verify_folder(self.data_folder)

        verify_folder(self.data_folder / str(FOLIONamespaces.instances.name).lower())
        verify_folder(self.data_folder / str(FOLIONamespaces.holdings.name).lower())
        verify_folder(self.data_folder / str(FOLIONamespaces.items.name).lower())
        verify_folder(self.data_folder / str(FOLIONamespaces.users.name).lower())
        self.archive_folder = self.base_folder / "archive"
        verify_folder(self.data_folder)

        self.results_folder = self.base_folder / "results"
        verify_folder(self.results_folder)

        self.reports_folder = self.base_folder / "reports"
        verify_folder(self.reports_folder)

        self.mapping_files_folder = self.base_folder / "mapping_files"
        verify_folder(self.mapping_files_folder)
        gitignore = self.base_folder / ".gitignore"
        verify_git_ignore(gitignore)

    def log_folder_structure(self):
        logging.info("Mapping files folder is %s", self.mapping_files_folder)
        logging.info("Git ignore is set up correctly")
        logging.info("Base folder is %s", self.base_folder)
        logging.info("Reports and logs folder is %s", self.reports_folder)
        logging.info("Results folder is %s", self.results_folder)
        logging.info("Data folder is %s", self.data_folder)
        logging.info("Source records files folder is %s", self.legacy_records_folder)
        logging.info("Log file will be located at %s", self.transformation_log_path)
        logging.info(
            "Extra data will be stored at%s", self.transformation_extra_data_path
        )
        logging.info("Data issue reports %s", self.data_issue_file_path)
        logging.info("Created objects will be stored at  %s", self.created_objects_path)
        logging.info(
            "Migration report file will be saved at %s", self.migration_reports_file
        )

    def setup_migration_file_structure(self, source_file_type: str = ""):
        time_str = (
            f'_{time.strftime("%Y%m%d-%H%M%S")}'
            if self.add_time_stamp_to_file_names
            else ""
        )
        file_template = (
            f"{self.iteration_identifier}{time_str}_{self.migration_task_name}"
        )
        object_type_string = str(self.object_type.name).lower()
        if source_file_type:
            self.legacy_records_folder = self.data_folder / source_file_type
        elif self.object_type == FOLIONamespaces.other:
            self.legacy_records_folder = self.data_folder

        else:
            self.legacy_records_folder = self.data_folder / object_type_string
        verify_folder(self.legacy_records_folder)

        self.transformation_log_path = self.reports_folder / (
            f"log_{object_type_string}_{file_template}.log"
        )

        self.transformation_extra_data_path = (
            self.results_folder / f"extradata_{file_template}.extradata"
        )

        self.data_issue_file_path = (
            self.reports_folder
            / f"data_issues_log_{object_type_string}_{file_template}.tsv"
        )
        self.created_objects_path = (
            self.results_folder / f"folio_{object_type_string}_{file_template}.json"
        )

        self.failed_bibs_file = (
            self.results_folder
            / f"failed_bib_records_{self.iteration_identifier}{time_str}.mrc"
        )
        self.failed_mfhds_file = (
            self.results_folder
            / f"failed_mfhd_records_{self.iteration_identifier}{time_str}.mrc"
        )

        self.migration_reports_file = (
            self.reports_folder
            / f"transformation_report_{object_type_string}_{file_template}.md"
        )

        self.srs_records_path = (
            self.results_folder / f"folio_srs_{object_type_string}_{file_template}.json"
        )

        self.instance_id_map_path = (
            self.results_folder / f"instance_id_map_{self.iteration_identifier}.json"
        )

        self.holdings_id_map_path = (
            self.results_folder / f"holdings_id_map_{self.iteration_identifier}.json"
        )

        # Mapping files
        self.temp_locations_map_path = self.mapping_files_folder / "temp_locations.tsv"
        self.material_type_map_path = self.mapping_files_folder / "material_types.tsv"
        self.loan_type_map_path = self.mapping_files_folder / "loan_types.tsv"
        self.temp_loan_type_map_path = self.mapping_files_folder / "temp_loan_types.tsv"
        self.statistical_codes_map_path = self.mapping_files_folder / "statcodes.tsv"
        self.item_statuses_map_path = self.mapping_files_folder / "item_statuses.tsv"


def verify_git_ignore(gitignore: Path):
    with open(gitignore, "r+") as f:
        contents = f.read()
        if "results/" not in contents:
            f.write("results/\n")
        if "archive/" not in contents:
            f.write("archive/\n")
        if "data/" not in contents:
            f.write("data/\n")
        if "*.data" not in contents:
            f.write("*.data\n")
    logging.info("Made sure there was a valid .gitignore file at %s", gitignore)


def verify_folder(folder_path: Path):
    if not folder_path.is_dir():
        logging.critical("There is no folder located at %s. Exiting.", folder_path)
        logging.critical("Create a folder by calling\n\tmkdir %s", folder_path)
        sys.exit()
    else:
        logging.info("Located %s", folder_path)
