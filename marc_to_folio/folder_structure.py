import logging
from pathlib import Path
import time


class FolderStructure:
    def __init__(self, base_path: Path, time_stamp:str):
        self.time_stamp = time_stamp
        self.base_folder = Path(base_path)
        if not self.base_folder.is_dir():
            logging.info(f"Base Folder Path is not a folder. Exiting.")
            exit()

        self.data_folder = self.base_folder / "data"
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
        logging.info(f"Mapping files folder is {self.mapping_files_folder}")
        logging.info("Git ignore is set up correctly")
        logging.info(f"Base folder is {self.base_folder}")
        logging.info(f"Reports and logs folder is {self.reports_folder}")
        logging.info(f"Results folder is {self.results_folder}")        
        logging.info(f"Data folder is {self.data_folder}")
        logging.info(f"Source records files folder is {self.legacy_records_folder}")

    def setup_migration_file_structure(self, object_type:str):
        self.legacy_records_folder = self.data_folder / object_type
        verify_folder(self.legacy_records_folder)
       

        self.transformation_log_path = (
            self.reports_folder / f"{object_type}_transformation_{self.time_stamp}.log"
        )
        logging.info(
            f"Log file will be located at {self.transformation_log_path}"
        )

        self.created_objects_path = (
            self.results_folder / f"folio_{object_type}_{self.time_stamp}.json"
        )
        logging.info(
            f"Created {object_type}s will be stored at  {self.created_objects_path}"
        )

        self.migration_reports_file = (
            self.reports_folder / f"{object_type}_transformation_report_{self.time_stamp}.md"
        )
        logging.info(f"{object_type} migration report file will be saved at {self.migration_reports_file}")

        self.srs_records_path = self.results_folder / f"srs_{self.time_stamp}.json"
        self.instance_id_map_path = self.results_folder / f"instance_id_map_{self.time_stamp}.json"
        self.holdings_from_bibs_path = self.results_folder / f"holdings_from_bibs_{self.time_stamp}.json"

        self.holdings_from_csv_path = self.results_folder / f"holdings_from_csv_{self.time_stamp}.json"
        self.holdings_id_map_path = self.results_folder / f"holdings_id_map_{self.time_stamp}.json"

        
        
        self.holdings_from_mfhd_path = self.results_folder / f"holdings_from_bibs_{self.time_stamp}.json"
        self.holdings_from_c_records_path = self.results_folder / f"holdings_from_bibs_{self.time_stamp}.json"

        # Mapping files
        self.locations_map_path = self.mapping_files_folder / 'locations.tsv'
        self.mfhd_rules_path = self.mapping_files_folder / "mfhd_rules.json"
        self.items_map_path = self.mapping_files_folder / "item_mapping.json"
        self.material_type_map_path = self.mapping_files_folder /"material_types.tsv"
        self.loan_type_map_path = self.mapping_files_folder /"loan_types.tsv"
        self.call_number_type_map_path = self.mapping_files_folder /"call_number_type_mapping.tsv"
        self.statistical_codes_map_path = self.mapping_files_folder /"statcodes.tsv"
        self.item_statuses_map_path = self.mapping_files_folder /"item_statuses.tsv"

        
def verify_git_ignore(gitignore: Path):
    with open(gitignore, "a+") as f:
        contents = f.read()
        if "results/" not in contents:
            f.write("results/\n")
        if "data/" not in contents:
            f.write("data/\n")
        if "*.data" not in contents:
            f.write("*.data/\n")
    logging.info(f"Made sure there was a valid .gitignore file at {gitignore}")


def verify_folder(folder_path: Path):
    if not folder_path.is_dir():
        logging.critical(f"There is no folder located at {folder_path}. Exiting.")
        exit()
