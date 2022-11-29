import csv
import ctypes
import json
import logging
import sys
import time
from os.path import isfile
from typing import List
from typing import Optional

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.organization_mapper import (
    OrganizationMapper,
)
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.task_configuration import AbstractTaskConfiguration

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


# Read files and do some work
class OrganizationTransformer(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        name: str
        migration_task_type: str
        files: List[FileDefinition]
        organization_map_path: str
        organization_types_map_path: Optional[str] = ""
        address_categories_map_path: Optional[str] = ""
        email_categories_map_path: Optional[str] = ""
        phone_categories_map_path: Optional[str] = ""

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.organizations

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        csv.register_dialect("tsv", delimiter="\t")

        super().__init__(library_config, task_configuration, use_logging)
        self.object_type_name = self.get_object_type().name
        self.task_configuration = task_configuration
        self.files = self.list_source_files()
        self.total_records = 0

        self.organization_map = self.setup_records_map(
            self.folder_structure.mapping_files_folder
            / self.task_configuration.organization_map_path
        )

        self.results_path = self.folder_structure.created_objects_path
        self.failed_files: List[str] = []

        self.folio_keys = []
        self.folio_keys = MappingFileMapperBase.get_mapped_folio_properties_from_map(
            self.organization_map
        )

        self.mapper = OrganizationMapper(
            self.folio_client,
            self.library_configuration,
            self.organization_map,
            self.load_ref_data_mapping_file(
                "organizationTypes",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.organization_types_map_path,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(
                "addresses[0].categories[0]",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.address_categories_map_path,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(
                "emails[0].categories[0]",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.email_categories_map_path,
                self.folio_keys,
                False,
            ),
            self.load_ref_data_mapping_file(
                "phoneNumbers[0].categories[0]",
                self.folder_structure.mapping_files_folder
                / self.task_configuration.phone_categories_map_path,
                self.folio_keys,
                False,
            ),
        )

    def list_source_files(self):
        files = [
            self.folder_structure.data_folder / self.object_type_name / f.file_name
            for f in self.task_configuration.files
            if isfile(self.folder_structure.data_folder / self.object_type_name / f.file_name)
        ]
        if not any(files):
            ret_str = ",".join(f.file_name for f in self.task_configuration.files)
            raise TransformationProcessError(
                f"Files {ret_str} not found in"
                "{self.folder_structure.data_folder} / {self.object_type_name}"
            )
        logging.info("Files to process:")
        for filename in files:
            logging.info("\t%s", filename)
        return files

    def process_single_file(self, filename):
        with open(filename, encoding="utf-8-sig") as records_file, open(
            self.folder_structure.created_objects_path, "w+"
        ) as results_file:
            self.mapper.migration_report.add_general_statistics("Number of files processed")
            start = time.time()
            records_processed = 0
            for idx, record in enumerate(self.mapper.get_objects(records_file, filename)):
                records_processed += 1

                try:
                    # Print first legacy record, then first transformed record
                    if idx == 0:
                        logging.info("First legacy record:")
                        logging.info(json.dumps(record, indent=4))
                    folio_rec, legacy_id = self.mapper.do_map(
                        record, f"row {idx}", FOLIONamespaces.organizations
                    )

                    clean_folio_rec = self.clean_org(folio_rec)

                    if idx == 0:
                        logging.info("First FOLIO record:")
                        logging.info(json.dumps(clean_folio_rec, indent=4))

                    # Writes record to file
                    Helper.write_to_file(results_file, clean_folio_rec)

                except TransformationProcessError as process_error:
                    self.mapper.handle_transformation_process_error(idx, process_error)
                except TransformationRecordFailedError as error:
                    self.mapper.handle_transformation_record_failed_error(idx, error)
                except Exception as excepion:
                    self.mapper.handle_generic_exception(idx, excepion)

                self.mapper.migration_report.add_general_statistics(
                    "Number of objects in source data file"
                )

                # TODO Rewrite to base % value on number of rows in file
                if idx > 1 and idx % 50 == 0:
                    elapsed = idx / (time.time() - start)
                    elapsed_formatted = "{0:.4g}".format(elapsed)
                    logging.info(  # pylint: disable=logging-fstring-interpolation
                        f"{idx:,} records processed. Recs/sec: {elapsed_formatted} "
                    )

            self.total_records = records_processed

            logging.info(  # pylint: disable=logging-fstring-interpolation
                f"Done processing {filename} containing {self.total_records:,} records. "
                f"Total records processed: {self.total_records:,}"
            )

    def do_work(self):
        logging.info("Getting started!")
        for file in self.files:
            logging.info("Processing %s", file)
            try:
                self.process_single_file(file)
            except Exception as ee:
                error_str = (
                    f"Processing of {file} failed:\n{ee}."
                    "Check source files for empty lines or missing reference data"
                )
                logging.exception(error_str)
                self.mapper.migration_report.add(Blurbs.FailedFiles, f"{file} - {ee}")
                sys.exit()

    def wrap_up(self):
        logging.info("Done. Transformer wrapping up...")
        self.extradata_writer.flush()
        with open(self.folder_structure.migration_reports_file, "w") as migration_report_file:
            logging.info(
                "Writing migration- and mapping report to %s",
                self.folder_structure.migration_reports_file,
            )
            self.mapper.migration_report.write_migration_report(
                "", migration_report_file, self.start_datetime
            )

            Helper.print_mapping_report(
                migration_report_file,
                self.total_records,
                self.mapper.mapped_folio_fields,
                self.mapper.mapped_legacy_fields,
            )
        self.clean_out_empty_logs()

        logging.info("All done!")

    def clean_org(self, folio_rec):

        self.clean_org_type_pre_morning_glory(folio_rec, self.library_configuration.folio_release)
        self.clean_addresses(folio_rec)

        return folio_rec

    def clean_org_type_pre_morning_glory(self, folio_rec, folio_release):
        # Remove the organizationTypes for older releases
        if folio_release in ["lotus"]:
            if folio_rec.get("organizationTypes"):
                del folio_rec["organizationTypes"]
        return folio_rec

    def clean_addresses(self, folio_rec):
        if addresses := folio_rec.get("addresses", []):
            primary_address_exists = False
            empty_addresses = []

            for address in addresses:
                # Check if the address has content
                address_content = {k: v for k, v in address.items() if k != "isPrimary"}
                if not any(address_content.values()):
                    empty_addresses.append(address)

                # Check if the address is primary
                if address["isPrimary"] is True:
                    primary_address_exists = True

            # If none of the existing addresses is pimrary
            # Make the first one primary
            if not primary_address_exists:
                addresses[0]["isPrimary"] = True

            folio_rec["addresses"] = [a for a in addresses if a not in empty_addresses]

            return folio_rec
