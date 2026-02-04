"""MARCImportTask module for FOLIO MARC data import operations.

This module provides an adapter that wraps folio_data_import.MARCImportJob
to conform to the folio_migration_tools MigrationTaskBase interface.
It supports importing MARC records directly into FOLIO using the Data Import
APIs (change-manager), bypassing the need for SRS record creation during MARC transformation.

This provides an alternative workflow for MARC record loading, using FOLIO's
native Data Import capabilities.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Annotated, Dict, List

from folio_data_import._progress import RichProgressReporter
from folio_data_import.MARCDataImport import MARCImportJob as FDIMARCImportJob
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class MARCImportTask(MigrationTaskBase):
    """MARCImportTask.

    An adapter that wraps folio_data_import.MARCImportJob to provide MARC import
    functionality via FOLIO's Data Import APIs while conforming to the
    MigrationTaskBase interface.

    This implementation:
    - Imports MARC records using the change-manager APIs
    - Uses configurable Data Import job profiles
    - Supports MARC record preprocessing
    - Handles large files with optional splitting
    - Tracks job IDs for monitoring in FOLIO

    Parents:
        MigrationTaskBase: Base class for all migration tasks

    Raises:
        TransformationProcessError: When a critical error occurs during processing
        FileNotFoundError: When input files are not found
    """

    class TaskConfiguration(AbstractTaskConfiguration):
        """Task configuration for MARCImportTask."""

        name: Annotated[
            str,
            Field(
                title="Task name",
                description="The name of the task",
            ),
        ]
        migration_task_type: Annotated[
            str,
            Field(
                title="Migration task type",
                description="The type of migration task",
            ),
        ]
        files: Annotated[
            List[FileDefinition],
            Field(
                title="List of files",
                description=(
                    "List of MARC files to be imported. Files should be in binary MARC "
                    "format (.mrc). Located in the source_data folder."
                ),
            ),
        ]
        import_profile_name: Annotated[
            str,
            Field(
                title="Import profile name",
                description=(
                    "The name of the Data Import job profile to use in FOLIO. "
                    "This profile determines how MARC records are processed and "
                    "what FOLIO records are created/updated."
                ),
            ),
        ]
        batch_size: Annotated[
            int,
            Field(
                title="Batch size",
                description="Number of MARC records to include in each batch sent to FOLIO",
                ge=1,
                le=1000,
            ),
        ] = 10
        batch_delay: Annotated[
            float,
            Field(
                title="Batch delay",
                description=(
                    "Number of seconds to wait between record batches. "
                    "Use this to throttle requests if needed."
                ),
                ge=0.0,
            ),
        ] = 0.0
        marc_record_preprocessors: Annotated[
            List[str],
            Field(
                title="MARC record preprocessors",
                description=(
                    "List of preprocessor names to apply to each record before import. "
                    "Preprocessors can modify MARC records before they are sent to FOLIO."
                ),
            ),
        ] = []
        preprocessors_args: Annotated[
            Dict[str, Dict] | str,
            Field(
                title="Preprocessor arguments",
                description=(
                    "Dictionary of arguments to pass to the MARC record preprocessors. "
                    "Keys are preprocessor names, values are dicts of arguments."
                ),
            ),
        ] = {}
        split_files: Annotated[
            bool,
            Field(
                title="Split files",
                description=(
                    "Split each file into smaller jobs of size split_size. "
                    "Useful for very large files that may timeout or be difficult to monitor."
                ),
            ),
        ] = False
        split_size: Annotated[
            int,
            Field(
                title="Split size",
                description="Number of records to include in each split file",
                ge=1,
            ),
        ] = 1000
        split_offset: Annotated[
            int,
            Field(
                title="Split offset",
                description=(
                    "Number of split files to skip before starting processing. "
                    "Useful for resuming a partially completed import."
                ),
                ge=0,
            ),
        ] = 0
        show_file_names_in_data_import_logs: Annotated[
            bool,
            Field(
                title="Show file names in Data Import logs",
                description=(
                    "If true, set the file name for each job in the Data Import logs. "
                    "This makes it easier to identify jobs in the FOLIO UI."
                ),
            ),
        ] = False
        let_summary_fail: Annotated[
            bool,
            Field(
                title="Let summary fail",
                description=(
                    "If true, do not retry or fail the import if the final job summary "
                    "cannot be retrieved. Useful when FOLIO is under heavy load."
                ),
            ),
        ] = False
        skip_summary: Annotated[
            bool,
            Field(
                title="Skip summary",
                description=(
                    "If true, skip fetching the final job summary after import. "
                    "The import will complete but detailed statistics won't be available."
                ),
            ),
        ] = False
        no_progress: Annotated[
            bool,
            Field(
                title="No progress",
                description=(
                    "Disable progress reporting in the console output. "
                    "Set to true for non-interactive/CI environments."
                ),
            ),
        ] = False

    task_configuration: TaskConfiguration

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.instances  # MARC imports primarily create instances

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        """Initialize MarcImport for MARC record import via Data Import APIs.

        Args:
            task_config (TaskConfiguration): MARC import configuration.
            library_config (LibraryConfiguration): Library configuration.
            folio_client: FOLIO API client.
            use_logging (bool): Whether to set up task logging.
        """
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.migration_report = MigrationReport()
        self.total_records_sent = 0
        self.job_ids: List[str] = []
        self.files_processed: List[str] = []

        logging.info("MARCImportTask initialized")
        logging.info("Import profile: %s", self.task_configuration.import_profile_name)
        logging.info("Batch size: %s", self.task_configuration.batch_size)
        logging.info("Results folder: %s", self.folder_structure.results_folder)

    def _create_fdi_config(self, file_paths: List[Path]) -> FDIMARCImportJob.Config:
        """Create a folio_data_import.MARCImportJob.Config from our TaskConfiguration.

        Args:
            file_paths: List of file paths to process

        Returns:
            FDIMARCImportJob.Config: Configuration for the underlying MARCImportJob

        Note:
            The folio_data_import MARCImportJob places error files (bad_marc_records_*.mrc,
            failed_batches_*.mrc) in the parent directory of the first MARC file. Since
            we're reading from results_folder, error files will also be created there.
        """
        # Convert preprocessor list to comma-separated string for folio_data_import
        # folio_data_import expects List[Callable], str (comma-separated), or None
        preprocessors_str = (
            ",".join(self.task_configuration.marc_record_preprocessors)
            if self.task_configuration.marc_record_preprocessors
            else None
        )
        if isinstance(self.task_configuration.preprocessors_args, str):
            with open(
                self.folder_structure.mapping_files_folder
                / self.task_configuration.preprocessors_args,
                "r",
            ) as f:
                preprocessors_args = json.load(f)
        else:
            preprocessors_args = self.task_configuration.preprocessors_args

        return FDIMARCImportJob.Config(
            marc_files=file_paths,
            import_profile_name=self.task_configuration.import_profile_name,
            batch_size=self.task_configuration.batch_size,
            batch_delay=self.task_configuration.batch_delay,
            marc_record_preprocessors=preprocessors_str,
            preprocessors_args=preprocessors_args,
            no_progress=self.task_configuration.no_progress,
            no_summary=self.task_configuration.skip_summary,
            let_summary_fail=self.task_configuration.let_summary_fail,
            split_files=self.task_configuration.split_files,
            split_size=self.task_configuration.split_size,
            split_offset=self.task_configuration.split_offset,
            job_ids_file_path=self.folder_structure.results_folder / "marc_import_job_ids.txt",
            show_file_names_in_data_import_logs=(
                self.task_configuration.show_file_names_in_data_import_logs
            ),
        )

    async def _do_work_async(self) -> None:
        """Async implementation of the work logic."""
        file_paths: List[Path] = []
        for file_def in self.task_configuration.files:
            path = self.folder_structure.results_folder / file_def.file_name
            if not path.exists():
                logging.error("File not found: %s", path)
                raise FileNotFoundError(f"File not found: {path}")
            file_paths.append(path)
            self.files_processed.append(file_def.file_name)
            logging.info("Will process file: %s", path)

        # Create the folio_data_import MARCImportJob config
        fdi_config = self._create_fdi_config(file_paths)

        # Create progress reporter
        if self.task_configuration.no_progress:
            from folio_data_import._progress import NoOpProgressReporter

            reporter = NoOpProgressReporter()
        else:
            reporter = RichProgressReporter(enabled=True)

        # Create and run the importer
        # folio_data_import handles its own error files and progress reporting
        importer = FDIMARCImportJob(
            folio_client=self.folio_client,
            config=fdi_config,
            reporter=reporter,
        )

        await importer.do_work()
        await importer.wrap_up()

        # Capture stats and job IDs from the importer
        self.total_records_sent = importer.total_records_sent
        self.job_ids = importer.job_ids

        # Note: Detailed stats (created/updated/discarded/error) are retrieved from
        # the job summary by folio_data_import and logged via log_job_summary().
        # We don't have direct access to those stats as they're logged, not returned.

    def do_work(self) -> None:
        """Main work method that processes MARC files and imports them to FOLIO.

        This method reads MARC records from the configured files and imports them
        to FOLIO using the Data Import APIs via folio_data_import.MARCImportJob.
        """
        logging.info("Starting MARCImportTask work...")

        try:
            # Run the async work in an event loop
            asyncio.run(self._do_work_async())
        except FileNotFoundError as e:
            logging.error("File not found: %s", e)
            raise
        except Exception as e:
            logging.error("Error during MARC import: %s", e)
            raise

        logging.info("MARCImportTask work complete")

    def _translate_stats_to_migration_report(self) -> None:
        """Translate MARC import stats to MigrationReport format.

        Note:
            Detailed stats (created, updated, discarded, error) are retrieved from
            the FOLIO job summary and logged by folio_data_import's log_job_summary().
            We report what we can track directly: records sent and job IDs.
        """
        # General statistics
        self.migration_report.set(
            "GeneralStatistics",
            "Records sent to Data Import",
            self.total_records_sent,
        )

        self.migration_report.set(
            "GeneralStatistics",
            "Data Import jobs created",
            len(self.job_ids),
        )

        # Add file information
        for file_name in self.files_processed:
            self.migration_report.add("FilesProcessed", file_name)

    def wrap_up(self) -> None:
        """Finalize the migration task and write reports.

        This method translates statistics to the MigrationReport format and writes
        both markdown and JSON reports. Error files created by folio_data_import
        (bad_marc_records_*.mrc, failed_batches_*.mrc) are already in results_folder
        since that's where we read the input files from.
        """
        logging.info("Done. Wrapping up MARCImportTask")

        # Translate stats to migration report
        self._translate_stats_to_migration_report()

        # Log summary
        logging.info("=" * 60)
        logging.info("MARCImportTask Summary")
        logging.info("=" * 60)
        logging.info("Records sent to Data Import: %d", self.total_records_sent)
        logging.info("Files processed: %d", len(self.files_processed))
        logging.info("Data Import jobs created: %d", len(self.job_ids))

        # Write markdown report
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                "MARC Data Import report",
                report_file,
                self.start_datetime,
            )

        # Write raw JSON report
        with open(self.folder_structure.migration_reports_raw_file, "w") as raw_report_file:
            self.migration_report.write_json_report(raw_report_file)

        # Clean up empty log files
        self.clean_out_empty_logs()

        logging.info("MARCImportTask wrap up complete")
