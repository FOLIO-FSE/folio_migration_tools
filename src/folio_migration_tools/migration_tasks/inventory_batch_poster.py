"""
BatchPosterV2 module for FOLIO inventory batch operations.

This module provides an adapter that wraps folio_data_import.BatchPoster
to conform to the folio_migration_tools MigrationTaskBase interface.
It supports posting Instances, Holdings, Items, and ShadowInstances
to FOLIO's inventory storage endpoints with support for upsert operations.

This is intended to eventually replace the existing BatchPoster implementation.
"""

import asyncio
import logging
from typing import Annotated, List, Literal

from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_data_import.BatchPoster import BatchPoster as FDIBatchPoster
from folio_data_import.BatchPoster import BatchPosterStats

from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration
from folio_data_import._progress import RichProgressReporter


class InventoryBatchPoster(MigrationTaskBase):
    """InventoryBatchPoster

    An adapter that wraps folio_data_import.BatchPoster to provide batch posting
    functionality for Instances, Holdings, Items, and ShadowInstances while
    conforming to the MigrationTaskBase interface.

    This implementation uses async operations internally and provides improved
    error handling, progress reporting, and upsert capabilities.

    Parents:
        MigrationTaskBase: Base class for all migration tasks

    Raises:
        TransformationProcessError: When a critical error occurs during processing
        TransformationRecordFailedError: When individual records fail to post
    """

    class TaskConfiguration(AbstractTaskConfiguration):
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
        object_type: Annotated[
            Literal["Instances", "Holdings", "Items", "ShadowInstances"],
            Field(
                title="Object type",
                description=(
                    "The type of inventory object to post: Instances, Holdings, Items, "
                    "or ShadowInstances (for consortium shadow copies)"
                ),
            ),
        ]
        files: Annotated[
            List[FileDefinition],
            Field(
                title="List of files",
                description="List of files to be processed from the results folder",
            ),
        ]
        batch_size: Annotated[
            int,
            Field(
                title="Batch size",
                description="Number of records to include in each batch (1-1000)",
                ge=1,
                le=1000,
            ),
        ] = 100
        upsert: Annotated[
            bool,
            Field(
                title="Upsert",
                description=(
                    "Enable upsert mode to create new records or update existing ones. "
                    "When enabled, records with matching IDs will be updated instead "
                    "of causing errors."
                ),
            ),
        ] = False
        preserve_statistical_codes: Annotated[
            bool,
            Field(
                title="Preserve statistical codes",
                description=(
                    "Preserve existing statistical codes during upsert. "
                    "When enabled, statistical codes from existing records will be retained "
                    "and merged with new codes."
                ),
            ),
        ] = False
        preserve_administrative_notes: Annotated[
            bool,
            Field(
                title="Preserve administrative notes",
                description=(
                    "Preserve existing administrative notes during upsert. "
                    "When enabled, administrative notes from existing records will be retained "
                    "and merged with new notes."
                ),
            ),
        ] = False
        preserve_temporary_locations: Annotated[
            bool,
            Field(
                title="Preserve temporary locations",
                description=(
                    "Preserve temporary location assignments on items during upsert. "
                    "Only applicable when object_type is 'Items'."
                ),
            ),
        ] = False
        preserve_temporary_loan_types: Annotated[
            bool,
            Field(
                title="Preserve temporary loan types",
                description=(
                    "Preserve temporary loan type assignments on items during upsert. "
                    "Only applicable when object_type is 'Items'."
                ),
            ),
        ] = False
        preserve_item_status: Annotated[
            bool,
            Field(
                title="Preserve item status",
                description=(
                    "Preserve item status during upsert. When enabled, the status "
                    "field from existing records will be retained. Only applicable "
                    "when object_type is 'Items'."
                ),
            ),
        ] = True
        patch_existing_records: Annotated[
            bool,
            Field(
                title="Patch existing records",
                description=(
                    "Enable selective field patching during upsert. When enabled, only fields "
                    "specified in patch_paths will be updated, preserving all other fields."
                ),
            ),
        ] = False
        patch_paths: Annotated[
            List[str],
            Field(
                title="Patch paths",
                description=(
                    "List of field paths to patch during upsert "
                    "(e.g., ['barcode', 'status']). "
                    "If empty and patch_existing_records is True, all fields "
                    "will be patched. Use this to selectively update only "
                    "specific fields while preserving others."
                ),
            ),
        ] = []
        rerun_failed_records: Annotated[
            bool,
            Field(
                title="Rerun failed records",
                description=(
                    "After the main run, reprocess any failed records one at a time. "
                    "This gives each record a second chance with individual error handling."
                ),
            ),
        ] = True
        no_progress: Annotated[
            bool,
            Field(
                title="No progress",
                description="Disable progress reporting in the console output.",
            ),
        ] = False

    task_configuration: TaskConfiguration

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.other

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.migration_report = MigrationReport()
        self.stats: BatchPosterStats = BatchPosterStats()
        self.batch_errors: List[str] = []

        logging.info("InventoryBatchPoster initialized")
        logging.info("Object type: %s", self.task_configuration.object_type)
        logging.info("Batch size: %s", self.task_configuration.batch_size)
        logging.info("Upsert mode: %s", "On" if self.task_configuration.upsert else "Off")

    def _create_fdi_config(self) -> FDIBatchPoster.Config:
        """
        Create a folio_data_import.BatchPoster.Config from our TaskConfiguration.

        Returns:
            FDIBatchPoster.Config: Configuration for the underlying BatchPoster
        """
        return FDIBatchPoster.Config(
            object_type=self.task_configuration.object_type,
            batch_size=self.task_configuration.batch_size,
            upsert=self.task_configuration.upsert,
            preserve_statistical_codes=self.task_configuration.preserve_statistical_codes,
            preserve_administrative_notes=self.task_configuration.preserve_administrative_notes,
            preserve_temporary_locations=self.task_configuration.preserve_temporary_locations,
            preserve_temporary_loan_types=self.task_configuration.preserve_temporary_loan_types,
            preserve_item_status=self.task_configuration.preserve_item_status,
            patch_existing_records=self.task_configuration.patch_existing_records,
            patch_paths=self.task_configuration.patch_paths or None,
            rerun_failed_records=self.task_configuration.rerun_failed_records,
        )

    def _on_batch_error(self, batch: list, error_message: str) -> None:
        """
        Callback for batch errors to capture in migration report.

        Args:
            batch: The batch of records that failed
            error_message: The error message
        """
        self.batch_errors.append(error_message)
        self.migration_report.add("Details", error_message)

    async def _do_work_async(self) -> None:
        """
        Async implementation of the work logic.
        """
        # Build list of file paths
        file_paths = []
        for file_def in self.task_configuration.files:
            path = self.folder_structure.results_folder / file_def.file_name
            if not path.exists():
                logging.error("File not found: %s", path)
                raise FileNotFoundError(f"File not found: {path}")
            file_paths.append(path)
            logging.info("Will process file: %s", path)

        # Create the folio_data_import BatchPoster config
        fdi_config = self._create_fdi_config()

        # Create the Progress Reporter
        if self.task_configuration.no_progress:
            from folio_data_import._progress import NoOpProgressReporter

            reporter = NoOpProgressReporter()
        else:
            reporter = RichProgressReporter(enabled=True)

        # Create the poster with our failed records path
        failed_records_path = self.folder_structure.failed_recs_path

        async with self.folio_client:
            poster = FDIBatchPoster(
                folio_client=self.folio_client,
                config=fdi_config,
                failed_records_file=failed_records_path,
                reporter=reporter,
            )

            async with poster:
                # Process all files
                self.stats = await poster.do_work(file_paths)

                # If rerun is enabled and there are failures, reprocess them
                if self.task_configuration.rerun_failed_records and self.stats.records_failed > 0:
                    logging.info(
                        "Rerunning %s failed records one at a time",
                        self.stats.records_failed,
                    )
                    await poster.rerun_failed_records_one_by_one()
                    # Update stats after rerun
                    self.stats = poster.get_stats()

    def do_work(self) -> None:
        """
        Main work method that processes files and posts records to FOLIO.

        This method reads records from the configured files and posts them
        to FOLIO in batches using the folio_data_import.BatchPoster.
        """
        logging.info("Starting InventoryBatchPoster work...")

        try:
            # Run the async work in an event loop
            asyncio.run(self._do_work_async())
        except FileNotFoundError as e:
            logging.error("File not found: %s", e)
            raise
        except Exception as e:
            logging.error("Error during batch posting: %s", e)
            raise

        logging.info("InventoryBatchPoster work complete")

    def _translate_stats_to_migration_report(self) -> None:
        """
        Translate BatchPosterStats to MigrationReport format.
        """
        # General statistics
        self.migration_report.set(
            "GeneralStatistics",
            "Records processed",
            self.stats.records_processed,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Records posted successfully",
            self.stats.records_posted,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Records created",
            self.stats.records_created,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Records updated",
            self.stats.records_updated,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Records failed",
            self.stats.records_failed,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Batches posted",
            self.stats.batches_posted,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Batches failed",
            self.stats.batches_failed,
        )

        # Rerun statistics if applicable
        if self.task_configuration.rerun_failed_records:
            self.migration_report.set(
                "GeneralStatistics",
                "Rerun succeeded",
                self.stats.rerun_succeeded,
            )
            self.migration_report.set(
                "GeneralStatistics",
                "Rerun still failed",
                self.stats.rerun_still_failed,
            )

        # Add file information
        for file_def in self.task_configuration.files:
            self.migration_report.add("FilesProcessed", file_def.file_name)

    def wrap_up(self) -> None:
        """
        Finalize the migration task and write reports.

        This method translates statistics from the underlying BatchPoster
        to the MigrationReport format and writes both markdown and JSON reports.
        """
        logging.info("Done. Wrapping up InventoryBatchPoster")

        # Translate stats to migration report
        self._translate_stats_to_migration_report()

        # Log summary
        logging.info("=" * 60)
        logging.info("InventoryBatchPoster Summary")
        logging.info("=" * 60)
        logging.info("Records processed: %d", self.stats.records_processed)
        logging.info("Records posted: %d", self.stats.records_posted)
        logging.info("Records created: %d", self.stats.records_created)
        logging.info("Records updated: %d", self.stats.records_updated)
        logging.info("Records failed: %d", self.stats.records_failed)
        if self.task_configuration.rerun_failed_records:
            logging.info("Rerun succeeded: %d", self.stats.rerun_succeeded)
            logging.info("Rerun still failed: %d", self.stats.rerun_still_failed)
        if self.stats.records_failed > 0:
            logging.info(
                "Failed records written to: %s",
                self.folder_structure.failed_recs_path,
            )

        # Write markdown report
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                f"{self.task_configuration.object_type} loading report",
                report_file,
                self.start_datetime,
            )

        # Write raw JSON report
        with open(self.folder_structure.migration_reports_raw_file, "w") as raw_report_file:
            self.migration_report.write_json_report(raw_report_file)

        # Clean up empty log files
        self.clean_out_empty_logs()
