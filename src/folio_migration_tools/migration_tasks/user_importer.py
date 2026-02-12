"""UserImporterTask module for FOLIO user import operations.

This module provides an adapter that wraps folio_data_import.UserImporter
to conform to the folio_migration_tools MigrationTaskBase interface.
It supports importing users with full relationship handling including
request preferences, permission users, and service points.

This provides an alternative to posting users via BatchPoster with the
/user-import endpoint, offering more granular control and better error handling.
"""

import asyncio
import logging
from pathlib import Path
from typing import Annotated, List, Literal

from folio_data_import._progress import RichProgressReporter
from folio_data_import.UserImport import UserImporter as FDIUserImporter
from folio_data_import.UserImport import UserImporterStats
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration

logger = logging.getLogger(__name__)


class UserImportTask(MigrationTaskBase):
    """A wrapper for folio_data_import.UserImporter.

    This class adapts the UserImporter from folio_data_import to fit within the
    folio_migration_tools MigrationTaskBase framework.

    This implementation handles:
    - User create/update with full upsert support
    - Automatic mapping of patron groups, address types, departments, service points
    - Creation/update of request preferences
    - Creation of permission users
    - Creation/update of service points users
    - Field protection to prevent overwriting specific fields

    Parents:
        MigrationTaskBase: Base class for all migration tasks

    Raises:
        TransformationProcessError: When a critical error occurs during processing
        FileNotFoundError: When input files are not found
    """

    class TaskConfiguration(AbstractTaskConfiguration):
        """Task configuration for UserImporter."""

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
                    "List of JSONL files to be processed. These should be output files "
                    "from UserTransformer containing mod-user-import compatible objects."
                ),
            ),
        ]
        batch_size: Annotated[
            int,
            Field(
                title="Batch size",
                description="Number of users to process concurrently in each batch",
                ge=1,
                le=1000,
            ),
        ] = 250
        user_match_key: Annotated[
            Literal["externalSystemId", "username", "barcode"],
            Field(
                title="User match key",
                description=(
                    "The key to use for matching existing users during upsert. "
                    "Users with matching values will be updated rather than created."
                ),
            ),
        ] = "externalSystemId"
        only_update_present_fields: Annotated[
            bool,
            Field(
                title="Only update present fields",
                description=(
                    "When enabled, only fields present in the input will be updated. "
                    "Missing fields will be left unchanged in existing records. "
                    "When disabled, missing fields may be cleared."
                ),
            ),
        ] = False
        default_preferred_contact_type: Annotated[
            Literal["001", "002", "003", "004", "005", "mail", "email", "text", "phone", "mobile"],
            Field(
                title="Default preferred contact type",
                description=(
                    "Default preferred contact type for users. "
                    "Can be specified as ID (001-005) or name (mail/email/text/phone/mobile). "
                    "Will be applied to users without a valid value already set."
                ),
            ),
        ] = "002"
        fields_to_protect: Annotated[
            List[str],
            Field(
                title="Fields to protect",
                description=(
                    "List of field paths to protect from updates "
                    "(e.g., ['personal.email', 'barcode']). "
                    "Protected fields will not be modified during updates."
                ),
            ),
        ] = []
        limit_simultaneous_requests: Annotated[
            int,
            Field(
                title="Limit simultaneous requests",
                description="Maximum number of concurrent async HTTP requests",
                ge=1,
                le=100,
            ),
        ] = 10
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
        return FOLIONamespaces.users

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
        use_logging: bool = True,
    ):
        """Initialize UserImporter for bulk user import via /users APIs.

        Args:
            task_config (TaskConfiguration): User import configuration.
            library_config (LibraryConfiguration): Library configuration.
            folio_client: FOLIO API client.
            use_logging (bool): Whether to set up task logging.
        """
        super().__init__(library_config, task_config, folio_client, use_logging)
        self.migration_report = MigrationReport()
        self.stats: UserImporterStats = UserImporterStats()
        self.total_records = 0
        self.files_processed: List[str] = []

        logger.info("UserImporterTask initialized")
        logger.info("Batch size: %s", self.task_configuration.batch_size)
        logger.info("User match key: %s", self.task_configuration.user_match_key)

    def _create_fdi_config(self, file_paths: List[Path]) -> FDIUserImporter.Config:
        """Create a folio_data_import.UserImporter.Config from our TaskConfiguration.

        Args:
            file_paths: List of file paths to process

        Returns:
            FDIUserImporter.Config: Configuration for the underlying UserImporter
        """
        return FDIUserImporter.Config(
            library_name=self.library_configuration.library_name,
            batch_size=self.task_configuration.batch_size,
            user_match_key=self.task_configuration.user_match_key,
            only_update_present_fields=self.task_configuration.only_update_present_fields,
            default_preferred_contact_type=self.task_configuration.default_preferred_contact_type,
            fields_to_protect=self.task_configuration.fields_to_protect,
            limit_simultaneous_requests=self.task_configuration.limit_simultaneous_requests,
            user_file_paths=file_paths,
            no_progress=self.task_configuration.no_progress,
        )

    async def _do_work_async(self) -> None:
        """Async implementation of the work logic."""
        # Build list of file paths
        file_paths: List[Path] = []
        for file_def in self.task_configuration.files:
            path = self.folder_structure.results_folder / file_def.file_name
            if not path.exists():
                logger.error("File not found: %s", path)
                raise FileNotFoundError(f"File not found: {path}")
            file_paths.append(path)
            self.files_processed.append(file_def.file_name)
            logger.info("Will process file: %s", path)

        # Count total records for reporting
        for file_path in file_paths:
            with open(file_path, "rb") as f:
                self.total_records += sum(
                    buf.count(b"\n") for buf in iter(lambda: f.read(1024 * 1024), b"")
                )

        # Create the folio_data_import UserImporter config
        fdi_config = self._create_fdi_config(file_paths)

        # Create Progress Reporter
        if self.task_configuration.no_progress:
            from folio_data_import._progress import NoOpProgressReporter

            reporter = NoOpProgressReporter()
        else:
            reporter = RichProgressReporter(enabled=True)

        # Error file path
        error_file_path = self.folder_structure.failed_recs_path

        # Create and run the importer
        importer = FDIUserImporter(
            folio_client=self.folio_client,
            config=fdi_config,
            reporter=reporter,
        )

        await importer.setup(error_file_path)

        try:
            await importer.do_import()
            self.stats = importer.stats
        finally:
            await importer.close()

    def do_work(self) -> None:
        """Main work method that processes files and imports users to FOLIO.

        This method reads user records from the configured files and imports them
        to FOLIO using the folio_data_import.UserImporter, handling all related
        objects (request preferences, permission users, service points).
        """
        logger.info("Starting UserImportTask work...")

        try:
            # Run the async work in an event loop
            asyncio.run(self._do_work_async())
        except FileNotFoundError as e:
            logger.error("File not found: %s", e)
            raise
        except Exception as e:
            logger.error("Error during user import: %s", e)
            raise

        logger.info("UserImportTask work complete")

    def _translate_stats_to_migration_report(self) -> None:
        """Translate UserImporterStats to MigrationReport format."""
        # General statistics
        total_processed = self.stats.created + self.stats.updated + self.stats.failed
        self.migration_report.set(
            "GeneralStatistics",
            "Total records in files",
            self.total_records,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Records processed",
            total_processed,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Users created",
            self.stats.created,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Users updated",
            self.stats.updated,
        )
        self.migration_report.set(
            "GeneralStatistics",
            "Users failed",
            self.stats.failed,
        )

        # Add file information
        for file_name in self.files_processed:
            self.migration_report.add("FilesProcessed", file_name)

    def wrap_up(self) -> None:
        """Finalize the migration task and write reports.

        This method translates statistics from the underlying UserImporter
        to the MigrationReport format and writes both markdown and JSON reports.
        """
        logger.info("Done. Wrapping up UserImportTask")

        # Translate stats to migration report
        self._translate_stats_to_migration_report()

        # Log summary
        logger.info("=" * 60)
        logger.info("UserImportTask Summary")
        logger.info("=" * 60)
        logger.info("Total records in files: %d", self.total_records)
        logger.info("Users created: %d", self.stats.created)
        logger.info("Users updated: %d", self.stats.updated)
        logger.info("Users failed: %d", self.stats.failed)
        if self.stats.failed > 0:
            logger.info(
                "Failed users written to: %s",
                self.folder_structure.failed_recs_path,
            )

        # Write markdown report
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                "User import report",
                report_file,
                self.start_datetime,
            )

        # Write raw JSON report
        with open(self.folder_structure.migration_reports_raw_file, "w") as raw_report_file:
            self.migration_report.write_json_report(raw_report_file)

        # Clean up empty log files
        self.clean_out_empty_logs()

        logger.info("UserImportTask wrap up complete")
