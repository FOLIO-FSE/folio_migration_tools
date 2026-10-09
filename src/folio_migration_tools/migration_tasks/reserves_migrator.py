"""Course reserves migration task.

Migrates course reserve records from legacy ILS to FOLIO Course Reserves module.
Handles course listings, items on reserve, and reserve relationships.
"""

import csv
import json
import logging
import sys
import time
from typing import Annotated, Dict

import httpx
import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.circulation_prevalidation import validate_item_barcodes
from folio_migration_tools.custom_dict import InsensitiveDictReader
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration
from folio_migration_tools.transaction_migration.legacy_reserve import LegacyReserve

logger = logging.getLogger(__name__)


def _error_message(resp: httpx.Response) -> str:
    """Return the first FOLIO error message in a response, or the raw body if there is none."""
    try:
        return json.loads(resp.text)["errors"][0]["message"]
    except (ValueError, KeyError, IndexError, TypeError):
        return resp.text[:500]


class ReservesMigrator(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        """Task configuration for ReservesMigrator."""

        name: Annotated[
            str,
            Field(
                title="Migration task name",
                description=(
                    "Name of this migration task. The name is being used to call the specific "
                    "task, and to distinguish tasks of similar types"
                ),
            ),
        ]
        migration_task_type: Annotated[
            str,
            Field(
                title="Migration task type",
                description="The type of migration task you want to perform",
            ),
        ]
        course_reserve_file_path: Annotated[
            FileDefinition,
            Field(
                title="Course reserve file path",
                description="Path to the file with course reserves",
            ),
        ]
        skip_barcode_prevalidation: Annotated[
            bool,
            Field(
                title="Skip barcode pre-validation",
                description=(
                    "Skip pre-validation of item barcodes against FOLIO. "
                    "By default, item barcodes are validated before reserves are posted."
                ),
            ),
        ] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.reserve

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
    ):
        """Initialize ReservesMigrator for migrating course reserves.

        Args:
            task_configuration (TaskConfiguration): Reserves migration configuration.
            library_config (LibraryConfiguration): Library configuration.
            folio_client: FOLIO API client.
        """
        csv.register_dialect("tsv", delimiter="\t")
        self.migration_report = MigrationReport()
        self.valid_reserves = []
        self.semi_valid_reserves = []
        super().__init__(library_config, task_configuration, folio_client)
        self.skip_barcode_prevalidation = task_configuration.skip_barcode_prevalidation
        with open(
            self.folder_structure.legacy_records_folder
            / task_configuration.course_reserve_file_path.file_name,
            "r",
            encoding="utf-8",
        ) as reserves_file:
            self.semi_valid_reserves = list(
                self.load_and_validate_legacy_reserves(
                    InsensitiveDictReader(reserves_file, dialect="tsv")
                )
            )
            logger.info(
                "Loaded and validated %s reserves in file",
                len(self.semi_valid_reserves),
            )
        self.t0 = time.time()
        self.failed: Dict = {}
        logger.info("Init completed")

    async def do_work(self):
        logger.info("Starting")
        self._pre_validate_barcodes()
        with self.folio_client.get_folio_http_client() as self.http_client:
            for num_reserves, legacy_reserve in enumerate(self.valid_reserves, start=1):
                t0_migration = time.time()
                self.migration_report.add_general_statistics(i18n_t("Processed reserves"))
                try:
                    self.post_single_reserve(legacy_reserve)
                except Exception as ee:
                    logger.exception(
                        f"Error in row {num_reserves}  Reserve: {json.dumps(legacy_reserve)} {ee}"
                    )
                if num_reserves % 50 == 0:
                    logger.info(f"{timings(self.t0, t0_migration, num_reserves)} {num_reserves}")

    def post_single_reserve(self, legacy_reserve: LegacyReserve):
        try:
            path = f"/coursereserves/courselistings/{legacy_reserve.course_listing_id}/reserves"
            if self.folio_put_post(
                path, legacy_reserve.to_dict(), "POST", i18n.t("Posted reserves")
            ):
                self.migration_report.add_general_statistics(
                    i18n_t("Successfully posted reserves")
                )
            else:
                self.migration_report.add_general_statistics(i18n_t("Failure to post reserve"))
        except Exception as ee:
            logger.exception(ee)

    async def wrap_up(self):
        self.extradata_writer.flush()
        for k, v in self.failed.items():
            self.failed_and_not_dupe[k] = [v.to_dict()]
        self.migration_report.set("GeneralStatistics", i18n_t("Failed reserves"), len(self.failed))
        self.write_failed_reserves_to_file()

        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                i18n_t("Reserves migration report"), report_file, self.start_datetime
            )
        with open(self.folder_structure.migration_reports_raw_file, "w") as raw_report_file:
            self.migration_report.write_json_report(raw_report_file)
        self.clean_out_empty_logs()

    def write_failed_reserves_to_file(self):
        # POST /coursereserves/courselistings/40a085bd-b44b-42b3-b92f-61894a75e3ce/reserves
        # Match on legacy course number ()

        csv_columns = ["legacy_identifier", "barcode"]
        with open(self.folder_structure.failed_recs_path, "w+") as failed_reserves_file:
            writer = csv.DictWriter(failed_reserves_file, fieldnames=csv_columns, dialect="tsv")
            writer.writeheader()
            for _k, failed_reserve in self.failed.items():
                writer.writerow(failed_reserve[0])

    def _pre_validate_barcodes(self):
        if self.skip_barcode_prevalidation:
            logger.info("Barcode pre-validation is disabled by configuration. Skipping.")
            self.valid_reserves = self.semi_valid_reserves
            return
        logger.info(
            "Performing item barcode pre-validation for %s legacy reserves...",
            len(self.semi_valid_reserves),
        )
        self.valid_reserves = list(self.check_barcodes())
        logger.info("Loaded and validated %s reserves against barcodes", len(self.valid_reserves))

    def check_barcodes(self):
        """Yield reserves whose item barcode exists as an item in FOLIO."""
        item_barcodes = {
            reserve.item_barcode for reserve in self.semi_valid_reserves if reserve.item_barcode
        }
        valid_item_barcodes = validate_item_barcodes(self.folio_client, item_barcodes)
        for reserve in self.semi_valid_reserves:
            if reserve.item_barcode in valid_item_barcodes:
                self.migration_report.add_general_statistics(
                    i18n_t("Reserve verified against migrated item")
                )
                yield reserve
            else:
                self.migration_report.add(
                    "DiscardedReserves",
                    i18n_t("Reserve discarded. Could not find migrated barcode"),
                )

    def load_and_validate_legacy_reserves(self, reserves_reader):
        num_bad = 0
        logger.info("Validating legacy reserves in file...")
        for legacy_reserve_count, legacy_reserve_dict in enumerate(reserves_reader):
            try:
                legacy_reserve = LegacyReserve(
                    legacy_reserve_dict,
                    self.folio_client,
                    legacy_reserve_count,
                )
                if any(legacy_reserve.errors):
                    num_bad += 1
                    self.migration_report.add_general_statistics(i18n.t("Discarded reserves"))
                    for error in legacy_reserve.errors:
                        self.migration_report.add("DiscardedReserves", f"{error[0]} - {error[1]}")
                else:
                    yield legacy_reserve
            except ValueError as ve:
                logger.exception(ve)
        logger.info(
            f"Done validating {legacy_reserve_count} legacy reserves with {num_bad} rotten apples"
        )
        if num_bad / legacy_reserve_count > 0.5:
            q = num_bad / legacy_reserve_count
            logger.error("%s percent of reserves failed to validate.", (q * 100))
            self.migration_report.log_me()
            logger.critical("Halting...")
            sys.exit(1)

    def folio_put_post(self, url, data_dict, verb, action_description=""):
        try:
            if verb == "PUT":
                resp = self.http_client.put(url, json=data_dict)
            elif verb == "POST":
                resp = self.http_client.post(url, json=data_dict)
            else:
                raise TransformationProcessError("Bad verb supplied. This is a code issue.")
            if resp.status_code == 422:
                error_message = _error_message(resp)
                logger.error(error_message)
                self.migration_report.add(
                    "Details",
                    i18n.t(
                        "%{action} error: %{message}",
                        action=action_description,
                        message=error_message,
                    ),
                )
                resp.raise_for_status()
            elif resp.is_success:
                self.migration_report.add(
                    "Details",
                    i18n.t("Successfully %{action}", action=action_description)
                    + f" ({resp.status_code})",
                )
            else:
                self.migration_report.add(
                    "Details",
                    i18n.t(
                        "%{action} error. http status: %{status}",
                        action=action_description,
                        status=resp.status_code,
                    ),
                )
                logger.error(json.dumps(data_dict))
                resp.raise_for_status()
            return True
        except httpx.HTTPStatusError as exception:
            logger.exception(f"{exception.response.status_code}. {verb} FAILED for {url}")
            return False
        except httpx.RequestError:
            logger.exception(f"{verb} FAILED for {url}")
            return False


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )
