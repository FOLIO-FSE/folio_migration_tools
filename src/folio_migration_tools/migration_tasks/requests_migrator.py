"""Request records migration task.

Migrates patron requests from legacy ILS to FOLIO. Validates patron and item
barcodes, handles request types and statuses, and maintains request dates.
"""

import asyncio
import csv
import json
import logging
import sys
import time
from collections.abc import AsyncGenerator
from typing import Annotated, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import folioclient
import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioValidationError
from pydantic import Field

from folio_migration_tools.circulation_helper import CirculationHelper
from folio_migration_tools.custom_dict import InsensitiveDictReader
from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    get_from_path,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration
from folio_migration_tools.transaction_migration.legacy_request import LegacyRequest

logger = logging.getLogger(__name__)


class RequestsMigrator(MigrationTaskBase):
    class TaskConfiguration(AbstractTaskConfiguration):
        """Task configuration for RequestsMigrator."""

        name: Annotated[
            str,
            Field(
                title="Task name",
                description=(
                    "Name of this migration task. The name is being used to call "
                    "the specific task, and to distinguish tasks of similar types"
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
        open_requests_file: Annotated[
            FileDefinition,
            Field(
                title="Open requests file",
                description="File with list of open requests",
            ),
        ]
        starting_row: Annotated[
            Optional[int],
            Field(
                title="Starting row",
                description=(
                    "Row number to start processing data from. Optional, by default is first row"
                ),
            ),
        ] = 1
        skip_barcode_prevalidation: Annotated[
            Optional[bool],
            Field(
                title="Skip barcode pre-validation",
                description=(
                    "Skip pre-validation of patron and item barcodes against FOLIO. "
                    "By default, barcodes are validated before request migration."
                ),
            ),
        ] = False

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.requests

    def __init__(
        self,
        task_configuration: TaskConfiguration,
        library_config: LibraryConfiguration,
        folio_client,
    ):
        """Initialize RequestsMigrator for migrating circulation requests.

        Args:
            task_configuration (TaskConfiguration): Requests migration configuration.
            library_config (LibraryConfiguration): Library configuration.
            folio_client: FOLIO API client.
        """
        csv.register_dialect("tsv", delimiter="\t")
        self.migration_report = MigrationReport()
        self.valid_legacy_requests = []
        super().__init__(library_config, task_configuration, folio_client)
        self.circulation_helper = CirculationHelper(
            self.folio_client,
            "",
            self.migration_report,
        )
        self.valid_patron_map = {}
        self.valid_item_barcodes = set()
        try:
            logger.info("Attempting to retrieve tenant timezone configuration...")
            my_path = (
                "/configurations/entries?query=(module==ORG%20and%20configName==localeSettings)"
            )
            self.tenant_timezone_str = json.loads(
                self.folio_client.folio_get_single_object(my_path)["configs"][0]["value"]
            )["timezone"]
            logger.info("Tenant timezone is: %s", self.tenant_timezone_str)
        except Exception:
            logger.info('Tenant locale settings not available. Using "UTC".')
            self.tenant_timezone_str = "UTC"
        self.tenant_timezone = ZoneInfo(self.tenant_timezone_str)
        other_circulation_settings_endpoint = (
            "/configurations/entries?query=(module==CHECKOUT%20and%20configName==other_settings)"
        )
        default_patron_identifiers = ["barcode"]
        try:
            other_circulation_settings = (
                self.folio_client.folio_get_single_object(other_circulation_settings_endpoint)
                or {}
            )
            settings_value = other_circulation_settings.get("configs", [{}])[0].get("value", "{}")
            parsed_settings = json.loads(settings_value)
            patron_identifier_config = parsed_settings.get(
                "prefPatronIdentifier", default_patron_identifiers
            )
            self.patron_identifiers = self._normalize_identifier_fields(patron_identifier_config)
            if not self.patron_identifiers:
                self.patron_identifiers = default_patron_identifiers
            logger.info(
                "Patron lookup identifiers available for this tenant: %s",
                ", ".join(self.patron_identifiers),
            )
        except (ValueError, KeyError, TypeError, IndexError, folioclient.FolioClientError) as e:
            if hasattr(e, "response"):
                logger.exception("Error retrieving circulation settings: %s", e.response.text)
            else:
                logger.exception("Error retrieving circulation settings: %s", str(e))
            self.patron_identifiers = default_patron_identifiers
        with open(
            self.folder_structure.legacy_records_folder
            / task_configuration.open_requests_file.file_name,
            "r",
            encoding="utf-8",
        ) as requests_file:
            self.semi_valid_legacy_requests = list(
                self.load_and_validate_legacy_requests(
                    InsensitiveDictReader(requests_file, dialect="tsv")
                )
            )
            logger.info(
                "Loaded and validated %s requests in file",
                len(self.semi_valid_legacy_requests),
            )

        self.t0 = time.time()
        self.skipped_since_already_added = 0
        self.failed_requests = set()
        logger.info("Starting row is %s", task_configuration.starting_row)
        logger.info("Init completed")

    @staticmethod
    def _normalize_identifier_fields(identifier_config: object) -> list[str]:
        if isinstance(identifier_config, str):
            return [p.strip() for p in identifier_config.split(",") if p and p.strip()]
        if isinstance(identifier_config, list):
            normalized = []
            for val in identifier_config:
                normalized.extend(RequestsMigrator._normalize_identifier_fields(val))
            return normalized
        if isinstance(identifier_config, dict):
            normalized = []
            for val in identifier_config.values():
                normalized.extend(RequestsMigrator._normalize_identifier_fields(val))
            return normalized
        return []

    @staticmethod
    def _flatten_identifier_values(value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (str, int, float, bool)):
            text = str(value).strip()
            return [text] if text else []
        if isinstance(value, list):
            flattened = []
            for val in value:
                flattened.extend(RequestsMigrator._flatten_identifier_values(val))
            return flattened
        if isinstance(value, dict):
            flattened = []
            for key in ["barcode", "value", "id", "identifier", "externalSystemId"]:
                if key in value:
                    flattened.extend(RequestsMigrator._flatten_identifier_values(value[key]))
            if flattened:
                return flattened
            for nested in value.values():
                flattened.extend(RequestsMigrator._flatten_identifier_values(nested))
            return flattened
        return []

    def _get_patron_lookup_value(self, patron: dict, original_barcode: str) -> str | None:
        candidate_paths = ["barcode", *self.patron_identifiers]
        for path in candidate_paths:
            value = get_from_path(patron, path, None)
            if value is None and path in patron:
                value = patron.get(path)
            values = self._flatten_identifier_values(value)
            if values:
                if original_barcode in values:
                    return original_barcode
                return values[0]
        return None

    async def _pre_validate_barcodes(self):
        if self.task_configuration.skip_barcode_prevalidation:
            logger.info("Barcode pre-validation is disabled by configuration. Skipping.")
            self.valid_legacy_requests = self.semi_valid_legacy_requests
        else:
            logger.info(
                "Performing barcode pre-validation for %s legacy requests...",
                len(self.semi_valid_legacy_requests),
            )
            self.valid_legacy_requests = []
            async for request in self.check_barcodes():
                self.valid_legacy_requests.append(request)
            logger.info(
                "Loaded and validated %s requests against barcodes",
                len(self.valid_legacy_requests),
            )
        self.valid_legacy_requests.sort(key=lambda x: x.request_date)
        logger.info("Sorted the list of requests by request date")

    def prepare_legacy_request(self, legacy_request: LegacyRequest):
        patron = self.circulation_helper.get_user_by_barcode(legacy_request.patron_barcode)
        self.migration_report.add_general_statistics(i18n_t("Patron lookups performed"))

        if not patron:
            logger.error(f"No user with barcode {legacy_request.patron_barcode} found in FOLIO")
            Helper.log_data_issue(
                f"{legacy_request.patron_barcode}",
                "No user with barcode.",
                f"{legacy_request.patron_barcode}",
            )
            self.migration_report.add_general_statistics(
                i18n_t("No user with barcode found in FOLIO")
            )
            self.failed_requests.add(legacy_request)
            return False, legacy_request
        legacy_request.patron_id = patron.get("id")

        item = self.circulation_helper.get_item_by_barcode(legacy_request.item_barcode)
        self.migration_report.add_general_statistics(i18n_t("Item lookups performed"))
        if not item:
            logger.error(f"No item with barcode {legacy_request.item_barcode} found in FOLIO")
            self.migration_report.add_general_statistics(
                i18n_t("No item with barcode found in FOLIO")
            )
            Helper.log_data_issue(
                f"{legacy_request.item_barcode}",
                "No item with barcode",
                f"{legacy_request.item_barcode}",
            )
            self.failed_requests.add(legacy_request)
            return False, legacy_request
        holding = self.circulation_helper.get_holding_by_uuid(item.get("holdingsRecordId"))
        self.migration_report.add_general_statistics(i18n_t("Holdings lookups performed"))
        legacy_request.item_id = item.get("id")
        legacy_request.holdings_record_id = item.get("holdingsRecordId")
        legacy_request.instance_id = holding.get("instanceId")
        if item["status"]["name"] in ["Available"]:
            legacy_request.request_type = "Page"
            logger.info(f"Setting request to Page, since the status is {item['status']['name']}")
        self.migration_report.add_general_statistics(
            i18n_t("Valid, prepared requests, ready for posting")
        )
        return True, legacy_request

    async def do_work(self):
        await self._pre_validate_barcodes()
        logger.info("Starting")
        if self.task_configuration.starting_row > 1:
            logger.info(f"Skipping {(self.task_configuration.starting_row - 1)} records")
        num_requests = 0
        for num_requests, legacy_request in enumerate(
            self.valid_legacy_requests[self.task_configuration.starting_row - 1 :],
            start=1,
        ):
            t0_migration = time.time()
            try:
                res, legacy_request = self.prepare_legacy_request(legacy_request)
                if res:
                    logger.debug(json.dumps(legacy_request.serialize(), indent=2))
                    success = self._create_request_with_inactive_user_retry(legacy_request)
                    if success:
                        self.migration_report.add_general_statistics(
                            i18n_t("Successfully migrated requests")
                        )
                    else:
                        self.migration_report.add_general_statistics(
                            i18n_t("Unsuccessfully migrated requests")
                        )
                        self.failed_requests.add(legacy_request)
                if num_requests == 1:
                    logger.info(json.dumps(legacy_request.to_dict(), indent=4))
            except Exception:
                logger.exception(
                    "Error in row %s  Item barcode: %s Patron barcode: %s",
                    num_requests,
                    legacy_request.item_barcode,
                    legacy_request.patron_barcode,
                )
                sys.exit(1)
            if num_requests % 10 == 0:
                logger.info(f"{timings(self.t0, t0_migration, num_requests)} {num_requests}")
        if num_requests > 0:
            logger.info(f"{timings(self.t0, t0_migration, num_requests)} {num_requests}")
        else:
            logger.info("No requests to process after pre-validation")

    async def wrap_up(self):
        self.extradata_writer.flush()
        self.write_failed_request_to_file()

        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                i18n_t("Requests migration report"), report_file, self.start_datetime
            )
        with open(self.folder_structure.migration_reports_raw_file, "w") as raw_report_file:
            self.migration_report.write_json_report(raw_report_file)
        self.clean_out_empty_logs()

    def write_failed_request_to_file(self):
        csv_columns = [
            "item_barcode",
            "patron_barcode",
            "request_date",
            "request_expiration_date",
            "comment",
            "request_type",
            "pickup_servicepoint_id",
        ]
        with open(self.folder_structure.failed_recs_path, "w+") as failed_requests_file:
            writer = csv.DictWriter(failed_requests_file, fieldnames=csv_columns, dialect="tsv")
            writer.writeheader()
            failed: LegacyRequest
            for failed in self.failed_requests:
                writer.writerow(failed.to_source_dict())

    def _create_request_with_inactive_user_retry(self, legacy_request: LegacyRequest) -> bool:
        """Create a request, retrying with user activation if inactive user error occurs.

        Args:
            legacy_request (LegacyRequest): The request to create.

        Returns:
            bool: True if request was successfully created, False otherwise.
        """
        try:
            return self.circulation_helper.create_request(
                self.folio_client, legacy_request, self.migration_report
            )
        except FolioValidationError as fve:
            error_response = self.folio_client.handle_json_response(fve.response)
            error_message = error_response.get("errors", [{}])[0].get("message", "")
            if "Inactive users cannot make requests" in error_message:
                logger.info(
                    "Inactive user detected. Attempting to reactivate for request creation."
                )
                return self._retry_request_for_inactive_user(legacy_request)
            return False

    def _retry_request_for_inactive_user(self, legacy_request: LegacyRequest) -> bool:
        """Retry request creation for inactive user by temporarily activating.

        Args:
            legacy_request (LegacyRequest): The request to create.

        Returns:
            bool: True if request was successfully created, False otherwise.
        """
        try:
            user = self.get_user_by_barcode(legacy_request.patron_barcode)
            if not user:
                return False
            original_expiration = user.get("expirationDate")
            user["expirationDate"] = (datetime.now() + timedelta(days=1)).isoformat()
            self.activate_user(user)
            logger.debug("Temporarily activated user for request creation")
            success = self.circulation_helper.create_request(
                self.folio_client, legacy_request, self.migration_report
            )
            if success:
                self.migration_report.add("Details", i18n_t("Handled inactive users"))
            self.deactivate_user(user, original_expiration)
            logger.debug("Deactivated user again")
            return success
        except Exception:
            logger.exception(
                "Error handling inactive user for request: %s",
                legacy_request.patron_barcode,
            )
            return False

    def activate_user(self, user: dict):
        """Activate a user by setting active=True.

        Args:
            user (dict): User object to activate.
        """
        user["active"] = True
        self.update_user(user)
        self.migration_report.add("Details", i18n_t("Successfully activated user"))

    def deactivate_user(self, user: dict, expiration_date: str | None):
        """Deactivate a user by setting active=False.

        Args:
            user (dict): User object to deactivate.
            expiration_date (str | None): Original expiration date to restore.
        """
        if expiration_date:
            user["expirationDate"] = expiration_date
        user["active"] = False
        self.update_user(user)
        self.migration_report.add("Details", i18n_t("Successfully deactivated user"))

    def update_user(self, user: dict):
        """Update a user via FOLIO API.

        Args:
            user (dict): User object to update.
        """
        url = f"/users/{user['id']}"
        self.folio_client.folio_put(url, user)

    def get_user_by_barcode(self, barcode: str) -> dict | None:
        """Fetch a user by barcode using direct HTTP client.

        Args:
            barcode (str): User barcode to search for.

        Returns:
            dict | None: User object if found, None otherwise.
        """
        try:
            query = f'barcode=="{barcode}"'
            users = self.folio_client.folio_get("/users", "users", query=query)
            return users[0] if users else None
        except Exception as e:
            logger.exception("Error fetching user by barcode %s: %s", barcode, str(e))
            return None

    async def pre_validate_patron_barcodes_async(self, max_concurrent: int = 10):
        request_barcodes = {
            request.patron_barcode
            for request in self.semi_valid_legacy_requests
            if request.patron_barcode
        }
        logger.info("Pre-validating %s unique patron barcodes (async)", len(request_barcodes))
        semaphore = asyncio.Semaphore(max_concurrent)
        self.valid_patron_map = {}
        counter = 0
        num_invalid = 0

        async def check_one(barcode: str):
            nonlocal num_invalid
            nonlocal counter
            query = " OR ".join(f"{field.strip()}=={barcode}" for field in self.patron_identifiers)
            async with semaphore:
                try:
                    fetch_patron = await self.folio_client.folio_get_async(
                        "/users", key="users", query=query
                    )
                except Exception as e:
                    if hasattr(e, "response"):
                        logger.exception(
                            "Error fetching patron for barcode %s: %s", barcode, e.response.text
                        )
                    else:
                        logger.exception(
                            "Error fetching patron for barcode %s: %s",
                            barcode,
                            str(e),
                        )
                    fetch_patron = []
                counter += 1
            if not fetch_patron:
                logger.warning("No patron found for barcode: %s", barcode)
                Helper.log_data_issue_failed(
                    "",
                    "No patron found for barcode",
                    f"Barcode: {barcode}",
                )
                num_invalid += 1
            elif len(fetch_patron) > 1:
                logger.warning("Multiple patrons found for barcode: %s", barcode)
                Helper.log_data_issue_failed(
                    "",
                    "Multiple patrons found for barcode",
                    f"Barcode: {barcode} - {json.dumps(fetch_patron)}",
                )
                num_invalid += 1
            else:
                patron_lookup_value = self._get_patron_lookup_value(fetch_patron[0], barcode)
                if patron_lookup_value:
                    self.valid_patron_map[barcode] = patron_lookup_value
                else:
                    logger.warning(
                        "Patron exists but has no lookupable identifier value: %s",
                        barcode,
                    )
                    Helper.log_data_issue(
                        "",
                        "Fetched patron has no lookupable identifier value",
                        f"Barcode: {barcode} - {json.dumps(fetch_patron)}",
                    )
                    num_invalid += 1
            if counter % 100 == 0:
                logger.info(
                    "Pre-validation progress: %s/%s barcodes checked. %s valid, %s not found.",
                    counter,
                    len(request_barcodes),
                    len(self.valid_patron_map),
                    num_invalid,
                )

        tasks = [check_one(bc) for bc in request_barcodes]
        await asyncio.gather(*tasks)
        logger.info(
            "Pre-validation progress: %s/%s barcodes checked. %s valid, %s not found.",
            counter,
            len(request_barcodes),
            len(self.valid_patron_map),
            num_invalid,
        )

    def pre_validate_item_barcodes(self, batch_size: int = 1000):
        request_barcodes = {
            request.item_barcode
            for request in self.semi_valid_legacy_requests
            if request.item_barcode
        }
        logger.info("Pre-validating item barcodes for %s unique barcodes", len(request_barcodes))
        logger.info(
            "Fetching items matching request barcodes via /item-storage/items/retrieve endpoint..."
        )
        fetch_items = []
        barcode_list = list(request_barcodes)
        for i in range(0, len(barcode_list), batch_size):
            batch = barcode_list[i : i + batch_size]
            try:
                response = self.folio_client.folio_post(  # type: ignore[misc]
                    "/item-storage/items/retrieve",
                    {
                        "query": " OR ".join([f'barcode=="{barcode}"' for barcode in batch]),
                        "limit": len(batch),
                    },
                )
                if not isinstance(response, dict):
                    response = {}
                fetch_items.extend(response.get("items", []))
            except folioclient.FolioClientError as e:
                logger.exception(
                    "Error fetching items batch %s: %s", i // batch_size + 1, e.response.text
                )
            logger.info(
                "Batch %s/%s: fetched %s items",
                i // batch_size + 1,
                (len(barcode_list) + batch_size - 1) // batch_size,
                len(fetch_items),
            )
        logger.info("Fetched %s items matching request barcodes", len(fetch_items))
        self.valid_item_barcodes = {item["barcode"] for item in fetch_items if "barcode" in item}
        missing_item_barcodes = request_barcodes - self.valid_item_barcodes
        for barcode in missing_item_barcodes:
            logger.warning("No item found for barcode: %s", barcode)
            Helper.log_data_issue_failed(
                "",
                "No item found for barcode",
                f"Barcode: {barcode}",
            )

    async def check_barcodes(self) -> AsyncGenerator[LegacyRequest, None]:
        self.pre_validate_item_barcodes()
        await self.pre_validate_patron_barcodes_async()
        request: LegacyRequest
        for request in self.semi_valid_legacy_requests:
            has_item_barcode = request.item_barcode in self.valid_item_barcodes
            has_patron_barcode = request.patron_barcode in self.valid_patron_map
            if has_item_barcode and has_patron_barcode:
                self.migration_report.add_general_statistics(
                    i18n.t("Requests successfully verified against migrated users and items")
                )
                request.patron_barcode = self.valid_patron_map.get(
                    request.patron_barcode, request.patron_barcode
                )
                yield request
            else:
                self.failed_requests.add(request)
                self.migration_report.add(
                    "DiscardedRequests",
                    i18n.t(
                        "Requests discarded. Had migrated item barcode: %{item_barcode}.\n "
                        "Had migrated user barcode: %{patron_barcode}",
                        item_barcode=has_item_barcode,
                        patron_barcode=has_patron_barcode,
                    ),
                )
                self.migration_report.add_general_statistics(
                    i18n.t("Requests that failed verification against migrated users and items")
                )
            if not has_item_barcode:
                Helper.log_data_issue(
                    "",
                    "Request without matched item barcode",
                    json.dumps(request.to_source_dict()),
                )
            if not has_patron_barcode:
                Helper.log_data_issue(
                    "",
                    "Request without matched patron barcode",
                    json.dumps(request.to_source_dict()),
                )

    def load_and_validate_legacy_requests(self, requests_reader):
        num_bad = 0
        logger.info("Validating legacy requests in file...")
        for legacy_reques_count, legacy_request_dict in enumerate(requests_reader, start=1):
            self.migration_report.add_general_statistics(i18n.t("Requests in file"))
            try:
                legacy_request = LegacyRequest(
                    legacy_request_dict,
                    self.tenant_timezone,
                    legacy_reques_count,
                )
                if any(legacy_request.errors):
                    num_bad += 1
                    self.migration_report.add_general_statistics(
                        i18n.t("Requests with valueErrors")
                    )
                    for error in legacy_request.errors:
                        self.migration_report.add("DiscardedRequests", f"{error[0]} - {error[1]}")
                        Helper.log_data_issue(
                            legacy_request.item_barcode,
                            f"{error[0]} - {error[1]}",
                            json.dumps(legacy_request.to_source_dict()),
                        )
                else:
                    self.migration_report.add_general_statistics(
                        i18n.t("Requests with valid source data")
                    )
                    yield legacy_request
            except ValueError as ve:
                logger.exception(ve)
        logger.info(
            f"Done validating {legacy_reques_count} legacy requests with {num_bad} rotten apples"
        )
        if num_bad > 0 and (num_bad / legacy_reques_count) > 0.5:
            q = num_bad / legacy_reques_count
            logger.error("%s percent of requests failed to validate.", (q * 100))
            self.migration_report.log_me()
            logger.critical("Halting...")
            sys.exit(1)


def timings(t0, t0func, num_objects):
    avg = num_objects / (time.time() - t0)
    elapsed = time.time() - t0
    elapsed_func = time.time() - t0func
    return (
        f"Total objects: {num_objects}\tTotal elapsed: {elapsed:.2f}\t"
        f"Average per object: {avg:.2f}\tElapsed this time: {elapsed_func:.2f}"
    )
