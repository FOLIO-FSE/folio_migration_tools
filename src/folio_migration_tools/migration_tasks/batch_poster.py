import asyncio
import copy
import json
import logging
import re
import sys
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Annotated, List, Optional
from uuid import uuid4

import folioclient
import i18n

if TYPE_CHECKING:
    from httpx import Response
from folio_uuid.folio_namespaces import FOLIONamespaces
from pydantic import Field

from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.library_configuration import (
    FileDefinition,
    LibraryConfiguration,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


def write_failed_batch_to_file(batch, file):
    for record in batch:
        file.write(f"{json.dumps(record)}\n")


class BatchPoster(MigrationTaskBase):
    """BatchPoster

    Parents:
        MigrationTaskBase (_type_): _description_

    Raises:
        ee: _description_
        TransformationRecordFailedError: _description_
        TransformationProcessError: _description_
        TransformationRecordFailedError: _description_
        TransformationRecordFailedError: _description_
        TransformationProcessError: _description_

    Returns:
        _type_: _description_
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
            str,
            Field(
                title="Object type",
                description=(
                    "The type of object being migrated"
                    "Examples of possible values: "
                    "'Extradata', 'Instances', 'Holdings', 'Items'"
                ),
            ),
        ]
        files: Annotated[
            List[FileDefinition],
            Field(
                title="List of files",
                description="List of files to be processed",
            ),
        ]
        batch_size: Annotated[
            int,
            Field(
                title="Batch size",
                description="The batch size for processing files",
            ),
        ]
        rerun_failed_records: Annotated[
            bool,
            Field(
                title="Rerun failed records",
                description=(
                    "Toggles whether or not BatchPoster should try to rerun "
                    "failed batches or just leave the failing records on disk."
                ),
            ),
        ] = True
        use_safe_inventory_endpoints: Annotated[
            bool,
            Field(
                title="Use safe inventory endpoints",
                description=(
                    "Toggles the use of the safe/unsafe Inventory storage "
                    "endpoints. Unsafe circumvents the Optimistic locking "
                    "in FOLIO. Defaults to True (using the 'safe' options)"
                ),
            ),
        ] = True
        extradata_endpoints: Annotated[
            dict,
            Field(
                title="Extradata endpoints",
                description=(
                    "A dictionary of extradata endpoints. "
                    "The key is the object type and the value is the endpoint"
                ),
            ),
        ] = {}
        upsert: Annotated[
            bool,
            Field(
                title="Upsert",
                description=(
                    "Toggles whether or not to use the upsert feature "
                    "of the Inventory storage endpoints. Defaults to False"
                ),
            ),
        ] = False
        preserve_statistical_codes: Annotated[
            bool,
            Field(
                title="Preserve statistical codes",
                description=(
                    "Toggles whether or not to preserve statistical codes "
                    "during the upsert process. Defaults to False"
                ),
            ),
        ] = False
        preserve_administrative_notes: Annotated[
            bool,
            Field(
                title="Preserve administrative notes",
                description=(
                    "Toggles whether or not to preserve administrative notes "
                    "during the upsert process. Defaults to False"
                ),
            ),
        ] = False
        preserve_temporary_locations: Annotated[
            bool,
            Field(
                title="Preserve temporary locations",
                description=(
                    "Toggles whether or not to preserve temporary locations "
                    "on items during the upsert process. Defaults to False"
                ),
            ),
        ] = False
        preserve_temporary_loan_types: Annotated[
            bool,
            Field(
                title="Preserve temporary loan types",
                description=(
                    "Toggles whether or not to preserve temporary loan types "
                    "on items during the upsert process. Defaults to False"
                ),
            ),
        ] = False
        preserve_item_status: Annotated[
            bool,
            Field(
                title="Preserve item status",
                description=(
                    "Toggles whether or not to preserve item status "
                    "on items during the upsert process. Defaults to False"
                ),
            ),
        ] = True
        patch_existing_records: Annotated[
            bool,
            Field(
                title="Patch existing records",
                description=(
                    "Toggles whether or not to patch existing records "
                    "during the upsert process. Defaults to False"
                ),
            ),
        ] = False
        patch_paths: Annotated[
            List[str],
            Field(
                title="Patch paths",
                description=(
                    "A list of fields in JSON Path notation to patch during the upsert process "
                    "(leave off the $). If empty, all fields will be patched. Examples: "
                    "['statisticalCodeIds', 'administrativeNotes', 'instanceStatusId']"
                ),
            ),
        ] = []

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
        self.performing_rerun = False
        self.failed_ids: list = []
        self.first_batch = True
        self.api_info = get_api_info(
            self.task_configuration.object_type,
            self.task_configuration.use_safe_inventory_endpoints,
        )
        self.query_params = {}
        if self.api_info["supports_upsert"]:
            self.query_params["upsert"] = self.task_configuration.upsert
        elif self.task_configuration.upsert and not self.api_info["supports_upsert"]:
            logging.info(
                "Upsert is not supported for this object type. Query parameter will not be set."
            )
        self.snapshot_id = str(uuid4())
        self.failed_objects: list = []
        self.batch_size = self.task_configuration.batch_size
        logging.info("Batch size is %s", self.batch_size)
        self.processed = 0
        self.failed_batches = 0
        self.users_created = 0
        self.users_updated = 0
        self.users_per_group: dict = {}
        self.failed_fields: set = set()
        self.num_failures = 0
        self.num_posted = 0
        self.starting_record_count_in_folio: Optional[int] = None
        self.finished_record_count_in_folio: Optional[int] = None

    def do_work(self):  # noqa: C901
        with open(
            self.folder_structure.failed_recs_path, "w", encoding="utf-8"
        ) as failed_recs_file:
            self.get_starting_record_count()
            try:
                batch = []
                for idx, file_def in enumerate(self.task_configuration.files):  # noqa: B007
                    path = self.folder_structure.results_folder / file_def.file_name
                    with open(path) as rows:
                        logging.info("Running %s", path)
                        last_row = ""
                        for self.processed, row in enumerate(rows, start=1):
                            last_row = row
                            if row.strip():
                                try:
                                    if self.task_configuration.object_type == "Extradata":
                                        self.post_extra_data(row, self.processed, failed_recs_file)
                                    elif not self.api_info["is_batch"]:
                                        self.post_single_records(
                                            row, self.processed, failed_recs_file
                                        )
                                    else:
                                        batch = self.post_record_batch(
                                            batch, failed_recs_file, row
                                        )
                                except UnicodeDecodeError as unicode_error:
                                    self.handle_unicode_error(unicode_error, last_row)
                                except TransformationProcessError as tpe:
                                    self.handle_generic_exception(
                                        tpe,
                                        last_row,
                                        batch,
                                        self.processed,
                                        failed_recs_file,
                                    )
                                    batch = []
                                    raise
                                except TransformationRecordFailedError as exception:
                                    self.handle_generic_exception(
                                        exception,
                                        last_row,
                                        batch,
                                        self.processed,
                                        failed_recs_file,
                                    )
                                    batch = []
                                except (FileNotFoundError, PermissionError) as ose:
                                    logging.error("Error reading file: %s", ose)

            except Exception as ee:
                if "idx" in locals() and self.task_configuration.files[idx:]:
                    for file_def in self.task_configuration.files[idx:]:
                        path = self.folder_structure.results_folder / file_def.file_name
                        try:
                            with open(path, "r") as failed_file:
                                failed_file.seek(self.processed)
                                failed_recs_file.write(failed_file.read())
                                self.processed = 0
                        except (FileNotFoundError, PermissionError) as ose:
                            logging.error("Error reading file: %s", ose)
                raise ee
            finally:
                if self.task_configuration.object_type != "Extradata" and any(batch):
                    try:
                        self.post_batch(batch, failed_recs_file, self.processed)
                    except Exception as exception:
                        self.handle_generic_exception(
                            exception, last_row, batch, self.processed, failed_recs_file
                        )
                logging.info("Done posting %s records. ", self.processed)

    @staticmethod
    def set_consortium_source(json_rec):
        if json_rec["source"] == "MARC":
            json_rec["source"] = "CONSORTIUM-MARC"
        elif json_rec["source"] == "FOLIO":
            json_rec["source"] = "CONSORTIUM-FOLIO"

    def set_version(self, batch, query_api, object_type) -> None:
        """
        Synchronous wrapper for set_version_async
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.set_version_async(batch, query_api, object_type))
            asyncio.set_event_loop(None)  # Reset the event loop
        else:
            loop.run_until_complete(self.set_version_async(batch, query_api, object_type))

    async def set_version_async(self, batch, query_api, object_type) -> None:
        """
        Fetches the current version of the records in the batch if the record exists in FOLIO

        Args:
            batch (list): List of records to fetch versions for
            query_api (str): The query API endpoint to use
            object_type (str): The key in the API response that contains the records

        Returns:
            None
        """
        fetch_batch_size = 90
        fetch_tasks = []
        existing_records = {}

        for i in range(0, len(batch), fetch_batch_size):
            batch_slice = batch[i : i + fetch_batch_size]
            fetch_tasks.append(
                self.get_with_retry(
                    query_api,
                    params={
                        "query": (
                            f"id==({' OR '.join([r['id'] for r in batch_slice if 'id' in r])})"
                        ),
                        "limit": fetch_batch_size,
                    },
                )
            )

        responses = await asyncio.gather(*fetch_tasks)

        for response in responses:
            self.collect_existing_records_for_upsert(object_type, response, existing_records)

        for record in batch:
            if record["id"] in existing_records:
                self.prepare_record_for_upsert(record, existing_records[record["id"]])

    def patch_record(self, new_record: dict, existing_record: dict, patch_paths: List[str]):
        """
        Updates new_record with values from existing_record according to patch_paths.

        Args:
            new_record (dict): The new record to be updated.
            existing_record (dict): The existing record to patch from.
            patch_paths (List[str]): List of fields in JSON Path notation (e.g., ['statisticalCodeIds', 'administrativeNotes', 'instanceStatusId']) to patch during the upsert process. If empty, all fields will be patched.
        """  # noqa: E501
        updates = {}
        updates.update(existing_record)
        keep_existing = {}
        self.handle_upsert_for_administrative_notes(updates, keep_existing)
        self.handle_upsert_for_statistical_codes(updates, keep_existing)
        if not patch_paths:
            keep_new = new_record
        else:
            keep_new = extract_paths(new_record, patch_paths)
        if "instanceStatusId" in new_record:
            updates["instanceStatusId"] = new_record["instanceStatusId"]
        deep_update(updates, keep_new)
        for key, value in keep_existing.items():
            if isinstance(value, list) and key in keep_new:
                updates[key] = list(dict.fromkeys(updates.get(key, []) + value))
            elif key not in keep_new:
                updates[key] = value
        new_record.clear()
        new_record.update(updates)

    @staticmethod
    def collect_existing_records_for_upsert(
        object_type: str, response_json: dict, existing_records: dict
    ):
        """
        Collects existing records from API response into existing_records dict.

        Args:
            object_type: The key in response containing the records array
            response_json: Parsed JSON response from API
            existing_records: Dict to populate with {record_id: record_data}
        """
        for record in response_json.get(object_type, []):
            existing_records[record["id"]] = record

    def handle_upsert_for_statistical_codes(self, updates: dict, keep_existing: dict):
        if not self.task_configuration.preserve_statistical_codes:
            updates["statisticalCodeIds"] = []
            keep_existing["statisticalCodeIds"] = []
        else:
            keep_existing["statisticalCodeIds"] = updates.pop("statisticalCodeIds", [])
            updates["statisticalCodeIds"] = []

    def handle_upsert_for_administrative_notes(self, updates: dict, keep_existing: dict):
        if not self.task_configuration.preserve_administrative_notes:
            updates["administrativeNotes"] = []
            keep_existing["administrativeNotes"] = []
        else:
            keep_existing["administrativeNotes"] = updates.pop("administrativeNotes", [])
            updates["administrativeNotes"] = []

    def handle_upsert_for_temporary_locations(self, updates: dict, keep_existing: dict):
        if self.task_configuration.preserve_temporary_locations:
            keep_existing["temporaryLocationId"] = updates.pop("temporaryLocationId", None)

    def handle_upsert_for_temporary_loan_types(self, updates: dict, keep_existing: dict):
        if self.task_configuration.preserve_temporary_loan_types:
            keep_existing["temporaryLoanTypeId"] = updates.pop("temporaryLoanTypeId", None)

    def keep_existing_fields(self, updates: dict, existing_record: dict):
        keep_existing_fields = ["hrid", "lastCheckIn"]
        if self.task_configuration.preserve_item_status:
            keep_existing_fields.append("status")
        for key in keep_existing_fields:
            if key in existing_record:
                updates[key] = existing_record[key]

    def prepare_record_for_upsert(self, new_record: dict, existing_record: dict):
        if "source" in existing_record and "MARC" in existing_record["source"]:
            patch_paths = [
                x
                for x in self.task_configuration.patch_paths
                if ("suppress" in x.lower() or x.lower() == "deleted")
            ]
            if patch_paths:
                logging.debug(
                    "Record %s is a MARC record, only suppression related fields will be patched",
                    existing_record["id"],
                )
            else:
                logging.debug(
                    "Record %s is a MARC record, patch_paths will be ignored",
                    existing_record["id"],
                )
            patch_paths.extend(["statisticalCodeIds", "administrativeNotes", "instanceStatusId"])
            self.patch_record(new_record, existing_record, patch_paths)
        elif self.task_configuration.patch_existing_records:
            self.patch_record(new_record, existing_record, self.task_configuration.patch_paths)
        else:
            updates = {
                "_version": existing_record["_version"],
            }
            self.keep_existing_fields(updates, existing_record)
            keep_new = {
                k: v
                for k, v in new_record.items()
                if k in ["statisticalCodeIds", "administrativeNotes"]
            }
            keep_existing = {}
            self.handle_upsert_for_statistical_codes(existing_record, keep_existing)
            self.handle_upsert_for_administrative_notes(existing_record, keep_existing)
            self.handle_upsert_for_temporary_locations(existing_record, keep_existing)
            self.handle_upsert_for_temporary_loan_types(existing_record, keep_existing)
            for k, v in keep_existing.items():
                if isinstance(v, list) and k in keep_new:
                    keep_new[k] = list(dict.fromkeys(v + keep_new.get(k, [])))
                elif k not in keep_new:
                    keep_new[k] = v
            updates.update(keep_new)
            new_record.update(updates)

    async def get_with_retry(self, url: str, params=None):
        """
        Wrapper around folio_get_async with selective retry logic.

        Retries on:
        - Connection errors (FolioConnectionError): Always retry
        - Server errors (5xx): Transient failures
        - Rate limiting (429): Too many requests

        Does NOT retry on:
        - Client errors (4xx except 429): Bad request, won't succeed on retry
        """
        if params is None:
            params = {}
        retries = 3

        for attempt in range(retries):
            try:
                return await self.folio_client.folio_get_async(url, query_params=params)

            except folioclient.FolioConnectionError as e:
                # Network/connection errors - always retry
                if attempt < retries - 1:
                    wait_time = 2**attempt
                    logging.warning(
                        f"Connection error, retrying in {wait_time}s "
                        f"(attempt {attempt + 1}/{retries}): {e}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logging.error(f"Connection failed after {retries} attempts: {e}")
                    raise

            except folioclient.FolioHTTPError as e:
                # HTTP errors - selective retry based on status code
                status_code = e.response.status_code
                should_retry = status_code >= 500 or status_code == 429

                if should_retry and attempt < retries - 1:
                    # Longer wait for rate limiting
                    wait_time = 5 if status_code == 429 else 2**attempt
                    logging.warning(
                        f"HTTP {status_code} error, retrying in {wait_time}s "
                        f"(attempt {attempt + 1}/{retries}): {e}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    # Either not retryable or out of attempts
                    if should_retry:
                        logging.error(
                            f"HTTP {status_code} error persisted after {retries} attempts: {e}"
                        )
                    else:
                        logging.error(f"HTTP {status_code} error (not retryable): {e}")
                    raise

    def post_record_batch(self, batch, failed_recs_file, row):
        json_rec = json.loads(row.split("\t")[-1])
        if self.task_configuration.object_type == "ShadowInstances":
            self.set_consortium_source(json_rec)
        if self.processed == 1:
            logging.info(json.dumps(json_rec, indent=True))
        batch.append(json_rec)
        if len(batch) == int(self.batch_size):
            self.post_batch(batch, failed_recs_file, self.processed)
            batch = []
        return batch

    def post_extra_data(self, row: str, num_records: int, failed_recs_file):
        (object_name, data) = row.split("\t")
        url = self.get_extradata_endpoint(self.task_configuration, object_name, data)
        body = data
        try:
            _ = self.folio_client.folio_post(url, payload=body)
            self.num_posted += 1
        except folioclient.FolioHTTPError as fhe:
            if fhe.response.status_code == 422:
                self.num_failures += 1
                error_msg = json.loads(fhe.response.text)["errors"][0]["message"]
                logging.error(
                    "Row %s\tHTTP %s\t %s", num_records, fhe.response.status_code, error_msg
                )
                if (
                    "id value already exists"
                    not in json.loads(fhe.response.text)["errors"][0]["message"]
                ):
                    failed_recs_file.write(row)
            else:
                self.num_failures += 1
                logging.error(
                    "Row %s\tHTTP %s\t%s", num_records, fhe.response.status_code, fhe.response.text
                )
                failed_recs_file.write(row)
        if num_records % 50 == 0:
            logging.info(
                "%s records posted successfully. %s failed",
                self.num_posted,
                self.num_failures,
            )

    @staticmethod
    def get_extradata_endpoint(
        task_configuration: TaskConfiguration, object_name: str, string_object: str
    ):
        object_types = {
            "precedingSucceedingTitles": "preceding-succeeding-titles",
            "precedingTitles": "preceding-succeeding-titles",
            "succeedingTitles": "preceding-succeeding-titles",
            "boundwithPart": "inventory-storage/bound-with-parts",
            "notes": "notes",
            "course": "coursereserves/courses",
            "courselisting": "coursereserves/courselistings",
            "contacts": "organizations-storage/contacts",
            "interfaces": "organizations-storage/interfaces",
            "account": "accounts",
            "feefineaction": "feefineactions",
            "bankInfo": "organizations/banking-information",
        }
        object_types.update(task_configuration.extradata_endpoints)
        if object_name == "instructor":
            instructor = json.loads(string_object)
            return f"coursereserves/courselistings/{instructor['courseListingId']}/instructors"

        if object_name == "interfaceCredential":
            credential = json.loads(string_object)
            return f"organizations-storage/interfaces/{credential['interfaceId']}/credentials"

        return object_types[object_name]

    def post_single_records(self, row: str, num_records: int, failed_recs_file):
        if self.api_info["is_batch"]:
            raise TypeError("This record type supports batch processing, use post_batch method")
        url = self.api_info.get("api_endpoint")
        try:
            _ = self.folio_client.folio_post(url, payload=row)
            self.num_posted += 1
        except folioclient.FolioHTTPError as fhe:
            if fhe.response.status_code == 422:
                self.num_failures += 1
                error_msg = json.loads(fhe.response.text)["errors"][0]["message"]
                logging.error(
                    "Row %s\tHTTP %s\t %s", num_records, fhe.response.status_code, error_msg
                )
                if (
                    "id value already exists"
                    not in json.loads(fhe.response.text)["errors"][0]["message"]
                ):
                    failed_recs_file.write(row)
            else:
                self.num_failures += 1
                logging.error(
                    "Row %s\tHTTP %s\t%s",
                    num_records,
                    fhe.response.status_code,
                    fhe.response.text,
                )
                failed_recs_file.write(row)
            if num_records % 50 == 0:
                logging.info(
                    "%s records posted successfully. %s failed",
                    self.num_posted,
                    self.num_failures,
                )

    def handle_generic_exception(self, exception, last_row, batch, num_records, failed_recs_file):
        logging.error("%s", exception)
        self.migration_report.add("Details", i18n.t("Generic exceptions (see log for details)"))
        # logging.error("Failed row: %s", last_row)
        self.failed_batches += 1
        self.num_failures += len(batch)
        write_failed_batch_to_file(batch, failed_recs_file)
        logging.info("Resetting batch...Number of failed batches: %s", self.failed_batches)
        batch = []
        if self.failed_batches > 50000:
            logging.error("Exceeded number of failed batches at row %s", num_records)
            logging.critical("Halting")
            sys.exit(1)

    def handle_unicode_error(self, unicode_error, last_row):
        self.migration_report.add("Details", i18n.t("Encoding errors"))
        logging.info("=========ERROR==============")
        logging.info(
            "%s Posting failed. Encoding error reading file",
            unicode_error,
        )
        logging.info(
            "Failing row, either the one shown here or the next row in %s",
            self.task_configuration.file.file_name,
        )
        logging.info(last_row)
        logging.info("=========Stack trace==============")
        traceback.logging.info_exc()  # type: ignore
        logging.info("=======================")

    def post_batch(self, batch, failed_recs_file, num_records):
        if self.query_params.get("upsert", False) and self.api_info.get("query_endpoint", ""):
            self.set_version(batch, self.api_info["query_endpoint"], self.api_info["object_name"])
        response = self.do_post(batch)
        if response.status_code == 401:
            logging.error("Authorization failed (%s). Fetching new auth token...", response.text)
            self.folio_client.login()
            response = self.do_post(batch)
        if response.status_code == 201:
            logging.info(
                (
                    "Posting successful! Total rows: %s Total failed: %s "
                    "in %ss "
                    "Batch Size: %s Request size: %s "
                ),
                num_records,
                self.num_failures,
                response.elapsed.total_seconds(),
                len(batch),
                get_req_size(response),
            )
        elif response.status_code == 200:
            json_report = json.loads(response.text)
            self.users_created += json_report.get("createdRecords", 0)
            self.users_updated += json_report.get("updatedRecords", 0)
            self.num_posted = self.users_updated + self.users_created
            self.num_failures += json_report.get("failedRecords", 0)
            if json_report.get("failedRecords", 0) > 0:
                logging.error(
                    "%s users in batch failed to load",
                    json_report.get("failedRecords", 0),
                )
                write_failed_batch_to_file(batch, failed_recs_file)
            if json_report.get("failedUsers", []):
                logging.error("Error message: %s", json_report.get("error", []))
                for failed_user in json_report.get("failedUsers"):
                    logging.error(
                        "User failed. %s\t%s\t%s",
                        failed_user.get("username", ""),
                        failed_user.get("externalSystemId", ""),
                        failed_user.get("errorMessage", ""),
                    )
                    self.migration_report.add("Details", failed_user.get("errorMessage", ""))
            logging.info(
                (
                    "Posting successful! Total rows: %s Total failed: %s "
                    "created: %s updated: %s in %ss Batch Size: %s Request size: %s "
                    "Message from server: %s"
                ),
                num_records,
                self.num_failures,
                self.users_created,
                self.users_updated,
                response.elapsed.total_seconds(),
                len(batch),
                get_req_size(response),
                json_report.get("message", ""),
            )
        elif response.status_code == 422:
            resp = json.loads(response.text)
            raise TransformationRecordFailedError(
                "",
                f"HTTP {response.status_code}\t"
                f"Request size: {get_req_size(response)}"
                f"{datetime.now(timezone.utc).isoformat()}\n",
                json.dumps(resp, indent=4),
            )
        elif response.status_code == 400:
            # Likely a json parsing error
            logging.error(response.text)
            raise TransformationProcessError("", "HTTP 400. Something is wrong. Quitting")
        elif (
            response.status_code == 413 and "DB_ALLOW_SUPPRESS_OPTIMISTIC_LOCKING" in response.text
        ):
            logging.error(response.text)
            raise TransformationProcessError("", response.text, "")

        else:
            try:
                logging.info(response.text)
                resp = json.dumps(response, indent=4)
            except TypeError:
                resp = response
            except Exception as e:
                logging.exception(f"something unexpected happened, {e}")
                resp = response
            raise TransformationRecordFailedError(
                "",
                f"HTTP {response.status_code}\t"
                f"Request size: {get_req_size(response)}"
                f"{datetime.now(timezone.utc).isoformat()}\n",
                resp,
            )

    def do_post(self, batch):
        with self.folio_client.get_folio_http_client() as http_client:
            url = self.api_info["api_endpoint"]
            if self.api_info["object_name"] == "users":
                payload = {self.api_info["object_name"]: list(batch), "totalRecords": len(batch)}
            elif self.api_info["total_records"]:
                payload = {"records": list(batch), "totalRecords": len(batch)}
            else:
                payload = {self.api_info["object_name"]: batch}
            return http_client.post(
                url,
                json=payload,
                params=self.query_params,
            )

    def get_current_record_count_in_folio(self):
        if "query_endpoint" in self.api_info:
            url = self.api_info["query_endpoint"]
            query_params = {"query": "cql.allRecords=1", "limit": 0}
            try:
                res = self.folio_client.folio_get(url, query_params=query_params)
                return res["totalRecords"]
            except folioclient.FolioHTTPError as fhe:
                logging.error(
                    "Failed to get current record count. HTTP %s", fhe.response.status_code
                )
                return 0
            except KeyError:
                logging.error(
                    "Failed to get current record count. "
                    f"No 'totalRecords' in response: {json.dumps(res, indent=2)}"
                )
                return 0
        else:
            raise ValueError(
                "No 'query_endpoint' available for %s. Cannot get current record count.",
                self.task_configuration.object_type,
            )

    def get_starting_record_count(self):
        if "query_endpoint" in self.api_info and not self.starting_record_count_in_folio:
            logging.info("Getting starting record count in FOLIO")
            self.starting_record_count_in_folio = self.get_current_record_count_in_folio()
        else:
            logging.info(
                "No query_endpoint available for %s. Cannot get starting record count.",
                self.task_configuration.object_type,
            )

    def get_finished_record_count(self):
        if "query_endpoint" in self.api_info:
            logging.info("Getting finished record count in FOLIO")
            self.finished_record_count_in_folio = self.get_current_record_count_in_folio()
        else:
            logging.info(
                "No query_endpoint available for %s. Cannot get ending record count.",
                self.task_configuration.object_type,
            )

    def wrap_up(self):
        logging.info("Done. Wrapping up")
        self.extradata_writer.flush()
        if self.task_configuration.object_type != "Extradata":
            logging.info(
                (
                    "Failed records: %s failed records in %s "
                    "failed batches. Failed records saved to %s"
                ),
                self.num_failures,
                self.failed_batches,
                self.folder_structure.failed_recs_path,
            )
        else:
            logging.info("Done posting %s records. %s failed", self.num_posted, self.num_failures)
        if self.starting_record_count_in_folio:
            self.get_finished_record_count()
            total_on_server = (
                self.finished_record_count_in_folio - self.starting_record_count_in_folio
            )
            discrepancy = self.processed - self.num_failures - total_on_server
            if discrepancy != 0:
                logging.error(
                    (
                        "Discrepancy in record count. "
                        "Starting record count: %s. Finished record count: %s. "
                        "Records posted: %s. Discrepancy: %s"
                    ),
                    self.starting_record_count_in_folio,
                    self.finished_record_count_in_folio,
                    self.num_posted - self.num_failures,
                    discrepancy,
                )
        else:
            discrepancy = 0
        run = "second time" if self.performing_rerun else "first time"
        self.migration_report.set("GeneralStatistics", f"Records processed {run}", self.processed)
        self.migration_report.set("GeneralStatistics", f"Records posted {run}", self.num_posted)
        self.migration_report.set("GeneralStatistics", f"Failed to post {run}", self.num_failures)
        if discrepancy:
            self.migration_report.set(
                "GeneralStatistics",
                f"Discrepancy in record count {run}",
                discrepancy,
            )
        self.rerun_run()
        with open(self.folder_structure.migration_reports_file, "w+") as report_file:
            self.migration_report.write_migration_report(
                f"{self.task_configuration.object_type} loading report",
                report_file,
                self.start_datetime,
            )
        self.clean_out_empty_logs()

    def rerun_run(self):
        if self.task_configuration.rerun_failed_records and (self.num_failures > 0):
            logging.info(
                "Rerunning the %s failed records from the load with a batchsize of 1",
                self.num_failures,
            )
            try:
                self.task_configuration.batch_size = 1
                self.task_configuration.files = [
                    FileDefinition(file_name=str(self.folder_structure.failed_recs_path.name))
                ]
                temp_report = copy.deepcopy(self.migration_report)
                temp_start = self.start_datetime
                self.task_configuration.rerun_failed_records = False
                self.__init__(
                    self.task_configuration, self.library_configuration, self.folio_client
                )
                self.performing_rerun = True
                self.migration_report = temp_report
                self.start_datetime = temp_start
                self.do_work()
                self.wrap_up()
                logging.info("Done rerunning the posting")
            except Exception as ee:
                logging.exception("Occurred during rerun")
                raise TransformationProcessError("Error during rerun") from ee
        elif not self.task_configuration.rerun_failed_records and (self.num_failures > 0):
            logging.info(
                (
                    "Task configured to not rerun failed records. "
                    " File with failed records is located at %s"
                ),
                str(self.folder_structure.failed_recs_path),
            )


def get_api_info(object_type: str, use_safe: bool = True):
    choices = {
        "Extradata": {
            "object_name": "",
            "api_endpoint": "",
            "total_records": False,
            "addSnapshotId": False,
            "supports_upsert": False,
        },
        "Items": {
            "object_name": "items",
            "api_endpoint": (
                "/item-storage/batch/synchronous"
                if use_safe
                else "/item-storage/batch/synchronous-unsafe"
            ),
            "query_endpoint": "/item-storage/items",
            "is_batch": True,
            "total_records": False,
            "addSnapshotId": False,
            "supports_upsert": True,
        },
        "Holdings": {
            "object_name": "holdingsRecords",
            "api_endpoint": (
                "/holdings-storage/batch/synchronous"
                if use_safe
                else "/holdings-storage/batch/synchronous-unsafe"
            ),
            "query_endpoint": "/holdings-storage/holdings",
            "is_batch": True,
            "total_records": False,
            "addSnapshotId": False,
            "supports_upsert": True,
        },
        "Instances": {
            "object_name": "instances",
            "api_endpoint": (
                "/instance-storage/batch/synchronous"
                if use_safe
                else "/instance-storage/batch/synchronous-unsafe"
            ),
            "query_endpoint": "/instance-storage/instances",
            "is_batch": True,
            "total_records": False,
            "addSnapshotId": False,
            "supports_upsert": True,
        },
        "ShadowInstances": {
            "object_name": "instances",
            "api_endpoint": (
                "/instance-storage/batch/synchronous"
                if use_safe
                else "/instance-storage/batch/synchronous-unsafe"
            ),
            "is_batch": True,
            "total_records": False,
            "addSnapshotId": False,
            "supports_upsert": True,
        },
        "Users": {
            "object_name": "users",
            "api_endpoint": "/user-import",
            "is_batch": True,
            "total_records": True,
            "addSnapshotId": False,
            "supports_upsert": False,
        },
        "Organizations": {
            "object_name": "",
            "api_endpoint": "/organizations/organizations",
            "is_batch": False,
            "total_records": False,
            "addSnapshotId": False,
            "supports_upsert": False,
        },
        "Orders": {
            "object_name": "",
            "api_endpoint": "/orders/composite-orders",
            "is_batch": False,
            "total_records": False,
            "addSnapshotId": False,
            "supports_upsert": False,
        },
    }

    try:
        return choices[object_type]
    except KeyError:
        key_string = ", ".join(choices.keys())
        logging.error(
            f"Wrong type. Only one of {key_string} are allowed, received {object_type=} instead"
        )
        logging.error("Halting")
        sys.exit(1)


def get_human_readable(size, precision=2):
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    suffix_index = 0
    while size > 1024 and suffix_index < 4:
        suffix_index += 1  # increment the index of the suffix
        size = size / 1024.0  # apply the division
    return "%.*f%s" % (precision, size, suffixes[suffix_index])


def get_req_size(response: "Response"):
    size = response.request.method
    size += str(response.request.url)
    size += "\r\n".join(f"{k}{v}" for k, v in response.request.headers.items())
    size += response.request.content.decode("utf-8") or ""
    return get_human_readable(len(size.encode("utf-8")))


def parse_path(path):
    """
    Parses a path like 'foo.bar[0].baz' into ['foo', 'bar', 0, 'baz']
    """
    tokens = []
    # Split by dot, then extract indices
    for part in path.split("."):
        # Find all [index] parts
        matches = re.findall(r"([^\[\]]+)|\[(\d+)\]", part)
        for name, idx in matches:
            if name:
                tokens.append(name)
            if idx:
                tokens.append(int(idx))
    return tokens


def get_by_path(data, path):
    keys = parse_path(path)
    for key in keys:
        data = data[key]
    return data


def set_by_path(data, path, value):
    keys = parse_path(path)
    for i, key in enumerate(keys[:-1]):
        next_key = keys[i + 1]
        if isinstance(key, int):
            while len(data) <= key:
                data.append({} if not isinstance(next_key, int) else [])
            data = data[key]
        else:
            if key not in data or not isinstance(data[key], (dict, list)):
                data[key] = {} if not isinstance(next_key, int) else []
            data = data[key]
    last_key = keys[-1]
    if isinstance(last_key, int):
        while len(data) <= last_key:
            data.append(None)
        data[last_key] = value
    else:
        data[last_key] = value


def extract_paths(data, paths):
    result = {}
    for path in paths:
        try:
            value = get_by_path(data, path)
            set_by_path(result, path, value)
        except KeyError:
            continue
    return result


def deep_update(target, patch):
    """
    Recursively update target dict/list with values from patch dict/list.
    For lists, only non-None values in patch are merged into target.
    """
    if isinstance(patch, dict):
        for k, v in patch.items():
            if k in target and isinstance(target[k], (dict, list)) and isinstance(v, (dict, list)):
                deep_update(target[k], v)
            else:
                target[k] = v
    elif isinstance(patch, list):
        for i, v in enumerate(patch):
            if v is None:
                continue  # Skip None values, leave target unchanged
            if i < len(target):
                if isinstance(target[i], (dict, list)) and isinstance(v, (dict, list)):
                    deep_update(target[i], v)
                else:
                    target[i] = v
            else:
                # Only append if not None
                target.append(v)
    else:
        return patch
