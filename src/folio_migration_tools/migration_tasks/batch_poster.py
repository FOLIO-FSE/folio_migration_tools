import copy
import json
import logging
import sys
import time
import traceback
from datetime import datetime
from typing import List
from typing import Optional
from uuid import uuid4

import requests
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from folio_migration_tools.report_blurbs import Blurbs
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


def write_failed_batch_to_file(batch, file):
    for record in batch:
        file.write(f"{json.dumps(record)}\n")


class BatchPoster(MigrationTaskBase):
    """Batchposter

    Args:
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
        name: str
        migration_task_type: str
        object_type: str
        files: List[FileDefinition]
        batch_size: int
        rerun_failed_records: Optional[bool] = True

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.other

    def __init__(
        self,
        task_config: TaskConfiguration,
        library_config: LibraryConfiguration,
        use_logging: bool = True,
    ):
        super().__init__(library_config, task_config, use_logging)
        self.migration_report = MigrationReport()
        self.performing_rerun = False
        self.failed_ids = []
        self.first_batch = True
        self.api_path = list_objects(self.task_configuration.object_type)
        self.snapshot_id = str(uuid4())
        self.failed_objects = []
        self.batch_size = self.task_configuration.batch_size
        logging.info("Batch size is %s", self.batch_size)
        self.processed = 0
        self.failed_batches = 0
        self.users_created = 0
        self.users_updated = 0
        self.users_per_group = {}
        self.failed_fields = set()
        self.num_failures = 0
        self.num_posted = 0

    def do_work(self):
        try:
            batch = []
            if self.task_configuration.object_type == "SRS":
                self.create_snapshot()
            with open(self.folder_structure.failed_recs_path, "w") as failed_recs_file:
                for file_def in self.task_configuration.files:
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
                                    elif self.task_configuration.object_type == "Authorities":
                                        self.post_authorities(
                                            row, self.processed, failed_recs_file
                                        )
                                    else:
                                        json_rec = json.loads(row.split("\t")[-1])
                                        if self.task_configuration.object_type == "SRS":
                                            json_rec["snapshotId"] = self.snapshot_id
                                        if self.processed == 1:
                                            logging.info(json.dumps(json_rec, indent=True))
                                        batch.append(json_rec)
                                        if len(batch) == int(self.batch_size):
                                            self.post_batch(
                                                batch, failed_recs_file, self.processed
                                            )
                                            batch = []
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
                                    logging.critical("Halting %s", tpe)
                                except TransformationRecordFailedError as exception:
                                    self.handle_generic_exception(
                                        exception,
                                        last_row,
                                        batch,
                                        self.processed,
                                        failed_recs_file,
                                    )
                                    batch = []

                if self.task_configuration.object_type != "Extradata" and any(batch):
                    try:
                        self.post_batch(batch, failed_recs_file, self.processed)
                    except Exception as exception:
                        self.handle_generic_exception(
                            exception, last_row, batch, self.processed, failed_recs_file
                        )
                logging.info("Done posting %s records. ", (self.processed))
        except Exception as ee:
            if self.task_configuration.object_type == "SRS":
                self.commit_snapshot()
            raise ee

    def post_extra_data(self, row: str, num_records: int, failed_recs_file):
        (object_name, data) = row.split("\t")
        endpoint = get_extradata_endpoint(object_name, data)
        url = f"{self.folio_client.okapi_url}/{endpoint}"
        body = data
        response = self.post_objects(url, body)
        if response.status_code == 201:
            self.num_posted += 1
        elif response.status_code == 422:
            self.num_failures += 1
            error_msg = json.loads(response.text)["errors"][0]["message"]
            logging.error("Row %s\tHTTP %s\t %s", num_records, response.status_code, error_msg)
            if "id value already exists" not in json.loads(response.text)["errors"][0]["message"]:
                failed_recs_file.write(row)
        else:
            self.num_failures += 1
            logging.error("Row %s\tHTTP %s\t%s", num_records, response.status_code, response.text)
            failed_recs_file.write(row)
        if num_records % 50 == 0:
            logging.info(
                "%s records posted successfully. %s failed",
                self.num_posted,
                self.num_failures,
            )

    def post_authorities(self, row: str, num_records: int, failed_recs_file):
        url = f"{self.folio_client.okapi_url}/authority-storage/authorities"
        response = self.post_objects(url, row)
        if response.status_code == 201:
            self.num_posted += 1
        elif response.status_code == 422:
            self.num_failures += 1
            error_msg = json.loads(response.text)["errors"][0]["message"]
            logging.error("Row %s\tHTTP %s\t %s", num_records, response.status_code, error_msg)
            if "id value already exists" not in json.loads(response.text)["errors"][0]["message"]:
                failed_recs_file.write(row)
        else:
            self.num_failures += 1
            logging.error("Row %s\tHTTP %s\t%s", num_records, response.status_code, response.text)
            failed_recs_file.write(row)
        if num_records % 50 == 0:
            logging.info(
                "%s records posted successfully. %s failed",
                self.num_posted,
                self.num_failures,
            )

    def post_objects(self, url, body):
        return requests.post(
            url, headers=self.folio_client.okapi_headers, data=body.encode("utf-8")
        )

    def handle_generic_exception(self, exception, last_row, batch, num_records, failed_recs_file):
        logging.error("%s", exception)
        self.migration_report.add(Blurbs.Details, "Generic exceptions (see log for details)")
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
        self.migration_report.add(Blurbs.Details, "Encoding errors")
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
        traceback.logging.info_exc()
        logging.info("=======================", flush=True)

    def post_batch(self, batch, failed_recs_file, num_records, recursion_depth=0):
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
                logging.error("Errormessage: %s", json_report.get("error", []))
                for failed_user in json_report.get("failedUsers"):
                    logging.error(
                        "User failed. %s\t%s\t%s",
                        failed_user.get("username", ""),
                        failed_user.get("externalSystemId", ""),
                        failed_user.get("errorMessage", ""),
                    )
                    self.migration_report.add(Blurbs.Details, failed_user.get("errorMessage", ""))
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
                f"{datetime.utcnow().isoformat()} UTC\n",
                json.dumps(resp, indent=4),
            )
        elif response.status_code == 400:
            # Likely a json parsing error
            logging.error(response.text)
            raise TransformationProcessError("", "HTTP 400. Somehting is wrong. Quitting")
        elif self.task_configuration.object_type == "SRS" and response.status_code >= 500:
            logging.info(
                "Post failed. Size: %s Waiting 30s until reposting. Number of tries: %s of 5",
                get_req_size(response),
                recursion_depth,
            )
            logging.info(response.text)
            time.sleep(30)
            if recursion_depth > 4:
                raise TransformationRecordFailedError(
                    "",
                    f"HTTP {response.status_code}\t"
                    f"Request size: {get_req_size(response)}"
                    f"{datetime.utcnow().isoformat()} UTC\n",
                    response.text,
                )
            else:
                self.post_batch(batch, failed_recs_file, num_records, recursion_depth + 1)
        else:
            try:
                logging.info(response.text)
                resp = json.dumps(response, indent=4)
            except TypeError:
                resp = response
            except Exception:
                logging.exception("something unexpected happened")
                resp = response
            raise TransformationRecordFailedError(
                "",
                f"HTTP {response.status_code}\t"
                f"Request size: {get_req_size(response)}"
                f"{datetime.utcnow().isoformat()} UTC\n",
                resp,
            )

    def do_post(self, batch):
        kind = list_objects(self.task_configuration.object_type)
        path = kind["api_endpoint"]
        url = self.folio_client.okapi_url + path
        if kind["object_name"] == "users":
            payload = {kind["object_name"]: list(batch), "totalRecords": len(batch)}
        elif kind["total_records"]:
            payload = {"records": list(batch), "totalRecords": len(batch)}
        else:
            payload = {kind["object_name"]: batch}
        return requests.post(
            url, data=json.dumps(payload), headers=self.folio_client.okapi_headers
        )

    def wrap_up(self):
        logging.info("Done. Wrapping up")
        self.extradata_writer.flush()
        if self.task_configuration.object_type == "SRS":
            self.commit_snapshot()
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

        run = "second time" if self.performing_rerun else "first time"
        self.migration_report.set(
            Blurbs.GeneralStatistics, f"Records processed {run}", self.processed
        )
        self.migration_report.set(
            Blurbs.GeneralStatistics, f"Records posted {run}", self.num_posted
        )
        self.migration_report.set(
            Blurbs.GeneralStatistics, f"Failed to post {run}", self.num_failures
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
                self.__init__(self.task_configuration, self.library_configuration)
                self.performing_rerun = True
                self.migration_report = temp_report
                self.start_datetime = temp_start
                self.do_work()
                self.wrap_up()
                logging.info("Done rerunning the posting")
            except Exception as ee:
                logging.exception("Occurred during rerun")
                raise TransformationProcessError("Error during rerun") from ee

    def create_snapshot(self):
        snapshot = {
            "jobExecutionId": self.snapshot_id,
            "status": "PARSING_IN_PROGRESS",
            "processingStartedDate": datetime.utcnow().isoformat(timespec="milliseconds"),
        }
        try:
            url = f"{self.folio_client.okapi_url}/source-storage/snapshots"
            res = requests.post(
                url, data=json.dumps(snapshot), headers=self.folio_client.okapi_headers
            )
            res.raise_for_status()
            logging.info("Posted Snapshot to FOLIO: %s", json.dumps(snapshot, indent=4))
            get_url = f"{self.folio_client.okapi_url}/source-storage/snapshots/{self.snapshot_id}"
            getted = False
            while not getted:
                logging.info("Sleeping while waiting for the snapshot to get created")
                time.sleep(5)
                res = requests.get(get_url, headers=self.folio_client.okapi_headers)
                if res.status_code == 200:
                    getted = True
                else:
                    logging.info(res.status_code)
        except Exception:
            logging.exception("Could not post the snapshot")
            sys.exit(1)

    def commit_snapshot(self):
        snapshot = {"jobExecutionId": self.snapshot_id, "status": "COMMITTED"}
        try:
            url = f"{self.folio_client.okapi_url}/source-storage/snapshots/{self.snapshot_id}"
            res = requests.put(
                url, data=json.dumps(snapshot), headers=self.folio_client.okapi_headers
            )
            res.raise_for_status()
            logging.info("Posted Committed snapshot to FOLIO: %s", json.dumps(snapshot, indent=4))
        except Exception:
            logging.exception(
                "Could not commit snapshot with id %s. Post this to /source-storage/snapshots/%s:",
                self.snapshot_id,
                self.snapshot_id,
            )
            logging.info("%s", json.dumps(snapshot, indent=4))
            sys.exit(1)


def list_objects(object_type: str):
    choices = {
        "Extradata": {
            "object_name": "",
            "api_endpoint": "",
            "total_records": False,
            "addSnapshotId": False,
        },
        "Items": {
            "object_name": "items",
            "api_endpoint": "/item-storage/batch/synchronous?upsert=true",
            "total_records": False,
            "addSnapshotId": False,
        },
        "Holdings": {
            "object_name": "holdingsRecords",
            "api_endpoint": "/holdings-storage/batch/synchronous?upsert=true",
            "total_records": False,
            "addSnapshotId": False,
        },
        "Instances": {
            "object_name": "instances",
            "api_endpoint": "/instance-storage/batch/synchronous?upsert=true",
            "total_records": False,
            "addSnapshotId": False,
        },
        "Authorities": {
            "object_name": "",
            "api_endpoint": "/authority-storage/authorities",
            "total_records": False,
            "addSnapshotId": False,
        },
        "SRS": {
            "object_name": "records",
            "api_endpoint": "/source-storage/batch/records",
            "total_records": True,
            "addSnapshotId": True,
        },
        "Users": {
            "object_name": "users",
            "api_endpoint": "/user-import",
            "total_records": True,
            "addSnapshotId": False,
        },
    }

    try:
        return choices[object_type]
    except KeyError:
        key_string = ",".join(choices.keys())
        logging.error(f"Wrong type. Only one of {key_string} are allowed")
        logging.error("Halting")
        sys.exit(1)


def chunks(records, number_of_chunks):
    """Yield successive n-sized chunks from lst.

    Args:
        records (_type_): _description_
        number_of_chunks (_type_): _description_

    Yields:
        _type_: _description_
    """
    for i in range(0, len(records), number_of_chunks):
        yield records[i : i + number_of_chunks]


def get_extradata_endpoint(object_name: str, string_object: str):
    object_types = {
        "precedingSucceedingTitles": "preceding-succeeding-titles",
        "precedingTitles": "preceding-succeeding-titles",
        "succeedingTitles": "preceding-succeeding-titles",
        "boundwithPart": "inventory-storage/bound-with-parts",
        "notes": "notes",
        "course": "coursereserves/courses",
        "courselisting": "coursereserves/courselistings",
    }
    if object_name == "instructor":
        instructor = json.loads(string_object)
        return f'coursereserves/courselistings/{instructor["courseListingId"]}/instructors'
    return object_types[object_name]


def get_human_readable(size, precision=2):
    suffixes = ["B", "KB", "MB", "GB", "TB"]
    suffix_index = 0
    while size > 1024 and suffix_index < 4:
        suffix_index += 1  # increment the index of the suffix
        size = size / 1024.0  # apply the division
    return "%.*f%s" % (precision, size, suffixes[suffix_index])


def get_req_size(response):
    size = response.request.method
    size += response.request.url
    size += "\r\n".join(f"{k}{v}" for k, v in response.request.headers.items())
    size += response.request.body or []
    return get_human_readable(len(size.encode("utf-8")))
