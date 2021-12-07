import json
import logging
import os
import time
import traceback
from abc import abstractmethod
from datetime import datetime

import requests
from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.custom_exceptions import TransformationRecordFailedError
from migration_tools.migration_configuration import MigrationConfiguration
from migration_tools.migration_tasks.migration_task_base import MigrationTaskBase


def write_failed_batch_to_file(batch, file):
    for record in batch:
        file.write(f"{json.dumps(record)}\n")


class BatchPoster(MigrationTaskBase):
    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        return FOLIONamespaces.other

    def __init__(self, configuration: MigrationConfiguration):
        super().__init__(configuration)
        self.failed_ids = []
        self.first_batch = True
        self.object_name = self.configuration.args.object_name
        self.api_path = list_objects()[self.object_name]
        self.failed_objects = []
        object_name_formatted = self.object_name.replace(" ", "").lower()
        time_stamp = time.strftime("%Y%m%d-%H%M%S")
        self.failed_recs_path = os.path.join(
            self.folder_structure.results_folder,
            f"failed_{object_name_formatted}_records_{time_stamp}.json",
        )
        self.batch_size = self.configuration.args.batch_size
        self.processed = 0
        self.failed_batches = 0
        self.failed_records = 0
        self.users_created = 0
        self.users_updated = 0
        self.objects_file = self.configuration.args.objects_file
        self.users_per_group = {}
        self.failed_fields = set()
        self.num_failures = 0

    def do_work(self):
        batch = []
        with open(self.objects_file) as rows, open(
            self.failed_recs_path, "w"
        ) as failed_recs_file:
            last_row = ""
            for num_records, row in enumerate(rows, start=1):
                last_row = row
                if row.strip():
                    try:
                        json_rec = json.loads(row.split("\t")[-1])
                        if num_records == 1:
                            logging.info(json.dumps(json_rec, indent=True))
                        batch.append(json_rec)
                        if len(batch) == int(self.batch_size):
                            self.post_batch(batch, failed_recs_file, num_records)
                            batch = []
                    except UnicodeDecodeError as unicode_error:
                        self.handle_unicode_error(unicode_error, last_row)
                    except Exception as exception:
                        self.handle_generic_exception(
                            exception, last_row, batch, num_records, failed_recs_file
                        )
            if any(batch):
                try:
                    self.post_batch(batch, failed_recs_file, num_records)
                except Exception as exception:
                    self.handle_generic_exception(
                        exception, last_row, batch, num_records, failed_recs_file
                    )
        logging.info("Done posting %s records. ", (num_records))
        logging.info(
            (
                "Failed records: %s failed records in %s "
                "failed batches. Failed records saved to %s"
            ),
            self.failed_records,
            self.failed_batches,
            self.failed_recs_path,
        )

    def handle_generic_exception(
        self, exception, last_row, batch, num_records, failed_recs_file
    ):
        logging.exception("%s", exception)
        logging.error("Failed row: %s", last_row)
        self.failed_batches += 1
        self.failed_records += len(batch)
        write_failed_batch_to_file(batch, failed_recs_file)
        batch = []
        self.num_failures += 0
        if self.num_failures > 50:
            logging.error("Exceeded number of failures at row %s", num_records)
            raise exception
            # Last batch

    def handle_unicode_error(self, unicode_error, last_row):
        logging.info("=========ERROR==============")
        logging.info(
            "%s Posting failed. Encoding error reading file",
            unicode_error,
        )
        logging.info(
            "Failing row, either the one shown here or the next row in %s",
            self.objects_file,
        )
        logging.info(last_row)
        logging.info("=========Stack trace==============")
        traceback.logging.info_exc()
        logging.info("=======================", flush=True)

    def post_batch(self, batch, failed_recs_file, num_records):
        response = self.do_post(batch)
        if response.status_code == 201:
            logging.info(
                (
                    "Posting successful! Total rows: %s Total failed: %s "
                    "in {response.elapsed.total_seconds()}s "
                    "Batch Size: %s Request size: %s "
                ),
                num_records,
                self.failed_records,
                len(batch),
                get_req_size(response),
            )
        elif response.status_code == 200:
            json_report = json.loads(response.text)
            self.users_created += json_report.get("createdRecords", 0)
            self.users_updated += json_report.get("updatedRecords", 0)
            self.failed_records += json_report.get("failedRecords", 0)
            if json_report.get("failedRecords", 0) > 0:
                failed_recs_file.write(response.text)
            logging.info(
                (
                    "Posting successful! Total rows: %s Total failed: %s "
                    "created: %s updated: %s in %ss Batch Size: %s Request size: %s "
                ),
                num_records,
                self.failed_records,
                self.users_created,
                self.users_updated,
                response.elapsed.total_seconds(),
                len(batch),
                get_req_size(response),
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
        else:
            raise TransformationRecordFailedError(
                "",
                f"HTTP {response.status_code}\t"
                f"Request size: {get_req_size(response)}"
                f"{datetime.utcnow().isoformat()} UTC\n",
                json.dumps(response, indent=4),
            )

    def do_post(self, batch):
        kind = list_objects()[self.object_name]
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

    @staticmethod
    @abstractmethod
    def add_arguments(sub_parser):
        MigrationTaskBase.add_common_arguments(sub_parser)
        sub_parser.add_argument(
            "--timestamp",
            help=(
                "timestamp or migration identifier. "
                "Used to chain multiple runs together"
            ),
            default=time.strftime("%Y%m%d-%H%M%S"),
            secure=False,
        )
        MigrationTaskBase.add_argument(sub_parser, "objects_file", "path data file")
        MigrationTaskBase.add_argument(sub_parser, "batch_size", "batch size")
        MigrationTaskBase.add_argument(
            sub_parser,
            "object_name",
            "What objects to batch post",
            choices=list(list_objects().keys()),
        )


def list_objects():
    return {
        "Items": {
            "object_name": "items",
            "api_endpoint": "/item-storage/batch/synchronous",
            "total_records": False,
        },
        "Holdings": {
            "object_name": "holdingsRecords",
            "api_endpoint": "/holdings-storage/batch/synchronous",
            "total_records": False,
        },
        "Instances": {
            "object_name": "instances",
            "api_endpoint": "/instance-storage/batch/synchronous",
            "total_records": False,
        },
        "SRS": {
            "object_name": "records",
            "api_endpoint": "/source-storage/batch/records",
            "total_records": True,
        },
        "Users": {
            "object_name": "users",
            "api_endpoint": "/user-import",
            "total_records": True,
        },
    }


def chunks(records, number_of_chunks):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(records), number_of_chunks):
        yield records[i : i + number_of_chunks]


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
    size += "\r\n".join(
        "{}{}".format(k, v) for k, v in response.request.headers.items()
    )
    size += response.request.body or []
    return get_human_readable(len(size.encode("utf-8")))
