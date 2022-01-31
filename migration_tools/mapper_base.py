import logging
import sys
import json

from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.migration_report import MigrationReport
from migration_tools.report_blurbs import Blurbs


class MapperBase:
    def __init__(self):
        logging.info("MapperBase initiating")
        self.mapped_folio_fields = {}
        self.migration_report = MigrationReport()
        self.num_criticalerrors = 0
        self.num_exeptions = 0
        self.mapped_legacy_fields = {}
        self.schema_properties = None

    def report_legacy_mapping(self, field_name, present, mapped):
        if field_name:
            try:
                self.mapped_legacy_fields[field_name][0] += int(present)
                self.mapped_legacy_fields[field_name][1] += int(mapped)
            except KeyError:
                self.mapped_legacy_fields[field_name] = [int(present), int(mapped)]

    def report_folio_mapping(self, folio_record, schema):
        try:
            for field_name in flatten(folio_record):
                try:
                    self.mapped_folio_fields[field_name][0] += 1
                except KeyError:
                    self.mapped_folio_fields[field_name] = [1]
            if not self.schema_properties:
                self.schema_properties = schema["properties"].keys()

            unmatched_properties = (
                p for p in self.schema_properties if p not in folio_record.keys()
            )
            for prop in unmatched_properties:
                if prop not in self.mapped_folio_fields:
                    self.mapped_folio_fields[prop] = [0]
        except Exception as ee:
            logging.error(ee, stack_info=True)
            raise ee

    def handle_transformation_field_mapping_error(self, index_or_id, error):
        self.migration_report.add(Blurbs.FieldMappingErrors, error)
        error.id = error.id or index_or_id
        error.log_it()
        self.migration_report.add_general_statistics("Field Mapping Errors found")

    def handle_transformation_process_error(
        self, idx, error: TransformationProcessError
    ):
        self.migration_report.add_general_statistics("Transformation process error")
        logging.critical("%s\t%s", idx, error)
        sys.exit()

    def handle_transformation_record_failed_error(
        self, records_processed: int, error: TransformationRecordFailedError
    ):
        self.migration_report.add(
            Blurbs.GeneralStatistics, "Records failed due to an error"
        )
        error.id = error.index_or_id or records_processed
        error.log_it()
        self.num_criticalerrors += 1
        if (
            self.num_criticalerrors / (records_processed + 1) > 0.2
            and self.num_criticalerrors > 5000
        ):
            logging.fatal(
                "Stopping. More than %s critical data errors", self.num_criticalerrors
            )
            logging.error(
                "Errors: %s\terrors/records: %s",
                self.num_criticalerrors,
                (self.num_criticalerrors / (records_processed + 1)),
            )
            sys.exit()

    @staticmethod
    def get_id_map_dict(legacy_id, folio_record):
        return {"legacy_id": legacy_id, "folio_id": folio_record["id"]}

    def handle_generic_exception(self, idx, excepion: Exception):
        self.num_exeptions += 1
        print("\n=======ERROR===========")
        print(
            f"Row {idx:,} failed with the following unhandled Exception: {excepion}  "
            f"of type {type(excepion).__name__}"
        )
        logging.error(excepion, exc_info=True)
        if self.num_exeptions > 500:
            logging.fatal(
                "Stopping. More than %s unhandled exceptions. Code needs fixing",
                self.num_exeptions,
            )
            sys.exit()

    @staticmethod
    def save_id_map_file(path, legacy_map: dict):
        with open(path, "w") as legacy_map_file:
            for id_string in legacy_map.values():
                legacy_map_file.write(f"{json.dumps(id_string)}\n")
            logging.info("Wrote %s id:s to legacy map", len(legacy_map))


def flatten(my_dict: dict, path=""):
    for k, v in iter(my_dict.items()):
        if v:
            if isinstance(v, list):
                for e in v:
                    if isinstance(e, dict):
                        yield from (flatten(dict(e), path + "." + k))
            elif isinstance(v, dict):
                yield from flatten(dict(v), path + "." + k)
            else:
                yield (path + "." + k).strip(".")
