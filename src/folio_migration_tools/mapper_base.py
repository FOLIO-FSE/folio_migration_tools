import json
import logging
import sys
from datetime import datetime
from datetime import timezone
from pathlib import Path

import requests
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_blurbs import Blurbs


class MapperBase:

    legacy_id_template = "Identifier(s) from previous system:"
    bib_id_template = "Bib id: "

    def __init__(
        self,
        library_configuration: LibraryConfiguration,
        folio_client: FolioClient,
        parent_id_map: dict[str, tuple] = None,
    ):
        logging.info("MapperBase initiating")
        self.parent_id_map: dict[str, tuple] = parent_id_map
        self.extradata_writer: ExtradataWriter = ExtradataWriter(Path(""))
        self.start_datetime = datetime.now(timezone.utc)
        self.folio_client: FolioClient = folio_client
        self.library_configuration: LibraryConfiguration = library_configuration

        self.mapped_folio_fields: dict = {}
        self.migration_report = MigrationReport()
        self.num_criticalerrors = 0
        self.num_exeptions = 0
        self.mapped_legacy_fields: dict = {}
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
            for field_name in set(flatten(folio_record)):
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
            raise ee from ee

    def report_legacy_mapping_no_schema(self, legacy_object):
        for field_name, value in legacy_object.items():
            v = 1 if value else 0
            if field_name not in self.mapped_legacy_fields:
                self.mapped_legacy_fields[field_name] = [1, v]
            else:
                self.mapped_legacy_fields[field_name][0] += 1
                self.mapped_legacy_fields[field_name][1] += v

    def report_folio_mapping_no_schema(self, folio_object):
        for field_name in set(flatten(folio_object)):
            if field_name not in self.mapped_folio_fields:
                self.mapped_folio_fields[field_name] = [1, 1]
            else:
                self.mapped_folio_fields[field_name][0] += 1
                self.mapped_folio_fields[field_name][1] += 1

    def reset_instance_hrid_counter(self):
        logging.info("Resetting Instances HRID settings to 1")
        self.instance_hrid_counter = 1
        self.migration_report.set(
            Blurbs.GeneralStatistics, "Instances HRID starting number", self.instance_hrid_counter
        )
        self.store_hrid_settings()

    def reset_holdings_hrid_counter(self):
        logging.info("Resetting Holdings HRID settings to 1")
        self.holdings_hrid_counter = 1
        self.migration_report.set(
            Blurbs.GeneralStatistics, "Holdings HRID starting number", self.holdings_hrid_counter
        )
        self.store_hrid_settings()

    def reset_item_hrid_counter(self):
        logging.info("Resetting Items HRID settings to 1")
        self.items_hrid_counter = 1
        self.migration_report.set(
            Blurbs.GeneralStatistics, "Items HRID starting number", self.items_hrid_counter
        )
        self.store_hrid_settings()

    def get_mapped_name(
        self,
        ref_data_mapping: RefDataMapping,
        legacy_object,
        index_or_id,
        folio_property_name="",
        prevent_default=False,
    ):
        try:
            # Get the values in the fields that will be used for mapping
            fieldvalues = [legacy_object.get(k) for k in ref_data_mapping.mapped_legacy_keys]

            # Gets the first line in the map satisfying all legacy mapping values.
            # Case insensitive, strips away whitespace
            right_mapping = ref_data_mapping.get_ref_data_mapping(legacy_object)

            if not right_mapping:
                raise StopIteration()
            self.migration_report.add(
                ref_data_mapping.blurb,
                (
                    f'{" - ".join(fieldvalues)} '
                    f'-> {right_mapping[f"folio_{ref_data_mapping.key_type}"]}'
                ),
            )
            return next(v for k, v in right_mapping.items() if k.startswith("folio_"))

        except StopIteration:
            if prevent_default:
                self.migration_report.add(
                    ref_data_mapping.blurb,
                    (f"Not to be mapped. " f'(No default) -- {" - ".join(fieldvalues)} -> ""'),
                )
                return ""
            self.migration_report.add(
                ref_data_mapping.blurb,
                (
                    f"Unmapped (Default value was set) -- "
                    f'{" - ".join(fieldvalues)} -> {ref_data_mapping.default_name}'
                ),
            )
            return ref_data_mapping.default_name
        except IndexError as exception:
            raise TransformationRecordFailedError(
                index_or_id,
                (
                    f"{ref_data_mapping.name} - folio_{ref_data_mapping.key_type} "
                    f"({ref_data_mapping.mapped_legacy_keys}) {exception} is not "
                    "a recognized field in the legacy data."
                ),
            ) from exception
        except KeyError as exception:
            raise TransformationProcessError(
                index_or_id,
                (
                    f"{ref_data_mapping.name} mapping - folio_{ref_data_mapping.key_type} "
                    f"({ref_data_mapping.mapped_legacy_keys})  is not "
                    f"a recognized field in the legacy data. KeyError: {exception}"
                ),
            ) from exception
        except Exception as exception:
            raise TransformationProcessError(
                index_or_id,
                (
                    f"{ref_data_mapping.name} - folio_{ref_data_mapping.key_type} "
                    f"({ref_data_mapping.mapped_legacy_keys}) {exception}"
                ),
            ) from exception

    def get_mapped_value(
        self,
        ref_data_mapping: RefDataMapping,
        legacy_object,
        index_or_id,
        folio_property_name="",
        prevent_default=False,
    ):

        # Gets mapped value from mapping file, translated to the right FOLIO UUID
        try:
            # Get the values in the fields that will be used for mapping
            fieldvalues = [legacy_object.get(k) for k in ref_data_mapping.mapped_legacy_keys]

            # Gets the first line in the map satisfying all legacy mapping values.
            # Case insensitive, strips away whitespace
            right_mapping = ref_data_mapping.get_ref_data_mapping(legacy_object)
            if not right_mapping:
                # Not all fields matched. Could it be a hybrid wildcard map?
                right_mapping = ref_data_mapping.get_hybrid_mapping(legacy_object)

            if not right_mapping:
                raise StopIteration()
            self.migration_report.add(
                ref_data_mapping.blurb,
                (
                    f'{" - ".join(fieldvalues)} '
                    f'-> {right_mapping[f"folio_{ref_data_mapping.key_type}"]}'
                ),
            )
            return right_mapping["folio_id"]
        except StopIteration:
            if prevent_default:
                self.migration_report.add(
                    ref_data_mapping.blurb,
                    (f"Not to be mapped. " f'(No default) -- {" - ".join(fieldvalues)} -> ""'),
                )
                return ""
            self.migration_report.add(
                ref_data_mapping.blurb,
                (
                    f"Unmapped (Default value was set) -- "
                    f'{" - ".join(fieldvalues)} -> {ref_data_mapping.default_name}'
                ),
            )
            return ref_data_mapping.default_id
        except IndexError as exception:
            raise TransformationRecordFailedError(
                index_or_id,
                (
                    f"{ref_data_mapping.name} - folio_{ref_data_mapping.key_type} "
                    f"({ref_data_mapping.mapped_legacy_keys}) {exception} is not "
                    "a recognized field in the legacy data."
                ),
            ) from exception
        except Exception as exception:
            raise TransformationRecordFailedError(
                index_or_id,
                (
                    f"{ref_data_mapping.name} - folio_{ref_data_mapping.key_type} "
                    f"({ref_data_mapping.mapped_legacy_keys}) {exception}"
                ),
            ) from exception

    def handle_transformation_field_mapping_error(self, index_or_id, error):
        self.migration_report.add(Blurbs.FieldMappingErrors, error)
        error.id = error.id or index_or_id
        error.log_it()
        self.migration_report.add_general_statistics("Field Mapping Errors found")

    def handle_transformation_process_error(self, idx, error: TransformationProcessError):
        self.migration_report.add_general_statistics("Transformation process error")
        logging.critical("%s\t%s", idx, error)
        print(f"\n{error.message}: {error.data_value}")
        sys.exit(1)

    def handle_transformation_record_failed_error(
        self, records_processed: int, error: TransformationRecordFailedError
    ):
        self.migration_report.add(Blurbs.GeneralStatistics, "Records failed due to an error")
        error.index_or_id = error.index_or_id or records_processed
        error.log_it()
        self.num_criticalerrors += 1
        if (
            self.num_criticalerrors / (records_processed + 1)
            > (self.library_configuration.failed_percentage_threshold / 100)
            and self.num_criticalerrors > self.library_configuration.failed_records_threshold
        ):
            logging.fatal(
                (
                    "Stopping. More than %s critical data errors. "
                    "Threshold reached. Fix error or raise the bar."
                ),
                self.num_criticalerrors,
            )
            logging.error(
                "Errors: %s\terrors/records: %s",
                self.num_criticalerrors,
                (self.num_criticalerrors / (records_processed + 1)),
            )
            sys.exit(1)

    def get_id_map_tuple(self, legacy_id: str, folio_record: dict, object_type: FOLIONamespaces):
        if object_type == FOLIONamespaces.instances:
            return (legacy_id, folio_record["id"], folio_record["hrid"])
        return (legacy_id, folio_record["id"])

    def handle_generic_exception(self, idx, excepion: Exception):
        self.num_exeptions += 1
        print("\n=======ERROR===========")
        print(
            f"Row {idx:,} failed with the following unhandled Exception: {excepion}  "
            f"of type {type(excepion).__name__}"
        )
        logging.error(excepion, exc_info=True)
        if self.num_exeptions > 50:
            logging.fatal(
                "Stopping. More than %s unhandled exceptions. Code needs fixing",
                self.num_exeptions,
            )
            sys.exit(1)

    def save_id_map_file(self, path, legacy_map: dict):
        with open(path, "w") as legacy_map_file:
            for _idx, id_string in enumerate(legacy_map.values(), start=1):
                legacy_map_file.write(f"{json.dumps(id_string)}\n")
            logging.info("Wrote %s id:s to legacy map", _idx)
            self.migration_report.set(
                Blurbs.GeneralStatistics, "Unique ID:s written to legacy map", _idx
            )

    def store_hrid_settings(self):
        logging.info("Setting HRID counter to current")
        try:

            if self.hrids_not_updated():
                logging.info("NOT POSTing HRID settings, since did not change.")
                return

            self.hrid_settings["instances"]["startNumber"] = self.instance_hrid_counter
            self.hrid_settings["holdings"]["startNumber"] = self.holdings_hrid_counter
            self.hrid_settings["items"]["startNumber"] = self.items_hrid_counter
            url = self.folio_client.okapi_url + self.hrid_path
            resp = requests.put(
                url,
                data=json.dumps(self.hrid_settings),
                headers=self.folio_client.okapi_headers,
            )
            resp.raise_for_status()
            logging.info("%s Successfully set HRID settings.", resp.status_code)
            a = self.folio_client.folio_get_single_object(self.hrid_path)
            logging.info("Current hrid settings: %s", json.dumps(a, indent=4))
        except Exception:
            logging.exception(
                f"Something went wrong when setting the HRID settings. "
                f"Update them manually. {json.dumps(self.hrid_settings)}"
            )

    def hrids_not_updated(self):
        return (
            self.hrid_settings["instances"]["startNumber"] == self.instance_hrid_counter
            and self.hrid_settings["holdings"]["startNumber"] == self.holdings_hrid_counter
            and self.hrid_settings["items"]["startNumber"] == self.items_hrid_counter
        )

    @staticmethod
    def validate_required_properties(
        legacy_id, folio_object: dict, schema: dict, object_type: FOLIONamespaces
    ):
        cleaned_folio_object = MapperBase.clean_none_props(folio_object)
        required = []
        missing = []
        if object_type != FOLIONamespaces.note:
            required = schema.get("required", [])
            missing = list(MapperBase.list_missing(required, cleaned_folio_object))
        else:
            required = (
                schema.get("properties", {}).get("notes", {}).get("items", {}).get("required", [])
            )
            for note in cleaned_folio_object.get("notes", []):
                missing.extend(MapperBase.list_missing(required, note))

        if any(missing):
            raise TransformationRecordFailedError(
                legacy_id,
                "One or many required properties empty",
                f"{json.dumps(missing)}",
            )
        cleaned_folio_object.pop("type", None)
        return cleaned_folio_object

    @staticmethod
    def list_missing(required: list, cleaned_folio_object: dict):
        for required_prop in required:
            if required_prop not in cleaned_folio_object:
                yield f"Missing: {required_prop}"
            elif not cleaned_folio_object[required_prop]:
                yield f"Empty: {required_prop}"

    @staticmethod
    def clean_none_props(d: dict):
        clean = {}
        for k, v in d.items():
            if isinstance(v, dict):
                nested = MapperBase.clean_none_props(v)
                if len(nested.keys()) > 0:
                    clean[k] = nested
            elif isinstance(v, list):
                clean[k] = list(filter(None, v))
            elif v is not None:
                clean[k] = v
        return clean

    def add_legacy_id_to_admin_note(self, folio_record: dict, legacy_id: str):
        if not legacy_id:
            raise TransformationFieldMappingError(legacy_id, "Legacy id is empty", legacy_id)
        if "administrativeNotes" not in folio_record:
            folio_record["administrativeNotes"] = []
        if id_string := next(
            (f for f in folio_record["administrativeNotes"] if MapperBase.legacy_id_template in f),
            None,
        ):
            if legacy_id not in id_string:
                folio_record["administrativeNotes"] = [
                    f
                    for f in folio_record["administrativeNotes"]
                    if MapperBase.legacy_id_template not in f
                ]

                folio_record["administrativeNotes"].append(f"{id_string}, {legacy_id}")
        else:
            folio_record["administrativeNotes"].append(
                f"{MapperBase.legacy_id_template} {legacy_id}"
            )


def flatten(my_dict: dict, path=""):

    for k, v in iter(my_dict.items()):
        if not path:
            yield k
        if v:
            if isinstance(v, list):
                if path and check_if_list_with_dict_keys(v):
                    yield f"{path}.{k}".strip(".")
                for e in v:
                    if isinstance(e, dict):
                        yield from flatten(dict(e), f"{path}.{k}")
                    elif isinstance(e, str) and path:
                        yield f"{path}.{k}".strip(".")

            elif isinstance(v, dict):
                if path:
                    yield f"{path}.{k}".strip(".")
                yield from flatten(dict(v), f"{path}.{k}")
            elif path:
                yield f"{path}.{k}".strip(".")


def check_if_list_with_dict_keys(data):
    return isinstance(data, list) and all(isinstance(x, dict) for x in data)
