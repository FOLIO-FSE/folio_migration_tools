import ast
import copy
import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient
from pymarc import Record

from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition, LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.task_configuration import AbstractTaskConfiguration


class MapperBase:
    legacy_id_template = "Identifier(s) from previous system:"
    bib_id_template = "Bib id: "

    def __init__(
        self,
        library_configuration: LibraryConfiguration,
        task_configuration: AbstractTaskConfiguration,
        folio_client: FolioClient,
        parent_id_map: Dict[str, Tuple] = {},
    ):
        logging.info("MapperBase initiating")
        self.parent_id_map: dict[str, tuple] = parent_id_map
        self.extradata_writer: ExtradataWriter = ExtradataWriter(Path(""))
        self.start_datetime = datetime.now(timezone.utc)
        self.folio_client: FolioClient = folio_client
        self.library_configuration: LibraryConfiguration = library_configuration
        self.task_configuration: AbstractTaskConfiguration = task_configuration
        self.mapped_folio_fields: dict = {}
        self.migration_report: MigrationReport = MigrationReport()
        self.num_criticalerrors = 0
        self.num_exceptions = 0
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

    def get_mapped_name(
        self,
        ref_data_mapping: RefDataMapping,
        legacy_object,
        index_or_id,
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
                ref_data_mapping.blurb_id,
                (
                    f'{" - ".join(fieldvalues)} '
                    f'-> {right_mapping[f"folio_{ref_data_mapping.key_type}"]}'
                ),
            )
            return next(v for k, v in right_mapping.items() if k.startswith("folio_"))

        except StopIteration:
            if prevent_default:
                self.migration_report.add(
                    ref_data_mapping.blurb_id,
                    (f"Not to be mapped. " f'(No default) -- {" - ".join(fieldvalues)} -> ""'),
                )
                return ""
            self.migration_report.add(
                ref_data_mapping.blurb_id,
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

    def get_mapped_ref_data_value(
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
                ref_data_mapping.blurb_id,
                (
                    f'{" - ".join(fieldvalues)} '
                    f'-> {right_mapping[f"folio_{ref_data_mapping.key_type}"]}'
                ),
            )
            return right_mapping["folio_id"]
        except StopIteration:
            if prevent_default:
                self.migration_report.add(
                    ref_data_mapping.blurb_id,
                    (f"Not to be mapped. " f'(No default) -- {" - ".join(fieldvalues)} -> ""'),
                )
                return ""
            self.migration_report.add(
                ref_data_mapping.blurb_id,
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
        self.migration_report.add("FieldMappingErrors", error)
        error.id = error.id or index_or_id
        error.log_it()
        self.migration_report.add_general_statistics(i18n.t("Field Mapping Errors found"))

    def handle_transformation_process_error(self, idx, error: TransformationProcessError):
        self.migration_report.add_general_statistics(i18n.t("Transformation process error"))
        logging.critical("%s\t%s", idx, error)
        print(f"\n{error.message}: {error.data_value}")
        sys.exit(1)

    def handle_transformation_record_failed_error(
        self, records_processed: int, error: TransformationRecordFailedError
    ):
        self.migration_report.add(
            "GeneralStatistics", i18n.t("FAILED Records failed due to an error")
        )
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
        if all(
            [
                object_type == FOLIONamespaces.instances,
                (not getattr(self.task_configuration, "data_import_marc", False)),
                getattr(self, "create_source_records", True),
            ]
        ):
            return (legacy_id, folio_record["id"], folio_record["hrid"])
        return (legacy_id, folio_record["id"])

    def handle_generic_exception(self, idx, exception: Exception):
        self.num_exceptions += 1
        print("\n=======ERROR===========")
        print(
            f"Row {idx:,} failed with the following unhandled Exception: {exception}  "
            f"of type {type(exception).__name__}"
        )
        logging.error(exception, exc_info=True)
        if self.num_exceptions > self.library_configuration.generic_exception_threshold:
            logging.fatal(
                "Stopping. More than %s unhandled exceptions. Code needs fixing",
                self.num_exceptions,
            )
            sys.exit(1)

    def save_id_map_file(self, path, legacy_map: dict):
        with open(path, "w") as legacy_map_file:
            for id_string in legacy_map.values():
                legacy_map_file.write(f"{json.dumps(id_string)}\n")
                self.migration_report.add(
                    "GeneralStatistics", i18n.t("Unique ID:s written to legacy map")
                )
        logging.info("Wrote legacy id map to %s", path)

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
        if object_type not in [FOLIONamespaces.users]:
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
            raise TransformationFieldMappingError(
                legacy_id, i18n.t("Legacy id is empty"), legacy_id
            )
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

    def create_and_write_boundwith_part(self, legacy_item_id: str, bound_with_holding_uuid: dict):
        part = {
            "id": str(uuid.uuid4()),
            "holdingsRecordId": bound_with_holding_uuid,
            "itemId": str(
                FolioUUID(
                    self.base_string_for_folio_uuid,
                    FOLIONamespaces.items,
                    legacy_item_id,
                )
            ),
        }
        self.extradata_writer.write("boundwithPart", part)

    def create_bound_with_holdings(
        self,
        folio_holding: dict,
        instance_ids: list,
        bound_with_holdings_type_id: str,
    ):
        if not bound_with_holdings_type_id:
            raise TransformationProcessError(
                "Missing task setting holdingsTypeUuidForBoundwiths. Add a "
                "holdingstype specifically for boundwith holdings and reference "
                "the UUID in this parameter."
            )
        for bwidx, instance_uuid in enumerate(instance_ids):
            if not instance_uuid:
                raise ValueError(f"No Instance ID for record {folio_holding}")
            bound_with_holding = copy.deepcopy(folio_holding)
            bound_with_holding["instanceId"] = instance_uuid

            if call_number := folio_holding.get("callNumber", None):
                if "[" in call_number:
                    try:
                        call_numbers: List = ast.literal_eval(str(folio_holding["callNumber"]))
                        bound_with_holding["callNumber"] = call_numbers[bwidx]
                    except IndexError:
                        if call_numbers:
                            bound_with_holding["callNumber"] = call_numbers[0]
                    except (SyntaxError, ValueError):
                        bound_with_holding["callNumber"] = call_number
                else:
                    bound_with_holding["callNumber"] = call_number
            bound_with_holding["holdingsTypeId"] = bound_with_holdings_type_id

            # The subsequent copies gets different ids, but the original is maintained.
            if bwidx > 0:
                bound_with_holding["id"] = self.generate_boundwith_holding_uuid(
                    folio_holding["id"], instance_uuid
                )
                if bound_with_holding.get("hrid", ""):
                    bound_with_holding["hrid"] = f'{bound_with_holding["hrid"]}_bw_{bwidx}'
            self.migration_report.add_general_statistics(i18n.t("Bound-with holdings created"))
            yield bound_with_holding

    def generate_boundwith_holding_uuid(self, holding_uuid, instance_uuid):
        return str(
            FolioUUID(
                self.base_string_for_folio_uuid,
                FOLIONamespaces.holdings,
                f"{holding_uuid}-{instance_uuid}",
            )
        )

    def map_statistical_codes(self, folio_record: dict, file_def: FileDefinition, legacy_record: Optional[Union[dict, Record]] = None):
        """Map statistical codes to the folio record.

        This method checks if the file definition contains statistical codes and
        if so, it splits the codes by the multi-field delimiter and adds them to
        the folio record's 'statisticalCodeIds' field. If the field does not exist,
        it initializes it as an empty list before appending the codes.

        Args:
            folio_record (dict): The FOLIO record to which the statistical codes will be added.
            file_def (FileDefinition): The file definition containing the statistical codes.
            legacy_record (Optional[Union[dict, Record]]): The legacy record from which the statistical codes are derived.
        """
        if file_def.statistical_code:
            code_strings = file_def.statistical_code.split(
                self.library_configuration.multi_field_delimiter
            )
            folio_record["statisticalCodeIds"] = folio_record.get("statisticalCodeIds", []) + code_strings

    def setup_statistical_codes_map(self, statistical_codes_map):
        if statistical_codes_map:
            self.statistical_codes_mapping = RefDataMapping(
                self.folio_client,
                "/statistical-codes",
                "statisticalCodes",
                statistical_codes_map,
                "code",
                "StatisticalCodeMapping",
            )
            logging.info(f"Statistical codes mapping set up {self.statistical_codes_mapping.mapped_legacy_keys}")
        else:
            self.statistical_codes_mapping = None
            logging.info("Statistical codes map is not set up")

    def get_statistical_code(self, legacy_item: dict, folio_prop_name: str, index_or_id):
        if self.statistical_codes_mapping:
            return self.get_mapped_ref_data_value(
                self.statistical_codes_mapping,
                legacy_item,
                index_or_id,
                folio_prop_name,
                True,
            )
        self.migration_report.add(
            "StatisticalCodeMapping",
            i18n.t("Mapping not set up"),
        )
        return ""

    def map_statistical_code_ids(
        self, legacy_ids, folio_record: dict
    ):
        if stat_codes := {x: None for x in folio_record.pop("statisticalCodeIds", [])}:
            folio_code_ids = set()
            for stat_code in stat_codes:
                if stat_code_id := self.get_statistical_code({"legacy_stat_code": stat_code}, "statisticalCodeId", legacy_ids):
                    folio_code_ids.add(stat_code_id)
                else:
                    Helper.log_data_issue(
                        legacy_ids,
                        i18n.t(
                            "Statistical code '%{code}' not found in FOLIO",
                            code=stat_code,
                        ),
                        stat_code,
                    )
            folio_record["statisticalCodeIds"] = list(folio_code_ids)

    @property
    def base_string_for_folio_uuid(self):
        if self.library_configuration.use_gateway_url_for_uuids and not self.library_configuration.is_ecs:
            return str(self.folio_client.gateway_url)
        elif self.library_configuration.ecs_tenant_id:
            return str(self.library_configuration.ecs_tenant_id)
        else:
            return str(self.library_configuration.tenant_id)

    @staticmethod
    def validate_location_map(location_map: List[Dict], locations: List[Dict]) -> List[Dict]:
        mapped_codes = [x['folio_code'] for x in location_map]
        existing_codes = [x['code'] for x in locations]
        missing_codes = set(mapped_codes) - set(existing_codes)
        if missing_codes:
            raise TransformationProcessError(
                "",
                f"Location map contains codes not found in locations: {', '.join(missing_codes)}",
                "",
            )
        return location_map

    @staticmethod
    def get_object_type() -> FOLIONamespaces:
        raise NotImplementedError("This method should be overridden in subclasses")

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
