import logging
import sys
import json

from migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.mapping_file_transformation.ref_data_mapping import RefDataMapping
from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.migration_report import MigrationReport
from migration_tools.report_blurbs import Blurbs


class MapperBase:
    def __init__(self, library_configuration: LibraryConfiguration):
        logging.info("MapperBase initiating")
        self.library_configuration: LibraryConfiguration = library_configuration

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

    def get_mapped_name(
        self,
        ref_dat_mapping: RefDataMapping,
        legacy_object,
        index_or_id,
        folio_property_name="",
        prevent_default=False,
    ):
        try:
            # Get the values in the fields that will be used for mapping
            fieldvalues = [
                legacy_object.get(k) for k in ref_dat_mapping.mapped_legacy_keys
            ]

            # Gets the first line in the map satisfying all legacy mapping values.
            # Case insensitive, strips away whitespace
            # TODO: add option for Wild card matching in individual columns
            right_mapping = self.get_ref_data_mapping(legacy_object, ref_dat_mapping)

            if not right_mapping:
                raise StopIteration()
            self.migration_report.add(
                Blurbs.ReferenceDataMapping,
                (
                    f'{ref_dat_mapping.name} mapping - {" - ".join(fieldvalues)} '
                    f'-> {right_mapping[f"folio_{ref_dat_mapping.key_type}"]}'
                ),
            )
            return next(v for k, v in right_mapping.items() if k.startswith("folio_"))

        except StopIteration:
            if prevent_default:
                self.migration_report.add(
                    Blurbs.ReferenceDataMapping,
                    (
                        f"{ref_dat_mapping.name} mapping - Not to be mapped. "
                        f'(No default) -- {" - ".join(fieldvalues)} -> ""'
                    ),
                )
                return ""
            self.migration_report.add(
                Blurbs.ReferenceDataMapping,
                (
                    f"{ref_dat_mapping.name} mapping - Unmapped (Default value was set) -- "
                    f'{" - ".join(fieldvalues)} -> {ref_dat_mapping.default_name}'
                ),
            )
            return ref_dat_mapping.default_name
        except IndexError as exception:
            raise TransformationRecordFailedError(
                index_or_id,
                (
                    f"{ref_dat_mapping.name} - folio_{ref_dat_mapping.key_type} "
                    f"({ref_dat_mapping.mapped_legacy_keys}) {exception} is not "
                    "a recognized field in the legacy data."
                ),
            )
        except Exception as exception:
            raise TransformationRecordFailedError(
                index_or_id,
                (
                    f"{ref_dat_mapping.name} - folio_{ref_dat_mapping.key_type} "
                    f"({ref_dat_mapping.mapped_legacy_keys}) {exception}"
                ),
            )

    def get_mapped_value(
        self,
        ref_dat_mapping: RefDataMapping,
        legacy_object,
        index_or_id,
        folio_property_name="",
        prevent_default=False,
    ):

        # Gets mapped value from mapping file, translated to the right FOLIO UUID
        try:
            # Get the values in the fields that will be used for mapping
            fieldvalues = [
                legacy_object.get(k) for k in ref_dat_mapping.mapped_legacy_keys
            ]

            # Gets the first line in the map satisfying all legacy mapping values.
            # Case insensitive, strips away whitespace
            # TODO: add option for Wild card matching in individual columns
            right_mapping = self.get_ref_data_mapping(legacy_object, ref_dat_mapping)
            if not right_mapping:
                # Not all fields matched. Could it be a hybrid wildcard map?
                right_mapping = self.get_hybrid_mapping(legacy_object, ref_dat_mapping)

            if not right_mapping:
                raise StopIteration()
            self.migration_report.add(
                Blurbs.ReferenceDataMapping,
                (
                    f'{ref_dat_mapping.name} mapping - {" - ".join(fieldvalues)} '
                    f'-> {right_mapping[f"folio_{ref_dat_mapping.key_type}"]}'
                ),
            )
            return right_mapping["folio_id"]
        except StopIteration:
            if prevent_default:
                self.migration_report.add(
                    Blurbs.ReferenceDataMapping,
                    (
                        f"{ref_dat_mapping.name} mapping - Not to be mapped. "
                        f'(No default) -- {" - ".join(fieldvalues)} -> ""'
                    ),
                )
                return ""
            self.migration_report.add(
                Blurbs.ReferenceDataMapping,
                (
                    f"{ref_dat_mapping.name} mapping - Unmapped (Default value was set) -- "
                    f'{" - ".join(fieldvalues)} -> {ref_dat_mapping.default_name}'
                ),
            )
            return ref_dat_mapping.default_id
        except IndexError as exception:
            raise TransformationRecordFailedError(
                index_or_id,
                (
                    f"{ref_dat_mapping.name} - folio_{ref_dat_mapping.key_type} "
                    f"({ref_dat_mapping.mapped_legacy_keys}) {exception} is not "
                    "a recognized field in the legacy data."
                ),
            )
        except Exception as exception:
            raise TransformationRecordFailedError(
                index_or_id,
                (
                    f"{ref_dat_mapping.name} - folio_{ref_dat_mapping.key_type} "
                    f"({ref_dat_mapping.mapped_legacy_keys}) {exception}"
                ),
            )

    @staticmethod
    def get_hybrid_mapping(legacy_object, rdm: RefDataMapping):
        highest_match = None
        highest_match_number = 0
        for mapping in rdm.hybrid_mappings:
            match_numbers = []
            for k in rdm.mapped_legacy_keys:
                if mapping[k].strip() == legacy_object[k].strip():
                    match_numbers.append(10)
                elif mapping[k].strip() == "*":
                    match_numbers.append(1)
            summa = sum(match_numbers)
            if summa > highest_match_number and min(match_numbers) > 0:
                highest_match_number = summa
                highest_match = mapping
        return highest_match

    @staticmethod
    def get_ref_data_mapping(legacy_object, rdm: RefDataMapping):
        for mapping in rdm.regular_mappings:
            match_number = sum(
                legacy_object[k].strip() == mapping[k].strip()
                for k in rdm.mapped_legacy_keys
            )
            if match_number == len(rdm.mapped_legacy_keys):
                return mapping
        return None

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
            self.num_criticalerrors / (records_processed + 1)
            > (self.library_configuration.failed_percentage_threshold / 100)
            and self.num_criticalerrors
            > self.library_configuration.failed_records_threshold
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
