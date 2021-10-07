import collections
import csv
import json
import logging
import uuid
from abc import abstractmethod

from folioclient import FolioClient
from migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.mapping_file_transformation.ref_data_mapping import RefDataMapping
from migration_tools.report_blurbs import Blurbs

empty_vals = ["Not mapped", None, ""]


class MapperBase:
    def __init__(self, folio_client: FolioClient, schema, record_map):
        self.schema = schema
        self.stats = {}
        self.total_records = 0
        self.migration_report = {}
        self.folio_client = folio_client
        self.mapped_folio_fields = {}
        self.mapped_legacy_fields = {}
        self.use_map = True  # Legacy
        self.record_map = record_map
        self.num_exeptions = 0
        self.num_criticalerrors = 0
        self.ref_data_dicts = {}
        self.empty_vals = empty_vals
        self.folio_keys = self.get_mapped_folio_properties_from_map(self.record_map)
        self.field_map = {}  # Map of folio_fields and source fields as an array
        for k in self.record_map["data"]:
            if not self.field_map.get(k["folio_field"]):
                self.field_map[k["folio_field"]] = [k["legacy_field"]]
            else:
                self.field_map[k["folio_field"]].append(k["legacy_field"])
        self.validate_map()
        self.mapped_from_values = {}
        for k in self.record_map["data"]:
            if k["value"] not in [None, ""]:
                self.mapped_from_values[k["folio_field"]] = k["value"]
        logging.info(
            f"Mapped values:\n{json.dumps(self.mapped_from_values, indent=4, sort_keys=True)}"
        )
        legacy_fields = set()
        self.mapped_from_legacy_data = {}
        for k in self.record_map["data"]:
            if (
                k["legacy_field"] not in self.empty_vals
                or k["value"] not in self.empty_vals
            ):
                legacy_fields.add(k["legacy_field"])
                if not self.mapped_from_legacy_data.get(k["folio_field"]):
                    self.mapped_from_legacy_data[k["folio_field"]] = {k["legacy_field"]}
                else:
                    self.mapped_from_legacy_data[k["folio_field"]].add(
                        k["legacy_field"]
                    )

        logging.info(
            f"Mapped legacy fields:\n{json.dumps(list(legacy_fields), indent=4, sort_keys=True)}"
        )
        logging.info(
            f"Mapped FOLIO fields:\n{json.dumps(self.folio_keys, indent=4, sort_keys=True)}"
        )
        csv.register_dialect("tsv", delimiter="\t")

    def report_folio_mapping(self, folio_object):
        flat_object = flatten(folio_object)
        for field_name, value in flat_object.items():
            v = 1 if value else 0
            if field_name not in self.mapped_folio_fields:
                self.mapped_folio_fields[field_name] = v
            else:
                self.mapped_folio_fields[field_name] += v

    def validate_map(self):
        # TODO: Add functionality here to validate that the map is complete.
        # That it maps the required fields etc
        return True

    def write_migration_report(self, report_file):
        """Writes the migrstion report, including section headers, section blurbs, and values."""
        report_file.write(f"{Blurbs.Introduction[1]}\n")

        for a in self.migration_report:
            blurb = a.get("blurb_tuple")
            report_file.write("   \n")
            report_file.write(f"## {blurb[0]}    \n")
            report_file.write(f"{blurb[1]}    \n")
            report_file.write(
                f"<details><summary>Click to expand all {len(self.migration_report[a])} things</summary>     \n"
            )
            report_file.write(f"   \n")
            report_file.write("Measure | Count   \n")
            report_file.write("--- | ---:   \n")
            b = self.migration_report[a]
            sortedlist = [
                (k, b[k]) for k in sorted(b, key=as_str) if k != "blurb_tuple"
            ]
            for b in sortedlist:
                report_file.write(f"{b[0]} | {b[1]}   \n")
            report_file.write("</details>   \n")

    def handle_transformation_process_error(
        self, idx, process_error: TransformationProcessError
    ):
        self.add_general_statistics("Critical process error")
        logging.critical(f"{idx}\t{process_error}")
        exit()

    def handle_transformation_critical_error(
        self, idx, data_error: TransformationRecordFailedError
    ):
        self.add_to_migration_report(
            Blurbs.GeneralStatistics, "Records failed due to a data error"
        )
        logging.error(f"{idx}\t{data_error}")
        self.num_criticalerrors += 1
        if self.num_criticalerrors / (idx + 1) > 0.2 and self.num_criticalerrors > 5000:
            logging.fatal(
                f"Stopping. More than {self.num_criticalerrors} critical data errors"
            )
            logging.error(
                f"Errors: {self.num_criticalerrors}\terrors/records:"
                f"{self.num_criticalerrors / (idx + 1)}"
            )
            exit()

    def handle_generic_exception(self, idx, excepion: Exception):
        self.num_exeptions += 1
        print("\n=======ERROR===========")
        print(
            f"Row {idx:,} failed with the following unhandled Exception: {excepion}  "
            f"of type {type(excepion).__name__}"
        )
        if self.num_exeptions > 500:
            logging.fatal(
                f"Stopping. More than {self.num_exeptions} unhandled exceptions"
            )
            exit()

    @staticmethod
    def get_mapped_folio_properties_from_map(map):
        return [
            k["folio_field"]
            for k in map["data"]
            if (
                k["legacy_field"] not in empty_vals
                or k.get("value", "") not in empty_vals
            )
        ]

    def print_mapping_report(self, report_file, total_records):

        logging.info("Writing mapping report")
        report_file.write("\n## Mapped FOLIO fields   \n")
        d_sorted = {
            k: self.mapped_folio_fields[k] for k in sorted(self.mapped_folio_fields)
        }
        report_file.write("FOLIO Field | Mapped | Empty | Unmapped  \n")
        report_file.write("--- | --- | --- | ---:  \n")
        for k, v in d_sorted.items():
            unmapped = total_records - v[0]
            mapped = v[0] - v[1]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            report_file.write(
                f"{k} | {mapped if mapped > 0 else 0} ({mapped_per}) | {v[1]} | {unmapped}  \n"
            )

        # Legacy fields (like marc)
        report_file.write("\n## Mapped Legacy fields  \n")
        d_sorted = {
            k: self.mapped_legacy_fields[k] for k in sorted(self.mapped_legacy_fields)
        }
        report_file.write("Legacy Field | Present | Mapped | Empty | Unmapped  \n")
        report_file.write("--- | --- | --- | --- | ---:  \n")
        for k, v in d_sorted.items():
            present = v[0]
            present_per = "{:.1%}".format(
                present / total_records if total_records else 0
            )
            unmapped = present - v[1]
            mapped = v[1]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(max(mp, 0))
            report_file.write(
                f"{k} | {present if present > 0 else 0} ({present_per}) | {mapped if mapped > 0 else 0} ({mapped_per}) | {v[1]} | {unmapped}  \n"
            )

    def report_legacy_mapping(self, field_name, was_mapped, was_empty=False):
        if field_name not in self.mapped_legacy_fields:
            self.mapped_legacy_fields[field_name] = [int(was_mapped), int(was_empty)]
        else:
            self.mapped_legacy_fields[field_name][0] += int(was_mapped)
            self.mapped_legacy_fields[field_name][1] += int(was_empty)

    def instantiate_record(self):
        return {
            "metadata": self.folio_client.get_metadata_construct(),
            "id": str(uuid.uuid4()),
            "type": "object",
        }

    def add_stats(self, a, number=1):
        # TODO: Move to interface or parent class
        if a not in self.stats:
            self.stats[a] = number
        else:
            self.stats[a] += number

    @staticmethod
    def print_dict_to_md_table(my_dict, h1="", h2=""):
        d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
        print(f"{h1} | {h2}")
        print("--- | ---:")
        for k, v in d_sorted.items():
            print(f"{k} | {v}")

    def get_mapped_value(
        self, ref_dat_mapping: RefDataMapping, legacy_object, prevent_default=False
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
            logging.debug(f"Found mapping is {right_mapping}")
            self.add_to_migration_report(
                Blurbs.ReferenceDataMapping,
                f'{ref_dat_mapping.name} mapping - {" - ".join(fieldvalues)} -> {right_mapping[f"folio_{ref_dat_mapping.key_type}"]}',
            )
            return right_mapping["folio_id"]
        except StopIteration:
            logging.debug(f"{ref_dat_mapping.name} mapping stopiteration")
            if prevent_default:
                self.add_to_migration_report(
                    Blurbs.ReferenceDataMapping,
                    f'{ref_dat_mapping.name} mapping - Not to be mapped. (No default) -- {" - ".join(fieldvalues)} -> ""',
                )
                return ""
            self.add_to_migration_report(
                Blurbs.ReferenceDataMapping,
                f'{ref_dat_mapping.name} mapping - Unmapped -- {" - ".join(fieldvalues)} -> {ref_dat_mapping.default_name}',
            )
            return ref_dat_mapping.default_id
        except IndexError as ee:
            logging.debug(f"{ref_dat_mapping.name} mapping indexerror")
            raise TransformationRecordFailedError(
                "",
                f"{ref_dat_mapping.name} - folio_{ref_dat_mapping.key_type} ({ref_dat_mapping.mapped_legacy_keys}) {ee} is not a recognized field in the legacy data.",
            )

        except Exception as ee:
            logging.debug(f"{ref_dat_mapping.name} mapping general error")
            raise TransformationRecordFailedError(
                "",
                f"{ref_dat_mapping.name} - folio_{ref_dat_mapping.key_type} ({ref_dat_mapping.mapped_legacy_keys}) {ee}",
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

    def add_to_migration_report(self, blurb_tuple: tuple, measure_to_add):
        """Add section header and values to migration report."""
        if blurb_tuple[0] not in self.migration_report:
            self.migration_report[blurb_tuple[0]] = {"blurb_tuple": blurb_tuple}
        if measure_to_add not in self.migration_report[blurb_tuple[0]]:
            self.migration_report[blurb_tuple[0]][measure_to_add] = 1
        else:
            self.migration_report[blurb_tuple[0]][measure_to_add] += 1

    def add_general_statistics(self, measure_to_add: str):
        self.add_to_migration_report(Blurbs.GeneralStatistics, measure_to_add)

    def set_to_migration_report(self, header: str, measure_to_add: str, number: int):
        if header not in self.migration_report:
            self.migration_report[header] = {}
        self.migration_report[header][measure_to_add] = number

    @abstractmethod
    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        raise NotImplementedError(
            "This method needs to be implemented in a implementing class"
        )

    def do_map(self, legacy_object, index_or_id: str):
        folio_object = self.instantiate_record()
        for property_name_level1, property_level1 in self.schema["properties"].items():
            try:
                self.map_level1_property(
                    property_name_level1,
                    property_level1,
                    folio_object,
                    index_or_id,
                    legacy_object,
                )
            except TransformationFieldMappingError as data_error:
                self.add_to_migration_report(Blurbs.FieldMappingErrors, data_error)
                self.add_stats("Data issues found")
                logging.error(data_error)
        self.validate_object(folio_object, index_or_id)
        return folio_object

    def map_level1_property(
        self,
        property_name_level1,
        property_level1,
        folio_object,
        index_or_id,
        legacy_object,
    ):
        if property_level1.get("description", "") == "Deprecated" or skip_property(
            property_name_level1, property_level1
        ):
            self.add_to_migration_report(
                Blurbs.PropertiesExcludedFromSchema,
                property_name_level1,
            )
        elif property_level1["type"] == "object":
            if "properties" in property_level1:
                self.map_object_props(
                    legacy_object,
                    property_name_level1,
                    property_level1,
                    folio_object,
                    index_or_id,
                )
        elif property_level1["type"] == "array":
            if property_level1["items"]["type"] == "object":
                self.map_objects_array_props(
                    legacy_object,
                    property_name_level1,
                    property_level1["items"]["properties"],
                    folio_object,
                    index_or_id,
                )
            elif property_level1["items"]["type"] == "string":
                self.map_string_array_props(
                    legacy_object,
                    property_name_level1,
                    folio_object,
                    index_or_id,
                )
            else:
                logging.debug(f"")
        else:  # Basic property
            self.map_basic_props(
                legacy_object, property_name_level1, folio_object, index_or_id
            )

    def validate_object(self, folio_object, index_or_id):
        required = self.schema["required"]
        missing = []
        for required_prop in required:
            if required_prop not in folio_object:
                if index_or_id == "row 1":
                    logging.info(json.dumps(folio_object, indent=4))
                missing.append(f"Missing: {required_prop}")
            elif not folio_object[required_prop]:
                if index_or_id == "row 1":
                    logging.info(json.dumps(folio_object, indent=4))
                missing.append(f"Empty: {required_prop}")
        if any(missing):
            raise TransformationRecordFailedError(
                f"Required properties empty for {index_or_id}\t{json.dumps(missing)}"
            )

        del folio_object["type"]

    @staticmethod
    def get_legacy_vals(legacy_item, legacy_item_keys):
        return {
            legacy_item[k]
            for k in legacy_item_keys
            if legacy_item.get(k, "") not in ["", None]
        }

    def map_object_props(
        self,
        legacy_object,
        property_name_level1,
        property_level1,
        folio_object,
        index_or_id,
    ):
        temp_object = {}
        prop_key = property_name_level1
        for property_name_level2, property_level2 in property_level1[
            "properties"
        ].items():
            sub_prop_key = prop_key + "." + property_name_level2
            if "properties" in property_level2:
                for property_name_level3, property_level3 in property_level2[
                    "properties"
                ].items():
                    # not parsing stuff on level three.
                    pass
            elif property_level2["type"] == "array":
                # not parsing arrays on level 2
                pass
                """
                # Object with subprop array
                temp_object[property_name_level2] = []
                for i in range(5):
                    prop_path = f"{sub_prop_key}.{sub_prop_name2}[{i}]"
                    if property_level2["items"]["type"] == "object":
                        # Array of objects
                        temp = {
                            sub_prop_name2: self.get_prop(
                                folio_object,
                                ,
                                index_or_id,
                            )
                            for sub_prop_name2, sub_prop2 in property_level2["items"][
                                "properties"
                            ].items()
                        }
                        if not all(value for key, value in temp.items()):
                            continue
                        temp_object[property_name_level2].append(temp)
                    else:
                        
                        mkey = sub_prop_key + "." + sub_prop_name2
                        a = self.get_prop(legacy_object, mkey, index_or_id, i)
                        if a:
                            temp_object[property_name_level2] = a"""
            else:
                p = self.get_prop(legacy_object, sub_prop_key, index_or_id)
                if p:
                    temp_object[property_name_level2] = p
        if temp_object:
            folio_object[property_name_level1] = temp_object

    def map_objects_array_props(
        self, legacy_object, prop_name, properties, folio_object, index_or_id
    ):
        resulting_array = []
        for i in range(9):
            temp_object = {}
            for prop in (
                k for k, p in properties.items() if not p.get("folio:isVirtual", False)
            ):
                prop_path = f"{prop_name}[{i}].{prop}"
                # logging.debug(f"object array prop_path {prop_path}")
                if prop_path in self.folio_keys:
                    res = self.get_prop(legacy_object, prop_path, index_or_id)
                    self.report_legacy_mapping(
                        self.legacy_basic_property(prop), True, res in ["", None, []]
                    )
                    temp_object[prop] = res

            if temp_object != {} and all(
                (v or (isinstance(v, bool)) for k, v in temp_object.items())
            ):
                # logging.debug(f"temporary object {temp_object}")
                resulting_array.append(temp_object)
            # else:
            #    logging..trace(f"empty temp object {json.dumps(temp_object, indent=4)}")
        if any(resulting_array):
            folio_object[prop_name] = resulting_array

    def map_string_array_props(self, legacy_object, prop, folio_object, index_or_id):
        logging.debug(f"String array {prop}")
        for i in range(9):
            prop_name = f"{prop}[{i}]"
            if prop_name in self.folio_keys and self.has_property(
                legacy_object, prop_name
            ):
                mapped_prop = self.get_prop(legacy_object, prop_name, index_or_id)
                if mapped_prop:
                    # logging.debug(f"Mapped string array prop {mapped_prop}")
                    if prop in folio_object and mapped_prop not in folio_object.get(
                        prop, []
                    ):
                        folio_object.get(prop, []).append(mapped_prop)
                    else:
                        folio_object[prop] = [mapped_prop]
                    # logging.debug(f"Mapped string array prop {folio_object[prop]}")
                    self.report_legacy_mapping(
                        self.legacy_basic_property(prop_name), True, False
                    )
                else:  # Match but empty field. Lets report this
                    self.report_legacy_mapping(
                        self.legacy_basic_property(prop_name), True, True
                    )

    def map_basic_props(self, legacy_object, prop, folio_object, index_or_id):
        if self.has_basic_property(legacy_object, prop):  # is there a match in the csv?
            mapped_prop = self.get_prop(legacy_object, prop, index_or_id)
            if mapped_prop:
                folio_object[prop] = mapped_prop
                self.report_legacy_mapping(
                    self.legacy_basic_property(prop), True, False
                )
            else:  # Match but empty field. Lets report this
                self.report_legacy_mapping(self.legacy_basic_property(prop), True, True)

    def get_objects(self, source_file, file_name: str):
        if file_name.endswith("tsv"):
            reader = csv.DictReader(source_file, dialect="tsv")
        else:
            reader = csv.DictReader(source_file)
        idx = 0
        try:
            for idx, row in enumerate(reader):
                yield row
        except Exception as ee:
            logging.error(f"{ee} at row {idx}")
            raise ee

    def has_property(self, legacy_object, folio_prop_name: str):
        if not self.use_map:
            return folio_prop_name in legacy_object

        legacy_keys = self.field_map.get(folio_prop_name, [])
        # logging.debug(f"{folio_prop_name} - {legacy_key}")
        return (
            any(legacy_keys)
            and any(k not in empty_vals for k in legacy_keys)
            and any(legacy_object.get(legacy_key, "") for legacy_key in legacy_keys)
        )

    def has_basic_property(self, legacy_object, folio_prop_name):
        if not self.use_map:
            return folio_prop_name in legacy_object

        if folio_prop_name not in self.folio_keys:
            # logging.debug(f"map_basic_props -> {folio_prop_name}")
            return False
        legacy_keys = self.field_map.get(folio_prop_name, [])
        # logging.debug(f"{folio_prop_name} - {legacy_key}")
        return (
            any(legacy_keys)
            and any(k not in empty_vals for k in legacy_keys)
            and any(legacy_object.get(legacy_key, "") for legacy_key in legacy_keys)
        )

    def legacy_basic_property(self, folio_prop):
        if not self.use_map:
            return folio_prop
        if folio_prop not in self.folio_keys:
            return ""
        return next(
            (
                k["legacy_field"]
                for k in self.record_map["data"]
                if k["folio_field"] == folio_prop
            ),
            "",
        )

    def get_ref_data_tuple_by_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        ref_object = self.ref_data_dicts.get(dict_key, {}).get(
            key_value.lower().strip(), ()
        )
        if ref_object:
            return ref_object
        d = {r[key_type].lower(): (r["id"], r["name"]) for r in ref_data}
        self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower().strip(), ())


def skip_property(property_name_level1, property_level1):
    return bool(
        property_name_level1 in ["metadata", "id", "type", "lastCheckIn"]
        or property_name_level1.startswith("effective")
        or property_level1.get("folio:isVirtual", False)
    )


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s


def weird_division(n, d):
    return n / d if d else 0


def flatten(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
