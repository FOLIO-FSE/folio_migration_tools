import csv
import json
import logging
from marc_to_folio.custom_exceptions import (
    TransformationCriticalDataError,
    TransformationDataError,
    TransformationProcessError,
)
import uuid
from abc import abstractmethod
import requests
import re
from folioclient import FolioClient
import os


class MapperBase:
    def __init__(self, folio_client: FolioClient, schema, record_map, error_file):
        self.schema = schema
        self.stats = {}
        self.migration_report = {}
        self.folio_client = folio_client
        self.mapped_folio_fields = {}
        self.mapped_legacy_fields = {}
        self.use_map = True  # Legacy
        self.record_map = record_map
        self.error_file = error_file
        self.ref_data_dicts = {}
        self.arr_re = r"\[[0-9]\]$"
        self.folio_keys = list(
            # re.sub(self.arr_re, ".", k["folio_field"]).strip(".")
            k["folio_field"]
            for k in self.record_map["data"]
            if k["legacy_field"] not in ["", "Not mapped"]
        )
        print("Mapped FOLIO Fields")
        print(json.dumps(self.folio_keys, indent=4, sort_keys=True))
        csv.register_dialect("tsv", delimiter="\t")

    def write_migration_report(self, report_file):
        logging.info("Writing migration report")
        for a in self.migration_report:
            report_file.write(f"   \n")
            report_file.write(f"## {a}    \n")
            report_file.write(
                f"<details><summary>Click to expand all {len(self.migration_report[a])} things</summary>     \n"
            )
            report_file.write(f"   \n")
            report_file.write(f"Measure | Count   \n")
            report_file.write(f"--- | ---:   \n")
            b = self.migration_report[a]
            sortedlist = [(k, b[k]) for k in sorted(b, key=as_str)]
            for b in sortedlist:
                report_file.write(f"{b[0]} | {b[1]}   \n")
            report_file.write("</details>   \n")

    def print_mapping_report(self, report_file, total_records):

        logging.info("Writing mapping report")
        report_file.write("\n## Mapped FOLIO fields   \n")
        d_sorted = {
            k: self.mapped_folio_fields[k] for k in sorted(self.mapped_folio_fields)
        }
        report_file.write(f"FOLIO Field | Mapped | Empty | Unmapped  \n")
        report_file.write("--- | --- | --- | ---:  \n")
        for k, v in d_sorted.items():
            unmapped = total_records - v[0]
            mapped = v[0] - v[1]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
            report_file.write(
                f"{k} | {mapped if mapped > 0 else 0} ({mapped_per}) | {v[1]} | {unmapped}  \n"
            )

        # Legacy fields (like marc)
        report_file.write("\n## Mapped Legacy fields  \n")
        d_sorted = {
            k: self.mapped_legacy_fields[k] for k in sorted(self.mapped_legacy_fields)
        }
        report_file.write(f"Legacy Field | Present | Mapped | Empty | Unmapped  \n")
        report_file.write("--- | --- | --- | --- | ---:  \n")
        for k, v in d_sorted.items():
            present = v[0]
            present_per = "{:.1%}".format(
                present / total_records if total_records else 0
            )
            unmapped = present - v[1]
            mapped = v[1]
            mp = mapped / total_records if total_records else 0
            mapped_per = "{:.0%}".format(mp if mp > 0 else 0)
            report_file.write(
                f"{k} | {present if present > 0 else 0} ({present_per}) | {mapped if mapped > 0 else 0} ({mapped_per}) | {v[1]} | {unmapped}  \n"
            )

    def report_legacy_mapping(self, field_name, was_mapped, was_empty=False):
        if field_name not in self.mapped_legacy_fields:
            self.mapped_legacy_fields[field_name] = [int(was_mapped), int(was_empty)]
        else:
            self.mapped_legacy_fields[field_name][0] += int(was_mapped)
            self.mapped_legacy_fields[field_name][1] += int(was_empty)

    def report_folio_mapping(self, field_name, transformed, was_empty=False):
        if field_name not in self.mapped_folio_fields:
            self.mapped_folio_fields[field_name] = [int(transformed), int(was_empty)]
        else:
            self.mapped_folio_fields[field_name][0] += int(transformed)
            self.mapped_folio_fields[field_name][1] += int(was_empty)

    def instantiate_record(self):
        record = {
            "metadata": self.folio_client.get_metadata_construct(),
            "id": str(uuid.uuid4()),
            "type": "object",
        }
        self.report_folio_mapping("id", True)
        self.report_folio_mapping("metadata", True)
        return record

    def add_stats(self, a):
        # TODO: Move to interface or parent class
        if a not in self.stats:
            self.stats[a] = 1
        else:
            self.stats[a] += 1

    def validate(self, folio_record, legacy_id, required_fields):
        failures = []
        for req in required_fields:
            if req not in folio_record:
                failures.append(req)
                self.add_to_migration_report(
                    "Failed records that needs to get fixed",
                    f"Required field {req} is missing from {legacy_id}",
                )
        if len(failures) > 0:
            self.add_to_migration_report("User validation", "Total failed users")
            for failure in failures:
                self.add_to_migration_report("Record validation", f"{failure}")
            raise ValueError(f"Record {legacy_id} failed validation {failures}")

    @staticmethod
    def print_dict_to_md_table(my_dict, h1="", h2=""):
        d_sorted = {k: my_dict[k] for k in sorted(my_dict)}
        print(f"{h1} | {h2}")
        print("--- | ---:")
        for k, v in d_sorted.items():
            print(f"{k} | {v}")

    def setup_location_mappings(self, location_map):
        # Locations
        logging.info("Fetching locations...")
        for idx, loc_map in enumerate(location_map):
            if idx == 1:
                self.location_keys = list(
                    [
                        k
                        for k in loc_map.keys()
                        if k
                        not in ["folio_code", "folio_id", "folio_name", "legacy_code"]
                    ]
                )
            if any(m for m in loc_map.values() if m == "*"):
                t = self.get_ref_data_tuple_by_code(
                    self.folio_client.locations, "locations", loc_map["folio_code"]
                )
                if t:
                    self.default_location_id = t[0]
                    logging.info(f'Set {loc_map["folio_code"]} as default location')
                else:
                    raise TransformationProcessError(
                        f"Default location {loc_map['folio_code']} not found in folio. "
                        "Change default code"
                    )
            else:
                t = self.get_ref_data_tuple_by_code(
                    self.folio_client.locations, "locations", loc_map["folio_code"]
                )
                if t:
                    loc_map["folio_id"] = t[0]
                else:
                    raise TransformationProcessError(
                        f"Location code {loc_map['folio_code']} from map not found in FOLIO"
                    )

        if not self.default_location_id:
            raise TransformationProcessError(
                "No Default Location set up in map. "
                "Add a row to mapping file with *:s and a valid Location code"
            )
        logging.info(
            f"loaded {idx} mappings for {len(self.folio_client.locations)} locations in FOLIO"
        )

    def get_mapped_value(
        self, name_of_mapping, legacy_item, legacy_keys, map, default_value, map_key
    ):
        # Gets mapped value from mapping file, translated to the right FOLIO UUID

        try:
            fieldvalues = [legacy_item.get(k) for k in legacy_keys]
            right_mapping = next(
                mapping
                for mapping in map
                if all(
                    legacy_item[k].strip().casefold() in mapping[k].casefold()
                    for k in legacy_keys
                )
            )
            self.add_to_migration_report(
                f"{name_of_mapping} mapping",
                f'{" - ".join(fieldvalues)} -> {right_mapping[map_key]}',
            )
            return right_mapping["folio_id"]
        except StopIteration:
            self.add_to_migration_report(
                f"{name_of_mapping} mapping",
                f'{" - ".join(fieldvalues)} -> {default_value} (Unmapped)',
            )
            return default_value
        except Exception as ee:
            raise TransformationCriticalDataError(
                f"{name_of_mapping} - {map_key} ({legacy_keys}) {ee}"
            )

    def add_to_migration_report(self, header, measure_to_add):
        if header not in self.migration_report:
            self.migration_report[header] = {}
        if measure_to_add not in self.migration_report[header]:
            self.migration_report[header][measure_to_add] = 1
        else:
            self.migration_report[header][measure_to_add] += 1

    @abstractmethod
    def get_prop(self, legacy_item, folio_prop_name, index_or_id, i=0):
        raise NotImplementedError(
            "This method needs to be implemented in a implementing class"
        )

    def do_map(self, legacy_object, index_or_id):
        folio_object = self.instantiate_record()
        required = self.schema["required"]
        for prop_name, prop in self.schema["properties"].items():
            try:
                if prop.get("description", "") == "Deprecated":
                    self.report_folio_mapping(f"{prop_name} (deprecated)", False, True)
                elif (
                    prop_name in ["metadata", "id", "type"]
                    or prop_name.startswith("effective")
                    or prop.get("folio:isVirtual", False)
                ):
                    self.report_folio_mapping(
                        f"{prop_name} (Not to be mapped)", False, True
                    )
                elif prop["type"] == "object":
                    temp_object = {}
                    prop_key = prop_name
                    if "properties" in prop:
                        for sub_prop_name, sub_prop in prop["properties"].items():
                            sub_prop_key = prop_key + "." + sub_prop_name
                            if "properties" in sub_prop:
                                for sub_prop_name2, sub_prop2 in sub_prop[
                                    "properties"
                                ].items():
                                    sub_prop_key2 = sub_prop_key + "." + sub_prop_name2
                                    if sub_prop2["type"] == "array":
                                        logging.debug(f"Array: {sub_prop_key2} ")
                            elif (
                                sub_prop["type"] == "array"
                            ):  # Object with subprop array
                                temp_object[sub_prop_name] = []
                                logging.debug(f"Array Sub prop name: {sub_prop_name}")
                                for i in range(0, 5):
                                    if sub_prop["items"]["type"] == "object":
                                        temp = {}
                                        for sub_prop_name2, sub_prop2 in sub_prop[
                                            "items"
                                        ]["properties"].items():
                                            temp[sub_prop_name2] = self.get_prop(
                                                folio_object,
                                                sub_prop_key + "." + sub_prop_name2,
                                                index_or_id,
                                                i,
                                            )
                                        if not all(
                                            value for key, value in temp.items()
                                        ):
                                            self.add_to_migration_report(
                                                "Skipped props since empty",
                                                f"{prop_name}.{sub_prop_name}",
                                            )
                                            continue
                                        temp_object[sub_prop_name].append(temp)
                                    else:
                                        mkey = sub_prop_key + "." + sub_prop_name2
                                        a = self.get_prop(
                                            legacy_object, mkey, index_or_id, i
                                        )
                                        if a:
                                            temp_object[sub_prop_name] = a
                            else:
                                p = self.get_prop(
                                    legacy_object, sub_prop_key, index_or_id
                                )
                                if p:
                                    temp_object[sub_prop_name] = p
                        if temp_object:
                            folio_object[prop_name] = temp_object
                elif prop["type"] == "array":
                    if prop["items"]["type"] == "object":
                        self.map_objects_array_props(
                            legacy_object,
                            prop_name,
                            prop["items"]["properties"],
                            folio_object,
                            index_or_id,
                        )
                    elif prop["items"]["type"] == "string":
                        self.map_string_array_props(
                            legacy_object, prop_name, folio_object, index_or_id
                        )
                    else:
                        self.report_folio_mapping(
                            f'Unhandled array of {prop["items"]["type"]}: {prop_name}',
                            False,
                        )
                else:  # Basic property
                    self.map_basic_props(
                        legacy_object, prop_name, folio_object, index_or_id
                    )
            except TransformationDataError as data_error:
                self.add_stats("Data issues found")
                self.error_file.write(data_error)
        for required_prop in required:
            if required_prop not in folio_object:
                raise TransformationCriticalDataError(
                    f"Required property {required_prop} missing for {index_or_id}"
                )
            elif not folio_object[required_prop]:
                raise TransformationCriticalDataError(
                    f"Required property {required_prop} empty for {index_or_id}"
                )
        del folio_object["type"]
        return folio_object

    def map_objects_array_props(
        self, legacy_object, prop_name, properties, folio_object, index_or_id
    ):
        a = []

        for i in range(0, 9):
            temp_object = {}
            for prop in (
                k for k, p in properties.items() if not p.get("folio:isVirtual", False)
            ):

                prop_path = f"{prop_name}[{i}].{prop}"
                # logging.debug(f"object array prop_path {prop_path}")
                if prop_path in self.folio_keys:
                    logging.debug(f"{prop_path} is IN folio_keys")
                    res = self.get_prop(legacy_object, prop_path, index_or_id, i)
                    self.report_legacy_mapping(self.legacy_property(prop), True, True)
                    self.report_folio_mapping(prop_path, True, False)
                    temp_object[prop] = res

            if temp_object != {} and all(
                (v or (isinstance(v, bool) and not v) for k, v in temp_object.items())
            ):
                logging.debug(f"temporary object {temp_object}")
                a.append(temp_object)
        if any(a):
            folio_object[prop_name] = a

    def map_string_array_props(self, legacy_object, prop, folio_object, index_or_id):
        logging.debug(f"String array {prop}")
        if self.has_property(legacy_object, prop):  # is there a match in the csv?
            logging.debug(f"Has string array property! {prop}")
            for i in range(0, 5):
                mapped_prop = self.get_prop(legacy_object, prop, index_or_id, i).strip()
                if mapped_prop:
                    if prop in folio_object:
                        folio_object.get(prop, []).append(mapped_prop)
                    else:
                        folio_object[prop] = [mapped_prop]
                    self.report_legacy_mapping(self.legacy_property(prop), True, False)
                    self.report_folio_mapping(prop, True, False)
                else:  # Match but empty field. Lets report this
                    self.report_legacy_mapping(self.legacy_property(prop), True, True)
                    self.report_folio_mapping(prop, True, True)
        else:
            self.report_folio_mapping(prop, False)

    def map_basic_props(self, legacy_object, prop, folio_object, index_or_id):
        if self.has_basic_property(legacy_object, prop):  # is there a match in the csv?
            mapped_prop = self.get_prop(legacy_object, prop, index_or_id).strip()
            if mapped_prop:
                folio_object[prop] = mapped_prop
                self.report_legacy_mapping(
                    self.legacy_basic_property(prop), True, False
                )
                self.report_folio_mapping(prop, True, False)
            else:  # Match but empty field. Lets report this
                self.report_legacy_mapping(self.legacy_property(prop), True, True)
                self.report_folio_mapping(prop, True, True)
        else:
            self.report_folio_mapping(prop, False)

    def get_objects(self, source_file, file_name: str):
        if file_name.endswith("tsv"):
            reader = csv.DictReader(source_file, dialect="tsv")
        else:
            reader = csv.DictReader(source_file)
        for row in reader:
            yield row

    def has_property(self, legacy_object, folio_prop_name: str):
        if self.use_map:
            # if folio_prop_name not in self.folio_keys:
            #     return False
            legacy_key = next(
                (
                    k["legacy_field"]
                    for k in self.record_map["data"]
                    if k["folio_field"] == folio_prop_name
                    or re.sub(self.arr_re, ".", k["folio_field"]).strip(".")
                    == folio_prop_name
                ),
                "",
            )
            logging.debug(f"{folio_prop_name} - {legacy_key}")
            b = (
                legacy_key
                and legacy_key not in ["", "Not mapped"]
                and legacy_object.get(legacy_key, "")
            )
            return b
        else:
            return folio_prop_name in legacy_object

    def has_basic_property(self, legacy_object, folio_prop_name):
        if self.use_map:
            if folio_prop_name not in self.folio_keys:
                logging.debug(f"map_basic_props -> {folio_prop_name}")
                return False
            legacy_key = next(
                (
                    k["legacy_field"]
                    for k in self.record_map["data"]
                    if k["folio_field"] == folio_prop_name
                ),
                "",
            )
            logging.debug(f"{folio_prop_name} - {legacy_key}")
            b = (
                legacy_key
                and legacy_key not in ["", "Not mapped"]
                and legacy_object.get(legacy_key, "")
            )
            return b
        else:
            return folio_prop_name in legacy_object

    def legacy_property(self, folio_prop):
        if self.use_map:
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
        else:
            return folio_prop

    def legacy_basic_property(self, folio_prop):
        if self.use_map:
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
        else:
            return folio_prop

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
        else:
            d = {}
            for r in ref_data:
                d[r[key_type].lower()] = (r["id"], r["name"])
            self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower().strip(), ())


def as_str(s):
    try:
        return str(s), ""
    except ValueError:
        return "", s


def weird_division(n, d):
    return n / d if d else 0
