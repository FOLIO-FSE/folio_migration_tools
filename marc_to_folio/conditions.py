import logging
import re


class Conditions:
    def __init__(self, folio, mapper):
        self.filter_chars = r"[.,\/#!$%\^&\*;:{}=\-_`~()]"
        self.stats = {}
        self.filter_chars_dop = r"[.,\/#!$%\^&\*;:{}=\_`~()]"
        self.filter_last_chars = r",$"
        self.folio = folio
        self.electronic_access_relationships = {}
        self.mapper = mapper
        self.cache = {}
        self.electronic_access_relationships = list(
            self.folio.folio_get_all(
                "/electronic-access-relationships",
                "electronicAccessRelationships",
                "?query=cql.allRecords=1 sortby name",
            )
        )
        print(
            f"Fetched {len(self.electronic_access_relationships)} electronic_access_relationships"
        )
        self.ref_data_dicts = {}
        self.holding_note_types = list(
            self.folio.folio_get_all(
                "/holdings-note-types",
                "holdingsNoteTypes",
                "?query=cql.allRecords=1 sortby name",
            )
        )
        print(f"Fetched {len(self.holding_note_types)} holding_note_types")

        self.call_number_types = list(
            self.folio.folio_get_all(
                "/call-number-types",
                "callNumberTypes",
                "?query=cql.allRecords=1 sortby name",
            )
        )
        print(f"Fetched {len(self.call_number_types)} call_number_types")

        self.locations = list(self.folio.folio_get_all("/locations", "locations",))
        print(f"Fetched {len(self.locations)} locations")

    def get_condition(self, name, value, parameter=None, marc_field=None):
        try:
            if not self.cache.get(name, ""):
                self.cache[name] = getattr(self, "condition_" + str(name))
            return self.cache[name](value, parameter, marc_field)

        except AttributeError as attrib_error:
            self.mapper.add_to_migration_report(
                "Undhandled condition defined in mapping rules", name
            )
            return ""

    def condition_trim_period(self, value, parameter, marc_field):
        return value.strip().rstrip(".").rstrip(",")

    def condition_trim(self, value, parameter, marc_field):
        return value.strip()

    def condition_remove_ending_punc(self, value, parameter, marc_field):
        v = value
        chars = ".;:,/+=- "
        while any(v) > 0 and v[-1] in chars:
            v = v.rstrip(v[-1])
        return v

    def condition_set_instance_format_id(self, value, parameter, marc_field):
        # This method only handles the simple case of 2-character codes of RDA in the first 338$b
        # Other cases are handled in performAddidtionalParsing in the mapper class
        format = next(
            (f for f in self.folio.instance_formats if f["code"] == value), None,
        )
        if format:
            self.mapper.add_to_migration_report(
                "Instance formats", f"{format['name']} set by mapping rules"
            )
            return format["id"]
        else:
            self.mapper.add_to_migration_report(
                "Instance formats", f"338$b value {value} not found in FOLIO"
            )
            return ""

    def condition_remove_prefix_by_indicator(self, value, parameter, marc_field):
        """Returns the index title according to the rules"""
        ind2 = marc_field.indicator2
        reg_str = r"[\s:\/]{0,3}$"
        if ind2 in map(str, range(1, 9)):
            num_take = int(ind2)
            return re.sub(reg_str, "", value[num_take:])
        else:
            return re.sub(reg_str, "", value)

    def condition_capitalize(self, value, parameter, marc_field):
        return value.capitalize()

    def condition_clean_isbn(self, value, parameter, marc_field):
        return value

    def condition_set_publisher_role(self, value, parameter, marc_field):
        roles = {
            "0": "Production",
            "1": "Publication",
            "2": "Distribution",
            "3": "Manufacture",
        }

        return roles.get(marc_field.indicator2, "")

    def condition_set_identifier_type_id_by_value(self, value, parameter, marc_field):
        if not self.folio.identifier_types:
            raise ValueError("No identifier_types setup in tenant")
        if "oclc_regex" in parameter:
            if re.match(parameter["oclc_regex"], value):
                t = self.get_ref_data_tuple_by_name(
                    self.folio.identifier_types,
                    "identifier_types",
                    parameter["names"][1],
                )
                self.mapper.add_to_migration_report("Mapped identifier types", t[1])
                return t[0]
            else:
                t = self.get_ref_data_tuple_by_name(
                    self.folio.identifier_types,
                    "identifier_types",
                    parameter["names"][0],
                )
                self.mapper.add_to_migration_report("Mapped identifier types", t[1])
                return t[0]
        identifier_type = next(
            (f for f in self.folio.identifier_types if f["name"] in parameter["names"]),
            None,
        )
        self.mapper.add_to_migration_report(
            "Mapped identifier types", identifier_type["name"]
        )
        my_id = identifier_type["id"]
        if not my_id:
            raise Exception(
                f"no matching identifier_types in {parameter['names']} {marc_field}"
            )
        if not validate_uuid(my_id):
            raise Exception(
                f"UUID validation failed for {my_id} identifier_types in {parameter['names']} {marc_field}"
            )
        return my_id

    def condition_set_holding_note_type_id_by_name(self, value, parameter, marc_field):
        t = self.get_ref_data_tuple_by_name(
            self.holding_note_types, "holding_note_types", parameter["name"]
        )
        self.mapper.add_to_migration_report("Mapped note types", t[1])
        return t[0]

    def condition_set_classification_type_id(self, value, parameter, marc_field):
        # undef = next((f['id'] for f in self.folio.class_types
        #             if f['name'] == 'No type specified'), '')
        if not self.folio.class_types:
            raise ValueError("No class_types setup in tenant")
        return get_ref_data_tuple_by_name(self.folio.class_types, parameter["name"])[0]

    def condition_char_select(self, value, parameter, marc_field):
        return value[parameter["from"] : parameter["to"]]

    def condition_set_identifier_type_id_by_name(self, value, parameter, marc_field):
        if not self.folio.identifier_types:
            raise ValueError("No identifier_types setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.folio.identifier_types, "identifier_types", parameter["name"]
        )
        self.mapper.add_to_migration_report("Mapped identifier types", t[1])
        return t[0]

    def condition_set_contributor_name_type_id(self, value, parameter, marc_field):
        if not self.folio.contrib_name_types:
            raise ValueError("No contrib_name_types setup in tenant")
        return get_ref_data_tuple_by_name(
            self.folio.contrib_name_types, parameter["name"]
        )

    def condition_set_contributor_type_id(self, value, parameter, marc_field):
        if not self.folio.contributor_types:
            raise ValueError("No contributor_types setup in tenant")
        if not self.default_contributor_type:
            self.default_contributor_type = next(
                ct for ct in self.folio.contributor_types if ct["code"] == "ctb"
            )
        for subfield in marc_field.get_subfields("4", "e"):
            for cont_type in self.folio.contributor_types:
                if subfield == cont_type["code"]:
                    self.mapper.add_to_migration_report(
                        "Mapped contributor types", cont_type["name"]
                    )
                    return cont_type["id"]
                elif subfield == cont_type["name"]:
                    self.mapper.add_to_migration_report(
                        "Mapped contributor types", cont_type["name"]
                    )
                    return cont_type["id"]
        self.mapper.add_to_migration_report(
            "Mapped contributor types", self.default_contributor_type["name"]
        )
        return self.default_contributor_type["id"]

    def condition_set_instance_id_by_map(self, value, parameter, marc_field):
        if value in self.mapper.instance_id_map:
            return self.mapper.instance_id_map[value]["id"]
        else:
            self.mapper.add_stats(self.mapper.stats, "bib id not in map")
            raise ValueError(f"Old instance id not in map: {value} Field: {marc_field}")

    def condition_set_url_relationship(self, value, parameter, marc_field):
        enum = {
            "0": "resource",
            "1": "version of resource",
            "2": "related resource",
            "8": "no information provided",
        }
        ind2 = marc_field.indicator2
        name = enum.get(ind2, enum["8"])
        if not self.electronic_access_relationships:
            raise ValueError("No electronic_access_relationships setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.electronic_access_relationships,
            "electronic_access_relationships",
            name,
        )
        self.mapper.add_to_migration_report(
            "Mapped electronic access relationships types", t[1]
        )
        return t[0]

    def condition_set_contributor_type_text(self, value, parameter, marc_field):
        if not self.folio.contributor_types:
            raise ValueError("No contributor_types setup in tenant")
        if not self.default_contributor_type:
            self.default_contributor_type = next(
                ct for ct in self.folio.contributor_types if ct["code"] == "ctb"
            )
        for subfield in marc_field.get_subfields("4", "e"):
            for cont_type in self.folio.contributor_types:
                if subfield == cont_type["code"]:
                    return cont_type["name"]
                elif subfield == cont_type["name"]:
                    return cont_type["name"]
        return self.default_contributor_type["name"]

    def condition_set_alternative_title_type_id(self, value, parameter, marc_field):
        if not self.folio.alt_title_types:
            raise ValueError("No alt_title_types setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.folio.alt_title_types, "alt_title_types", parameter["name"]
        )
        self.mapper.add_to_migration_report("Mapped Alternative title types", t[1])
        return t[0]

    def condition_set_location_id_by_code(self, value, parameter, marc_field):
        self.mapper.add_to_migration_report("Legacy location codes", value)
        if "legacy_locations" not in self.ref_data_dicts:
            d = {}
            for lm in self.mapper.location_map:
                d[lm["legacy_code"]] = lm["folio_code"]
            self.ref_data_dicts["legacy_locations"] = d

        if self.mapper.location_map and any(self.mapper.location_map):
            mapped_code = self.ref_data_dicts["legacy_locations"].get(value, "")
            if not mapped_code:
                self.mapper.add_to_migration_report(
                    "Locations - Unmapped legacy codes", value
                )
        else:
            mapped_code = value

        t = self.get_ref_data_tuple_code(self.locations, "locations", mapped_code)
        if not t:
            t = self.get_ref_data_tuple_code(
                self.locations, "locations", parameter["unspecifiedLocationCode"]
            )
        self.mapper.add_to_migration_report("Mapped Locations", t[1])
        return t[0]

    def get_ref_data_tuple_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        if dict_key not in self.ref_data_dicts:
            d = {}
            for r in ref_data:
                d[r[key_type].lower()] = (r["id"], r["name"])
            self.ref_data_dicts[dict_key] = d
        ref_object = (
            self.ref_data_dicts[dict_key][key_value.lower()]
            if key_value.lower() in self.ref_data_dicts[dict_key]
            else None
        )
        if not ref_object:
            logging.debug(f"No matching element for {key_value} in {list(ref_data)}")
            return None
        if validate_uuid(ref_object[0]):
            return ref_object
        else:
            raise Exception(f"UUID Validation error for {key_value} in {ref_data}")

    def condition_remove_substring(self, value, parameter, marc_field):
        return value.replace(parameter["substring"], "")

    def condition_set_instance_type_id(self, value, parameter, marc_field):
        if not self.folio.instance_types:
            raise ValueError("No instance_types setup in tenant")
        if marc_field.tag == "008":
            t = self.get_ref_data_tuple_code(
                self.folio.instance_types, "instance_types", value[:3]
            )
            if not t:
                t = self.get_ref_data_tuple_code(
                    self.folio.instance_types, "instance_types", "zzz"
                )
            self.mapper.add_to_migration_report("Mapped Instance types", t[1])
            return t[0]
        elif marc_field.tag == "336" and "b" in marc_field:
            t = self.get_ref_data_tuple_by_name(
                self.folio.instance_types, "instance_types", marc_field["b"]
            )
            if not t:
                t = self.get_ref_data_tuple_code(
                    self.folio.instance_types, "instance_types", "zzz"
                )
            self.mapper.add_to_migration_report("Mapped Instance types", t[1])
            return t[0]
        else:
            # TODO Remove later. Corenell specific
            t = self.get_ref_data_tuple_code(
                self.folio.instance_types, "instance_types", "txt"
            )
            self.mapper.add_to_migration_report("Mapped Instance types", t[1])
            return t[0]
        raise ValueError(
            f"Something went wrong when trying to parse Instance type from {marc_field}"
        )

    def condition_set_electronic_access_relations_id(
        self, value, parameter, marc_field
    ):
        enum = {
            "0": "resource",
            "1": "version of resource",
            "2": "related resource",
            "3": "no information provided",
        }
        ind2 = marc_field.indicator2
        name = enum.get(ind2, enum["3"])

        if not self.electronic_access_relationships:
            raise ValueError("No electronic_access_relationships setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.electronic_access_relationships,
            "electronic_access_relationships",
            name,
        )
        self.mapper.add_to_migration_report(
            "Mapped electronic access relationships types", t[1]
        )
        return t[0]


def validate_uuid(my_uuid):
    # removed for performance reasons
    return True
    """reg = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    pattern = re.compile(reg)
    if pattern.match(my_uuid):
        return True
    else:
        return False"""
