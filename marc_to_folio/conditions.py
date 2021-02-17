import logging
import traceback
import re
import pymarc


class Conditions:
    def __init__(self, folio, mapper):
        self.filter_chars = r"[.,\/#!$%\^&\*;:{}=\-_`~()]"
        self.stats = {}
        self.filter_chars_dop = r"[.,\/#!$%\^&\*;:{}=\_`~()]"
        self.filter_last_chars = r",$"
        self.folio = folio
        self.electronic_access_relationships = {}
        self.default_contributor_type = ""
        self.mapper = mapper
        self.default_call_number_type = {}
        self.condition_cache = {}
        self.holdings_types = list(folio.folio_get_all("/holdings-types", "holdingsTypes"))
        print(f"Fetched {len(self.holdings_types)} holdings types")
        print(
            f"Fetched {len(self.folio.modes_of_issuance)} modes of issuances",
            flush=True,
        )
        print(
            f"Fetched {len(self.folio.identifier_types)} identifier types", flush=True
        )
        print(f"Fetched {len(self.folio.instance_note_types)} note types", flush=True)
        print(f"Fetched {len(self.folio.class_types)} Classification types", flush=True)
        print(
            f"Fetched {len(self.folio.contrib_name_types)} contrib_name_types",
            flush=True,
        )
        print(
            f"Fetched {len(self.folio.contributor_types)} contributor_types", flush=True
        )
        print(f"Fetched {len(self.folio.alt_title_types)} alt_title_types", flush=True)
        print(f"Fetched {len(self.folio.instance_types)} instance_types", flush=True)
        print(
            f"Fetched {len(self.folio.instance_formats)} instance_formats", flush=True
        )
        self.electronic_access_relationships = list(
            self.folio.folio_get_all(
                "/electronic-access-relationships",
                "electronicAccessRelationships",
                "?query=cql.allRecords=1 sortby name",
            )
        )
        self.default_contributor_name_type = self.folio.contrib_name_types[0]["id"]
        print(
            f"Default contributor name type is {self.default_contributor_name_type}",
            flush=True,
        )
        print(
            f"Fetched {len(self.electronic_access_relationships)} electronic_access_relationships",
            flush=True,
        )
        self.ref_data_dicts = {}
        self.holding_note_types = list(
            self.folio.folio_get_all(
                "/holdings-note-types",
                "holdingsNoteTypes",
                "?query=cql.allRecords=1 sortby name",
            )
        )
        print(f"Fetched {len(self.holding_note_types)} holding_note_types", flush=True)

        self.call_number_types = list(
            self.folio.folio_get_all(
                "/call-number-types",
                "callNumberTypes",
                "?query=cql.allRecords=1 sortby name",
            )
        )
        print(f"Fetched {len(self.call_number_types)} call_number_types", flush=True)

        self.locations = list(
            self.folio.folio_get_all(
                "/locations",
                "locations",
            )
        )
        print(f"Fetched {len(self.locations)} locations", flush=True)
        print(
            f"{len(self.folio.contrib_name_types)} contrib_name_types in tenant",
            flush=True,
        )

    def get_condition(self, name, value, parameter=None, marc_field=None):
        try:
            return self.condition_cache.get(name)(value, parameter, marc_field)
        except Exception:
            attr = getattr(self, "condition_" + str(name))
            self.condition_cache[name] = attr
            return attr(value, parameter, marc_field)

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
        t = self.get_ref_data_tuple_by_code(
            self.folio.instance_formats, "instance_formats_code", value
        )
        if not t:
            print("Unmapped Instance format code", value)
            return ""
        self.mapper.add_to_migration_report("Mapped instance formats", t[1])
        return t[0]

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

    def condition_set_issuance_mode_id(self, value, parameter, marc_field):
        # mode of issuance is handled elsewhere in the mapping.
        return ""

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
        return self.get_ref_data_tuple_by_name(
            self.folio.class_types, "class_types", parameter["name"]
        )[0]

    def condition_char_select(self, value, parameter, marc_field):
        return value[parameter["from"] : parameter["to"]]

    def condition_set_identifier_type_id_by_name(self, value, parameter, marc_field):
        if not self.folio.identifier_types:
            raise ValueError("No identifier_types setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.folio.identifier_types, "identifier_types", parameter["name"]
        )
        if not t:
            print("Unmapped identifier name types", parameter["name"])
            print(marc_field)
            raise Exception(
                f'Identifier mapping error.\n Parameter: {parameter.get("name", "")}\nMARC Field: {marc_field}'
            )
        self.mapper.add_to_migration_report("Mapped identifier types", t[1])
        return t[0]

    def condition_set_contributor_name_type_id(self, value, parameter, marc_field):
        if not self.folio.contrib_name_types:
            raise Exception("No contributor name types setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.folio.contrib_name_types, "contrib_name_types", parameter["name"]
        )
        if not t:
            self.mapper.add_to_migration_report(
                "Unmapped contributor name types", parameter["name"]
            )
            return self.default_contributor_name_type
        self.mapper.add_to_migration_report("Mapped contributor name types", t[1])
        return t[0]

    def condition_set_note_type_id(self, value, parameter, marc_field):
        t = self.get_ref_data_tuple_by_name(
            self.folio.instance_note_types, "instance_not_types", parameter["name"]
        )
        self.mapper.add_to_migration_report("Mapped note types", t[1])
        return t[0]

    def condition_set_contributor_type_id(self, value, parameter, marc_field):

        # Bootstrap stuff
        if not self.folio.contributor_types:
            raise ValueError("No contributor_types setup in tenant")
        if not self.default_contributor_type:
            self.default_contributor_type = next(
                ct for ct in self.folio.contributor_types if ct["code"] == "ctb"
            )

        for subfield in marc_field.get_subfields("4"):
            t = self.get_ref_data_tuple_by_code(
                self.folio.contributor_types, "contrib_types_c", subfield
            )
            if not t:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f"Mapping failed for $4 {subfield} ",
                )
            else:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f"Contributor type code {t} found for $4 {subfield})",
                )
                return t[0]

        for subfield in marc_field.get_subfields("e"):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            t = self.get_ref_data_tuple_by_name(
                self.folio.contributor_types, "contrib_types_n", normalized_subfield
            )

            if not t:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f"Mapping failed for $e {normalized_subfield} ({subfield}) ",
                )
            else:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f"Contributor type name {t[1]} found for $e {normalized_subfield} ({subfield}) ",
                )
                return t[0]
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

    def condition_set_call_number_type_by_indicator(
        self, value, parameter, marc_field: pymarc.Field
    ):
        if not self.call_number_types:
            raise ValueError("No call_number_types setup in tenant")

        if not self.default_call_number_type:
            self.default_call_number_type = next(
                ct
                for ct in self.call_number_types
                if ct["name"] == "Other scheme"
            )
        first_level_map = {
            "1": "Dewey Decimal classification",
            "0": "Library of Congress classification",
            "2": "National Library of Medicine classification",
            "8": "Other scheme",
            "6": "Shelved separately",
            "4": "Shelving control number",
            "7": "Source specified in subfield $2",
            "3": "Superintendent of Documents classification",
            "5": "Title",
        }

        # CallNumber type specified in $2. This needs further mapping
        if marc_field.indicator1 == "7" and "2" in marc_field:
            self.mapper.add_to_migration_report(
                "Callnumber types",
                f"Unhandled call number type in $2 (ind1 == 7) {marc_field['2']}",
            )
            return self.default_call_number_type["id"]

        # Normal way. Type in ind1
        call_number_type_name_temp = first_level_map.get(marc_field.indicator1, "")
        if not call_number_type_name_temp:
            self.mapper.add_to_migration_report(
                "Callnumber types",
                f'Unhandled call number type in ind1: "{marc_field.indicator1}"',
            )
            return self.default_call_number_type["id"]
        t = self.get_ref_data_tuple_by_name(
            self.call_number_types, "cnt", call_number_type_name_temp
        )
        if t:
            self.mapper.add_to_migration_report(
                "Callnumber types", f"Mapped from Indicator 1 {t[0]}"
            )
            return t[0]

        self.mapper.add_to_migration_report(
            "Callnumber types", f"Mapping failed. Setting default CallNumber type."
        )
        return self.default_call_number_type["id"]

    def condition_set_electronic_if_serv_remo(self, value, parameter, marc_field):
        if value in ["serv", "remo"]:
            t = self.conditions.get_ref_data_tuple_by_name(self.holdings_types, "hold_types", "Electronic" )
            if t:
                self.add_to_migration_report("Holdings type mapping", f"special cornell case {t[1]}")
                return t[0]
        return ""
    
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
        if not t:
            raise Exception(
                f"Alternative title type not found for {parameter['name']} {marc_field}"
            )

        self.mapper.add_to_migration_report("Mapped Alternative title types", t[1])
        return t[0]

    def condition_set_location_id_by_code(self, value, parameter, marc_field):
        self.mapper.add_to_migration_report("Legacy location codes", value)

        # Setup mapping if not already set up
        if "legacy_locations" not in self.ref_data_dicts:
            d = {}
            for lm in self.mapper.location_map:
                d[lm["legacy_code"]] = lm["folio_code"]
            self.ref_data_dicts["legacy_locations"] = d

        # Get the right code from the location map
        if self.mapper.location_map and any(self.mapper.location_map):
            mapped_code = self.ref_data_dicts["legacy_locations"].get(value, "")
            if not mapped_code:
                self.mapper.add_to_migration_report(
                    "Locations - Unmapped legacy codes", value
                )
        else:  # IF there is no map, assume legacy code is the same as FOLIO code
            mapped_code = value

        # Get the FOLIO UUID for the code and return it
        t = self.get_ref_data_tuple_by_code(self.locations, "locations", mapped_code)
        if not t:
            t = self.get_ref_data_tuple_by_code(
                self.locations, "locations", parameter["unspecifiedLocationCode"]
            )
        if not t:
            raise Exception(
                f"Location not found: {parameter['unspecifiedLocationCode']} {marc_field}"
            )
        self.mapper.add_to_migration_report("Mapped Locations", t[1])
        return t[0]

    def get_ref_data_tuple_by_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        ref_object = self.ref_data_dicts.get(dict_key, {}).get(key_value.lower(), ())
        if ref_object:
            return ref_object
        else:
            d = {}
            for r in ref_data:
                d[r[key_type].lower()] = (r["id"], r["name"])
            self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower(), ())

    def condition_remove_substring(self, value, parameter, marc_field):
        return value.replace(parameter["substring"], "")

    def condition_set_instance_type_id(self, value, parameter, marc_field):
        if marc_field.tag not in ["008", "336"]:
            self.mapper.add_to_migration_report(
                "Instance Type Mapping (336, 008)",
                f"Unhandled MARC tag {marc_field.tag} ",
            )
        return ""  # functionality moved

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
