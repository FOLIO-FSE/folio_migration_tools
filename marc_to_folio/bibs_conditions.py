import logging
import re


class BibsConditions:
    def __init__(self, folio, mapper):
        self.filter_chars = r"[.,\/#!$%\^&\*;:{}=\-_`~()]"
        self.stats = {}
        self.filter_chars_dop = r"[.,\/#!$%\^&\*;:{}=\_`~()]"
        self.filter_last_chars = r",$"
        self.folio = folio
        self.instance_note_types = {}
        self.contrib_name_types = {}
        self.electronic_access_relationships = {}
        self.mapper = mapper
        self.cache = {}
        self.default_contributor_type = {}
        print(f"Fetched {len(self.folio.modes_of_issuance)} modes of issuances")
        print(f"Fetched {len(self.folio.identifier_types)} identifier types")
        print(f"Fetched {len(self.folio.instance_note_types)} note types")
        print(f"Fetched {len(self.folio.modes_of_issuance)} modes of issuances")
        print(f"Fetched {len(self.folio.contrib_name_types)} contrib_name_types")
        print(f"Fetched {len(self.folio.contributor_types)} contributor_types")
        print(f"Fetched {len(self.folio.alt_title_types)} alt_title_types")
        print(f"Fetched {len(self.folio.instance_types)} instance_types")
        print(f"Fetched {len(self.folio.instance_formats)} instance_formats")
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
                t = get_ref_data_tuple_by_name(
                    self.folio.identifier_types, parameter["names"][1]
                )
                self.mapper.add_to_migration_report("Mapped identifier types", t[1])
                return t[0]
            else:
                t = get_ref_data_tuple_by_name(
                    self.folio.identifier_types, parameter["names"][0]
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

    def condition_set_note_type_id(self, value, parameter, marc_field):
        t = get_ref_data_tuple_by_name(
            self.folio.instance_note_types, parameter["name"]
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
        t = get_ref_data_tuple_by_name(self.folio.identifier_types, parameter["name"])
        self.mapper.add_to_migration_report("Mapped identifier types", t[1])
        return t[0]

    def condition_set_contributor_name_type_id(self, value, parameter, marc_field):
        if not self.folio.contrib_name_types:
            raise ValueError("No contrib_name_types setup in tenant")
        t = get_ref_data_tuple_by_name(self.folio.contrib_name_types, parameter["name"])
        self.mapper.add_to_migration_report("Mapped Contributor name types", t[1])
        return t[0]

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
        t = get_ref_data_tuple_by_name(self.folio.alt_title_types, parameter["name"])
        self.mapper.add_to_migration_report("Mapped Alternative title types", t[1])
        return t[0]

    def condition_remove_substring(self, value, parameter, marc_field):
        return value.replace(parameter["substring"], "")

    def condition_set_instance_type_id(self, value, parameter, marc_field):
        if not self.folio.instance_types:
            raise ValueError("No instance_types setup in tenant")
        if marc_field.tag == "008":
            t = get_ref_data_tuple_code(self.folio.instance_types, value[:3])
            if not t:
                t = get_ref_data_tuple_code(self.folio.instance_types, "zzz")
            self.mapper.add_to_migration_report("Mapped Instance types", t[1])
            return t[0]
        elif marc_field.tag == "336" and "b" in marc_field:
            t = get_ref_data_tuple_by_name(self.folio.instance_types, marc_field["b"])
            if not t:
                t = get_ref_data_tuple_code(self.folio.instance_types, "zzz")
            self.mapper.add_to_migration_report("Mapped Instance types", t[1])
            return t[0]
        else:
            # TODO Remove later. Corenell specific
            t = get_ref_data_tuple_code(self.folio.instance_types, "txt")
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
        t = get_ref_data_tuple_by_name(self.electronic_access_relationships, name)
        self.mapper.add_to_migration_report(
            "Mapped electronic access relationships types", t[1]
        )
        return t[0]


def get_ref_data_tuple_code(ref_data, code):
    return get_ref_data_tuple(ref_data, code, "code")


def get_ref_data_tuple_by_name(ref_data, name):
    return get_ref_data_tuple(ref_data, name, "name")


def get_ref_data_tuple(ref_data, key_value, key_type):
    ref_object = next(
        (
            f
            for f in ref_data
            if str(f[key_type]).casefold() == str(key_value).casefold()
        ),
        None,
    )
    if not ref_object:
        logging.debug(f"No matching element for {key_value} in {list(ref_data)}")
        return None
    if validate_uuid(ref_object["id"]):
        return (ref_object["id"], ref_object["name"])
    else:
        raise Exception(f"UUID Validation error for {key_value} in {ref_data}")


def validate_uuid(my_uuid):
    # removed for performance reasons
    return True
    """reg = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    pattern = re.compile(reg)
    if pattern.match(my_uuid):
        return True
    else:
        return False"""
