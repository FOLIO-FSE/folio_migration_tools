import re


class Conditions:
    def __init__(self, folio):
        self.filter_chars = r"[.,\/#!$%\^&\*;:{}=\-_`~()]"
        self.filter_chars_dop = r"[.,\/#!$%\^&\*;:{}=\_`~()]"
        self.filter_last_chars = r",$"
        self.folio = folio
        self.instance_note_types = {}
        self.contrib_name_types = {}
        self.electronic_access_relationships = {}
        self.cache = {}
        self.default_contributor_type = {}

    def get_condition(self, name, value, parameter=None, marc_field=None):
        # try:
        if not self.cache.get(name, ""):
            self.cache[name] = getattr(self, "condition_" + str(name))
        return self.cache[name](value, parameter, marc_field)
        # return getattr(self, 'condition_' + str(name))(value, parameter,
        # marc_field)
        # except AttributeError as attrib_error:
        #     print(f'Unhandled condition: {name} {attrib_error} ')
        #    return value
        # except ValueError as value_error:
        #     print(
        #         f'Unhandled value: {name} {value_error} {type(value_error)} ')
        #    return value

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
        try:
            seventh = marc_field[6]
            m_o_i_s = {
                "m": "Monograph",
                "s": "Serial",
                "i": "Integrating Resource",
            }
            name = m_o_i_s.get(seventh, "Other")
            if not name:
                raise Exception(f"{name} is not a valid mode of issuance")
            return next(
                i["id"] for i in self.folio.modes_of_issuance if name == i["name"]
            )
        except IndexError:
            raise ValueError(f"No seven in {marc_field}")

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

        try:
            if "oclc_regex" in parameter:
                if re.match(parameter["oclc_regex"], value):
                    return next(
                        f["id"]
                        for f in self.folio.identifier_types
                        if f["name"] in parameter["names"][1]
                    )
                else:
                    return next(
                        f["id"]
                        for f in self.folio.identifier_types
                        if f["name"] in parameter["names"][0]
                    )

            return next(
                f["id"]
                for f in self.folio.identifier_types
                if f["name"] in parameter["names"]
            )
        except Exception as exception:
            raise ValueError(
                f"no matching identifier_types in {parameter['names']} {marc_field} {exception}"
            )

    def condition_set_note_type_id(self, value, parameter, marc_field):
        if not any(self.instance_note_types):
            self.instance_note_types = self.folio.folio_get_all(
                "/instance-note-types", "instanceNoteTypes", "?query=cql.allRecords=1"
            )
        v = next(
            (
                f["id"]
                for f in self.instance_note_types
                if f["name"].casefold() == parameter["name"].casefold()
            ),
            "",
        )
        if not v:
            raise ValueError(
                f"no matching instance_note_types {parameter['name']} {marc_field}"
            )
        return v

    def condition_set_classification_type_id(self, value, parameter, marc_field):
        # undef = next((f['id'] for f in self.folio.class_types
        #             if f['name'] == 'No type specified'), '')
        if not self.folio.class_types:
            raise ValueError("No class_types setup in tenant")
        v = next(
            (f["id"] for f in self.folio.class_types if f["name"] == parameter["name"]),
            "",
        )
        if not v:
            raise ValueError(
                f'no matching classification_type {parameter["name"]} {marc_field}'
            )
        return v

    def condition_char_select(self, value, parameter, marc_field):
        return value[parameter["from"] : parameter["to"]]

    def condition_set_identifier_type_id_by_name(self, value, parameter, marc_field):
        if not self.folio.identifier_types:
            raise ValueError("No identifier_types setup in tenant")
        v = next(
            (
                f["id"]
                for f in self.folio.identifier_types
                if f["name"].casefold() == parameter["name"].casefold()
            ),
            "",
        )
        if not v:
            raise ValueError(
                f'no matching identifier_type_id {parameter["name"]} {marc_field}'
            )
        return v

    def condition_set_contributor_name_type_id(self, value, parameter, marc_field):
        if not any(self.contrib_name_types):
            self.contrib_name_types = {
                f["name"]: f["id"] for f in self.folio.contrib_name_types
            }
        if not self.folio.contrib_name_types:
            raise ValueError("No contrib_name_types setup in tenant")
        return self.contrib_name_types[parameter["name"]]

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
                    return cont_type["id"]
                elif subfield == cont_type["name"]:
                    return cont_type["id"]
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
        v = next(
            (
                f["id"]
                for f in self.folio.alt_title_types
                if f["name"] == parameter["name"]
            ),
            "",
        )
        if not v:
            raise ValueError(
                f"no matching alt_title_types {parameter['name']}  {marc_field}"
            )
        return v

    def condition_remove_substring(self, value, parameter, marc_field):
        return value.replace(parameter["substring"], "")

    def condition_set_instance_type_id(self, value, parameter, marc_field):
        if not self.folio.instance_types:
            raise ValueError("No instance_types setup in tenant")
        v = next(
            (f["id"] for f in self.folio.instance_types if f["code"] == value[:3]), ""
        )
        if not v:
            if "a" in marc_field:
                w = next(
                    (
                        f["id"]
                        for f in self.folio.instance_types
                        if f["name"] == marc_field["a"]
                    ),
                    "",
                )
                if not w:
                    raise ValueError(
                        f"no matching instance_types {value[:3]} {marc_field}"
                    )
                return w
        return v

    def condition_set_instance_format_id(self, value, parameter, marc_field):
        if not self.folio.instance_formats:
            raise ValueError("No instance_formats setup in tenant")
        v = next(
            (f["id"] for f in self.folio.instance_formats if f["code"] == value), ""
        )
        if not v:
            raise ValueError(f"no matching instance_formats {value} {marc_field}")
        return v

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

        if not any(self.electronic_access_relationships):
            self.electronic_access_relationships = self.folio.folio_get_all(
                "/electronic-access-relationships",
                "electronicAccessRelationships",
                "?query=cql.allRecords=1 sortby name",
            )
        if not self.electronic_access_relationships:
            raise ValueError("No electronic_access_relationships setup in tenant")
        v = next(
            (
                f["id"]
                for f in self.electronic_access_relationships
                if f["name"].lower() == name
            ),
            "",
        )
        if not v:
            raise ValueError(
                f"no matching electronic_access_relationships {name} {marc_field}"
            )
        return v
