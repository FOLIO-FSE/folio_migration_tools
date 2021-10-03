import logging
from marc_to_folio.rules_mapper_base import RulesMapperBase
from marc_to_folio.mapping_file_transformation.mapper_base import MapperBase
from marc_to_folio.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationRecordFailedError,
    TransformationProcessError,
)
import re
import pymarc
from pymarc import field


class Conditions:
    def __init__(
        self,
        folio,
        mapper: RulesMapperBase,
        object_type,
        default_location_code="",
        default_call_number_type_id="",
    ):
        logging.debug(f"default location code is still {default_location_code}")
        self.default_location_code = default_location_code
        logging.debug("Init conditions!")
        self.filter_chars = r"[.,\/#!$%\^&\*;:{}=\-_`~()]"
        self.stats = {}
        self.filter_chars_dop = r"[.,\/#!$%\^&\*;:{}=\_`~()]"
        self.filter_last_chars = r",$"
        self.folio = folio
        self.default_contributor_type = ""
        self.mapper = mapper
        self.ref_data_dicts = {}
        self.setup_reference_data_for_all()
        if object_type == "bibs":
            self.setup_reference_data_for_bibs()
        else:
            self.setup_reference_data_for_items_and_holdings(
                default_call_number_type_id
            )
        self.condition_cache = {}

    def setup_reference_data_for_bibs(self):
        logging.info("Setting up reference data for bib transformation")
        logging.info(f"{len(self.folio.contrib_name_types)}\tcontrib_name_types")
        logging.info(f"{len(self.folio.contributor_types)}\tcontributor_types")
        logging.info(f"{len(self.folio.alt_title_types )}\talt_title_types")
        logging.info(f"{len(self.folio.identifier_types)}\tidentifier_types")
        # Raise for empty settings
        if not self.folio.contributor_types:
            raise TransformationProcessError("No contributor_types setup in tenant")
        if not self.folio.contrib_name_types:
            raise TransformationProcessError(
                "No contributor name types setup in tenant"
            )
        if not self.folio.identifier_types:
            raise TransformationProcessError("No identifier_types setup in tenant")
        if not self.folio.identifier_types:
            raise TransformationProcessError("No identifier_types setup in tenant")
        if not self.folio.alt_title_types:
            raise TransformationProcessError("No alt_title_types setup in tenant")

        # Set defaults
        logging.info("Setting defaults")
        self.default_contributor_name_type = self.folio.contrib_name_types[0]["id"]
        logging.info(f"Contributor name type:\t{self.default_contributor_name_type}")
        self.default_contributor_type = next(
            ct for ct in self.folio.contributor_types if ct["code"] == "ctb"
        )
        logging.info(f"Contributor type:\t{self.default_contributor_type['id']}")

    def setup_reference_data_for_items_and_holdings(self, default_call_number_type_id):
        logging.info(f"{len(self.folio.locations)}\tlocations")
        self.default_call_number_type = {}
        logging.info(f"{len(self.folio.holding_note_types)}\tholding_note_types")
        logging.info(f"{len(self.folio.call_number_types)}\tcall_number_types")
        self.holdings_types = list(
            self.folio.folio_get_all("/holdings-types", "holdingsTypes")
        )
        logging.info(f"{len(self.holdings_types)}\tholdings types")
        # Raise for empty settings
        if not self.folio.holding_note_types:
            raise TransformationProcessError("No holding_note_types setup in tenant")
        if not self.folio.call_number_types:
            raise TransformationProcessError("No call_number_types setup in tenant")
        if not self.holdings_types:
            raise TransformationProcessError("No holdings_types setup in tenant")
        if not self.folio.locations:
            raise TransformationProcessError("No locations set up in tenant")

        # Set defaults
        logging.info("Defaults")
        self.default_call_number_type_id = default_call_number_type_id
        logging.info(f"Default Callnumber type ID:\t{self.default_call_number_type_id}")
        self.default_call_number_type = next(
            ct
            for ct in self.folio.call_number_types
            if ct["id"] == self.default_call_number_type_id
        )
        if not self.default_call_number_type:
            raise TransformationProcessError(
                f"No callnumber type with ID "
                f"{self.default_call_number_type_id} set up in tenant"
            )
        logging.info(
            f"Default Callnumber type Name:\t{self.default_call_number_type['name']}"
        )
        t = self.get_ref_data_tuple_by_name(
            self.holdings_types, "holdings_types", "Unmapped"
        )
        if t:
            self.default_holdings_type_id = t[0]
        else:
            raise TransformationProcessError(
                "Holdings type Unmapped not set in client: Please add one to the tenant."
            )

        if not self.default_location_code:
            raise TransformationProcessError(
                "Default location code is not set up. Make sure it is set up"
            )

        logging.info(f"Default location code is {self.default_location_code}")
        t = self.get_ref_data_tuple_by_code(
            self.folio.locations, "locations", self.default_location_code
        )
        if t:
            self.default_location_id = t[0]
            logging.info(f"Default location id is {self.default_location_id}")
        else:
            raise TransformationProcessError(
                f"Default location for {self.default_location_code} is not set up"
            )

    def setup_reference_data_for_all(self):
        logging.info(
            f"{len(self.folio.electronic_access_relationships)}\telectronic_access_relationships"
        )
        logging.info(f"{len(self.folio.class_types)}\tclass_types")
        self.statistical_codes = list(
            self.folio.folio_get_all("/statistical-codes", "statisticalCodes")
        )
        logging.info(f"{len(self.statistical_codes)} \tstatistical_codes")

        # Raise for empty settings
        if not self.folio.class_types:
            raise TransformationProcessError("No class_types setup in tenant")

    def get_condition(
        self, name, value, parameter=None, marc_field: field.Field = None
    ):
        try:
            return self.condition_cache.get(name)(value, parameter, marc_field)
        # Exception should only handle the missing condition from the cache.
        # All other exceptions should propagate up
        except Exception:
            attr = getattr(self, "condition_" + str(name))
            self.condition_cache[name] = attr
            return attr(value, parameter, marc_field)

    def condition_trim_period(self, value, parameter, marc_field: field.Field):
        return value.strip().rstrip(".").rstrip(",")

    def condition_trim(self, value, parameter, marc_field: field.Field):
        return value.strip()

    def condition_get_value_if_subfield_is_empty(
        self, value, parameter, marc_field: field.Field
    ):
        if value.strip():
            return value.strip()
        self.mapper.add_to_migration_report(
            "Added value since value is empty",
            f"Tag: {marc_field.tag}. Added value: {parameter['value']}",
        )
        return parameter["value"]

    def condition_remove_ending_punc(self, value, parameter, marc_field: field.Field):
        v = value
        chars = ".;:,/+=- "
        while any(v) > 0 and v[-1] in chars:
            v = v.rstrip(v[-1])
        return v

    def condition_set_instance_format_id(
        self, value, parameter, marc_field: field.Field
    ):
        # This method only handles the simple case of 2-character codes of RDA in the first 338$b
        # Other cases are handled in performAddidtionalParsing in the mapper class
        try:
            t = self.get_ref_data_tuple_by_code(
                self.folio.instance_formats, "instance_formats_code", value
            )
            self.mapper.add_to_migration_report(
                "Instance format ids handling (337 + 338)",
                f'Successful match  - "{value}"->{t[1]}',
            )
            return t[0]
        except:
            self.mapper.add_to_migration_report(
                "Instance format ids handling (337 + 338)",
                f'Value NOT found in FOLIO: "{value}"',
            )
            return ""

    def condition_remove_prefix_by_indicator(
        self, value, parameter, marc_field: field.Field
    ):
        """Returns the index title according to the rules"""
        ind2 = marc_field.indicator2
        reg_str = r"[\s:\/]{0,3}$"
        if ind2 not in map(str, range(1, 9)):
            return re.sub(reg_str, "", value)

        num_take = int(ind2)
        return re.sub(reg_str, "", value[num_take:])

    def condition_capitalize(self, value, parameter, marc_field: field.Field):
        return value.capitalize()

    def condition_clean_isbn(self, value, parameter, marc_field: field.Field):
        return value

    def condition_set_issuance_mode_id(self, value, parameter, marc_field: field.Field):
        # mode of issuance is handled elsewhere in the mapping.
        return ""

    def condition_set_publisher_role(self, value, parameter, marc_field: field.Field):
        roles = {
            "0": "Production",
            "1": "Publication",
            "2": "Distribution",
            "3": "Manufacture",
            "4": "Copyright notice date",
        }
        role = roles.get(marc_field.indicator2, "")
        self.mapper.add_to_migration_report(
            "Mapped publisher role from Indicator2",
            f"{marc_field.tag} ind2 {marc_field.indicator2}->{role}",
        )
        return role

    def condition_set_identifier_type_id_by_value(
        self, value, parameter, marc_field: field.Field
    ):
        if "oclc_regex" in parameter:
            if re.match(parameter["oclc_regex"], value):
                t = self.get_ref_data_tuple_by_name(
                    self.folio.identifier_types,
                    "identifier_types",
                    parameter["names"][1],
                )
            else:
                t = self.get_ref_data_tuple_by_name(
                    self.folio.identifier_types,
                    "identifier_types",
                    parameter["names"][0],
                )
            self.mapper.add_to_migration_report(
                "Mapped identifier types", f"{marc_field.tag} -> {t[1]}"
            )
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
            raise TransformationFieldMappingError(
                "", f"no matching identifier_types in {parameter['names']}", marc_field
            )
        return my_id

    def condition_set_holding_note_type_id_by_name(
        self, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.holding_note_types, "holding_note_types", parameter["name"]
            )
            self.mapper.add_to_migration_report("Mapped note types", t[1])
            return t[0]
        except Exception as ee:
            logging.error(ee)
            raise TransformationRecordFailedError(
                "unknown",
                f'Holdings note type mapping error.\tParameter: {parameter.get("name", "")}\t'
                f"MARC Field: {marc_field}. Is mapping rules and ref data aligned?",
                parameter.get("name", ""),
            )

    def condition_set_classification_type_id(
        self, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.class_types, "class_types", parameter["name"]
            )
            self.mapper.add_to_migration_report("Mapped classification types", t[1])
            return t[0]
        except:
            raise TransformationRecordFailedError(
                "unknown",
                f'Classification mapping error.\tParameter: "{parameter.get("name", "")}"\t'
                f"MARC Field: {marc_field}. Is mapping rules and ref data aligned?",
                parameter.get("name", ""),
            )

    def condition_char_select(self, value, parameter, marc_field: field.Field):
        return value[parameter["from"] : parameter["to"]]

    def condition_set_receipt_status(self, value, parameter, marc_field: field.Field):
        if len(value) < 7:
            self.mapper.add_to_migration_report(
                "Reciept status mapping", f"008 is too short: {value}"
            )
            return ""
        try:
            status_map = {
                "0": "Unknown",
                "1": "Other receipt or acquisition status",
                "2": "Received and complete or ceased",
                "3": "On order",
                "4": "Currently received",
                "5": "Not currently received",
                "6": "External access",
            }
            mapped_value = status_map[value[6]]
            self.mapper.add_to_migration_report(
                "Reciept status mapping", f"{value[6]} mapped to {mapped_value}"
            )

            return
        except:
            self.mapper.add_to_migration_report(
                "Reciept status mapping", f"{value[6]} not found in map."
            )
            return "Unknown"

    def condition_set_identifier_type_id_by_name(
        self, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.identifier_types, "identifier_types", parameter["name"]
            )
            self.mapper.add_to_migration_report(
                "Mapped identifier types", f"{marc_field.tag} -> {t[1]}"
            )
            return t[0]
        except:
            raise TransformationRecordFailedError(
                "",
                f'Unmapped identifier name type: "{parameter["name"]}"\tMARC Field: {marc_field}'
                f"MARC Field: {marc_field}. Is mapping rules and ref data aligned?",
                {parameter["name"]},
            )

    def condition_set_contributor_name_type_id(
        self, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.contrib_name_types, "contrib_name_types", parameter["name"]
            )
            self.mapper.add_to_migration_report(
                "Mapped contributor name types", f"{marc_field.tag} -> {t[1]}"
            )
            return t[0]
        except:
            self.mapper.add_to_migration_report(
                "Unmapped contributor name types", parameter["name"]
            )
            return self.default_contributor_name_type

    def condition_set_note_type_id(self, value, parameter, marc_field: field.Field):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.instance_note_types, "instance_not_types", parameter["name"]
            )
            self.mapper.add_to_migration_report(
                "Mapped note types",
                f"{marc_field.tag} ({parameter.get('name', '')}) -> {t[1]}",
            )
            return t[0]
        except:
            raise ValueError(
                f"Instance note type not found for {marc_field} {parameter}"
            )

    def condition_set_contributor_type_id(
        self, value, parameter, marc_field: field.Field
    ):
        for subfield in marc_field.get_subfields("4"):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            t = self.get_ref_data_tuple_by_code(
                self.folio.contributor_types, "contrib_types_c", normalized_subfield
            )
            if not t:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f'Mapping failed for $4 "{subfield}" ({normalized_subfield}) ',
                )
            else:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f'Contributor type code {t[1]} found for $4 "{subfield}" ({normalized_subfield}))',
                )
                return t[0]
        subfield_code = "j" if marc_field.tag in ["111", "711"] else "e"
        for subfield in marc_field.get_subfields(subfield_code):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            t = self.get_ref_data_tuple_by_name(
                self.folio.contributor_types, "contrib_types_n", normalized_subfield
            )

            if not t:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f'Mapping failed for $e "{normalized_subfield}" ({subfield}) ',
                )
            else:
                self.mapper.add_to_migration_report(
                    "Contributor type mapping",
                    f'Contributor type name {t[1]} found for $e "{normalized_subfield}" ({subfield}) ',
                )
                return t[0]
        return self.default_contributor_type["id"]

    def condition_set_instance_id_by_map(
        self, value, parameter, marc_field: field.Field
    ):
        try:
            return self.mapper.instance_id_map[value]["folio_id"]
        except:
            self.mapper.add_stats(self.mapper.stats, "bib id not in map")
            raise TransformationRecordFailedError(
                f"Old instance id not in map: {value} Field: {marc_field}"
            )

    def condition_set_url_relationship(self, value, parameter, marc_field: field.Field):
        enum = {
            "0": "resource",
            "1": "version of resource",
            "2": "related resource",
            "8": "no information provided",
        }
        ind2 = marc_field.indicator2
        name = enum.get(ind2, enum["8"])
        if not self.folio.electronic_access_relationships:
            raise ValueError("No electronic_access_relationships setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.folio.electronic_access_relationships,
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
        first_level_map = {
            "0": "Library of Congress classification",
            "1": "Dewey Decimal classification",
            "2": "National Library of Medicine classification",
            "3": "Superintendent of Documents classification",
            "4": "Shelving control number",
            "5": "Title",
            "6": "Shelved separately",
            "7": "Source specified in subfield $2",
            "8": "Other scheme",
        }

        # CallNumber type specified in $2. This needs further mapping
        if marc_field.indicator1 == "7" and "2" in marc_field:
            self.mapper.add_to_migration_report(
                "Callnumber type mapping",
                f"Unhandled call number type in $2 (ind1 == 7) {marc_field['2']}",
            )
            return self.default_call_number_type["id"]

        # Normal way. Type in ind1
        call_number_type_name_temp = first_level_map.get(marc_field.indicator1, "")
        if not call_number_type_name_temp:
            self.mapper.add_to_migration_report(
                "Callnumber type mapping",
                f'Unhandled call number type in ind1: "{marc_field.indicator1}"',
            )
            return self.default_call_number_type["id"]
        t = self.get_ref_data_tuple_by_name(
            self.folio.call_number_types, "cnt", call_number_type_name_temp
        )
        if t:
            self.mapper.add_to_migration_report(
                "Callnumber type mapping",
                f"Mapped from Indicator 1 {marc_field.indicator1} -> {t[1]}",
            )
            return t[0]

        self.mapper.add_to_migration_report(
            "Callnumber type mapping",
            "Mapping failed. Setting default CallNumber type.",
        )

        return self.default_call_number_type["id"]

    def condition_set_electronic_if_serv_remo(
        self, value, parameter, marc_field: field.Field
    ):
        if value == "serv,remo":
            t = self.get_ref_data_tuple_by_name(
                self.holdings_types, "hold_types", "Electronic"
            )
            if t:
                self.mapper.add_to_migration_report(
                    "Holdings type mapping", f"special cornell case {t[1]}"
                )
                return t[0]
        return ""

    def condition_set_contributor_type_text(
        self, value, parameter, marc_field: field.Field
    ):
        for subfield in marc_field.get_subfields("4", "e"):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            for cont_type in self.folio.contributor_types:
                if normalized_subfield in [cont_type["code"], cont_type["name"]]:
                    return cont_type["name"]
        return self.default_contributor_type["name"]

    def condition_set_alternative_title_type_id(self, value, parameter, marc_field):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.alt_title_types, "alt_title_types", parameter["name"]
            )
            self.mapper.add_to_migration_report("Mapped Alternative title types", t[1])
            return t[0]
        except:
            raise TransformationFieldMappingError(
                f"Alternative title type not found for {parameter['name']} {marc_field}"
            )

    def condition_set_location_id_by_code(
        self, value, parameter, marc_field: field.Field
    ):
        # Setup mapping if not already set up
        try:
            if "legacy_locations" not in self.ref_data_dicts:
                d = {
                    lm["legacy_code"]: lm["folio_code"]
                    for lm in self.mapper.location_map
                }
                self.ref_data_dicts["legacy_locations"] = d
        except KeyError as ke:
            if "folio_code" in str(ke):
                raise TransformationProcessError(
                    "Your location map lacks the column folio_code"
                )
            if "legacy_code" in str(ke):
                raise TransformationProcessError(
                    "Your location map lacks the column legacy_code"
                )
        # Get the right code from the location map
        if self.mapper.location_map and any(self.mapper.location_map):
            mapped_code = (
                self.ref_data_dicts["legacy_locations"].get(value.strip(), "").strip()
            )
        else:  # IF there is no map, assume legacy code is the same as FOLIO code
            mapped_code = value.strip()
        # Get the FOLIO UUID for the code and return it
        try:
            t = self.get_ref_data_tuple_by_code(
                self.folio.locations, "locations", mapped_code
            )
            self.mapper.add_to_migration_report(
                "Location mapping", f"'{value}' ({mapped_code}) -> {t[1]}"
            )
            return t[0]
        except Exception:
            t = self.get_ref_data_tuple_by_code(
                self.folio.locations, "locations", parameter["unspecifiedLocationCode"]
            )
            if not t:
                raise TransformationProcessError(
                    f"DefaultLocation not found: {parameter['unspecifiedLocationCode']} {marc_field}"
                )
            self.mapper.add_to_migration_report(
                "Location mapping",
                f"Unmapped. Set default location. '{value}' ({mapped_code}) -> {t[1]}",
            )
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
        d = {r[key_type].lower(): (r["id"], r["name"]) for r in ref_data}
        self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower(), ())

    def condition_remove_substring(self, value, parameter, marc_field: field.Field):
        return value.replace(parameter["substring"], "")

    def condition_set_instance_type_id(self, value, parameter, marc_field: field.Field):
        if marc_field.tag not in ["008", "336"]:
            self.mapper.add_to_migration_report(
                "Instance Type Mapping (336, 008)",
                (
                    f"Unhandled MARC tag {marc_field.tag}. Instance Type ID is only mapped "
                    "from 008 and 336 "
                ),
            )
        return ""  # functionality moved

    def condition_set_electronic_access_relations_id(
        self, value, parameter, marc_field: field.Field
    ):
        enum = {
            "0": "resource",
            "1": "version of resource",
            "2": "related resource",
            "3": "no information provided",
        }
        ind2 = marc_field.indicator2
        name = enum.get(ind2, enum["3"])

        if not self.folio.electronic_access_relationships:
            raise ValueError("No electronic_access_relationships setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.folio.electronic_access_relationships,
            "electronic_access_relationships",
            name,
        )
        self.mapper.add_to_migration_report(
            "Mapped electronic access relationships types", t[1]
        )
        return t[0]

    def condition_set_note_staff_only_via_indicator(
        self, value, parameter, marc_field: field.Field
    ):
        """Returns true of false depending on the first indicator"""
        # https://www.loc.gov/marc/bibliographic/bd541.html
        ind1 = marc_field.indicator1
        self.mapper.add_to_migration_report(
            "Set note staff only via indicator",
            f"{marc_field.tag} indicator1: {ind1} (1 is public, all other values are Staff only)",
        )
        if ind1 != "1":
            return "true"
        return "false"
