import logging
import re
import traceback

import i18n
from folioclient import FolioClient
from pymarc import field

from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)

# flake8: noqa: s


class Conditions:
    holdings_type_map = {
        "u": "Unknown",
        "v": "Multi-part monograph",
        "x": "Monograph",
        "y": "Serial",
    }

    def __init__(
        self,
        folio: FolioClient,
        mapper: RulesMapperBase,
        object_type,
        folio_release: FolioRelease,
        default_call_number_type_name="",
    ):
        self.filter_chars = r"[.,\/#!$%\^&\*;:{}=\-_`~()]"
        self.filter_chars_dop = r"[.,\/#!$%\^&\*;:{}=\_`~()]"
        self.folio_release: FolioRelease = folio_release
        self.filter_last_chars = r",$"
        self.folio = folio
        self.default_contributor_type: dict = {}
        self.mapper: RulesMapperBase = mapper
        self.ref_data_dicts = {}
        if object_type == "bibs":
            self.setup_reference_data_for_all()
            self.setup_reference_data_for_bibs()
        else:
            self.setup_reference_data_for_all()
            self.setup_reference_data_for_items_and_holdings(default_call_number_type_name)
        self.object_type = object_type
        self.condition_cache: dict = {}

    def setup_reference_data_for_bibs(self):
        logging.info("Setting up reference data for bib transformation")
        logging.info("%s\tcontrib_name_types", len(self.folio.contrib_name_types))  # type: ignore
        logging.info("%s\tcontributor_types", len(self.folio.contributor_types))  # type: ignore
        logging.info("%s\talt_title_types", len(self.folio.alt_title_types))  # type: ignore
        logging.info("%s\tidentifier_types", len(self.folio.identifier_types))  # type: ignore
        logging.info("%s\tsubject_types", len(self.folio.subject_types))  # type: ignore
        # Raise for empty settings
        if not self.folio.contributor_types:
            raise TransformationProcessError("", "No contributor_types in FOLIO")
        if not self.folio.contrib_name_types:
            raise TransformationProcessError("", "No contributor name types in FOLIO")
        if not self.folio.identifier_types:
            raise TransformationProcessError("", "No identifier_types in FOLIO")
        if not self.folio.alt_title_types:
            raise TransformationProcessError("", "No alt_title_types in FOLIO")
        if not self.folio.subject_types:
            raise TransformationProcessError("", "No subject_types in FOLIO")

        # Set defaults
        logging.info("Setting defaults")
        self.default_contributor_name_type: str = self.folio.contrib_name_types[0]["id"]  # type: ignore
        logging.info("Contributor name type:\t%s", self.default_contributor_name_type)
        self.default_contributor_type = next(
            ct
            for ct in self.folio.contributor_types
            if ct["code"] == "ctb"  # type: ignore
        )
        logging.info("Contributor type:\t%s", self.default_contributor_type["id"])

    def setup_reference_data_for_items_and_holdings(self, default_call_number_type_name):
        logging.info(f"{len(self.folio.locations)}\tlocations")  # type: ignore
        self.default_call_number_type = {}
        logging.info("%s\tholding_note_types", len(self.folio.holding_note_types))  # type: ignore
        logging.info("%s\tcall_number_types", len(self.folio.call_number_types))  # type: ignore
        self.setup_and_validate_holdings_types()
        self.ill_policies = self.folio.folio_get_all("/ill-policies", "illPolicies")
        # Raise for empty settings
        if not self.folio.holding_note_types:
            raise TransformationProcessError("", "No holding_note_types in FOLIO")
        if not self.folio.call_number_types:
            raise TransformationProcessError("", "No call_number_types in FOLIO")
        if not self.folio.locations:
            raise TransformationProcessError("", "No locations in FOLIO")

        # Set defaults
        logging.info("Defaults")
        self.default_call_number_type: dict = next(
            (
                ct
                for ct in self.folio.call_number_types  # type: ignore
                if ct["name"] == default_call_number_type_name
            ),
            None,
        )
        if not self.default_call_number_type:
            raise TransformationProcessError(
                "",
                (
                    f"No callnumber type with name "
                    f"{default_call_number_type_name} in FOLIO.\n"
                    "Please specify another UUID as the default Callnumber Type"
                ),
            )
        logging.info("Default Callnumber type Name:\t%s", self.default_call_number_type["name"])

    def setup_and_validate_holdings_types(self):
        self.holdings_types = self.folio.holdings_types
        if not self.holdings_types:
            raise TransformationProcessError("", "No holdings_types in FOLIO")
        missing_holdings_types = [
            ht
            for ht in self.holdings_type_map.values()
            if ht not in [ht_ref["name"] for ht_ref in self.holdings_types]  # type: ignore
        ]
        if any(missing_holdings_types):
            raise TransformationProcessError(
                "",
                "Holdings types are missing from the tenant. Please set them up",
                missing_holdings_types,
            )
        logging.info("%s\tholdings types", len(self.holdings_types))  # type: ignore

    def setup_reference_data_for_all(self):
        logging.info(f"{len(self.folio.class_types)}\tclass_types")  # type: ignore
        logging.info(
            f"{len(self.folio.electronic_access_relationships)}\telectronic_access_relationships"  # type: ignore
        )
        self.statistical_codes = self.folio.statistical_codes
        logging.info(f"{len(self.statistical_codes)} \tstatistical_codes")  # type: ignore

        # Raise for empty settings
        if not self.folio.class_types:
            raise TransformationProcessError("", "No class_types in FOLIO")

    def get_condition(
        self, name, legacy_id, value, parameter=None, marc_field: field.Field | None = None
    ):
        try:
            return self.condition_cache.get(name)(legacy_id, value, parameter, marc_field)  # type: ignore
        # Exception should only handle the missing condition from the cache.
        # All other exceptions should propagate up
        except Exception:
            attr = getattr(self, "condition_" + str(name))
            self.condition_cache[name] = attr
            return attr(legacy_id, value, parameter, marc_field)

    def condition_trim_punctuation(self, legacy_id, value, parameter, marc_field: field.Field):
        """
        Strip leading and trailing whitespace, as well as any trailing commas or periods, unless
        the period is preceded by a single alpha character (eg. "John D."). Also preserves any
        trailing "-" (eg. "1981-"). This condition was introduced in Poppy.
        """
        pattern1 = re.compile(r"^(.*?)\s.[.]$")
        pattern2 = re.compile(r"^(.*?)\s.,[.]$")
        value = value.strip()
        if pattern1.match(value) or value.endswith("-"):
            return value
        elif pattern2.match(value):
            return value.rstrip(",")
        elif value.endswith(".") or value.endswith(","):
            return value[:-1]
        return value

    def condition_trim_period(self, legacy_id, value, parameter, marc_field: field.Field):
        return value.strip().rstrip(".").rstrip(",")

    def condition_trim(self, legacy_id, value, parameter, marc_field: field.Field):
        return value.strip()

    def condition_set_contributor_type_id_by_code_or_name(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        contributor_code_subfield = parameter.get("contributorCodeSubfield", "4")
        for subfield in marc_field.get_subfields(contributor_code_subfield):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            t = self.get_ref_data_tuple_by_code(
                self.folio.contributor_types, "contrib_types_c", normalized_subfield
            )
            if not t:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    (
                        f'Mapping failed for ${contributor_code_subfield} "{subfield}" '
                        f"({normalized_subfield}) "
                    ),
                )
                Helper.log_data_issue(
                    legacy_id,
                    f"Mapping failed for ${contributor_code_subfield}",
                    f'{subfield}" ({normalized_subfield}) ',
                )
            else:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    (
                        i18n.t(
                            'Contributor type code "%{code}" found for $%{code_subfield}',
                            code=t[1],
                            code_subfield=contributor_code_subfield,
                        )
                        + f' "{subfield}" ({normalized_subfield}))'
                    ),
                )
                return t[0]
        fallback_name_field = "j" if marc_field.tag in ["111", "711"] else "e"
        contributor_name_subfield = parameter.get("contributorNameSubfield", fallback_name_field)
        for subfield in marc_field.get_subfields(contributor_name_subfield):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            t = self.get_ref_data_tuple_by_name(
                self.folio.contributor_types, "contrib_types_n", normalized_subfield
            )

            if not t:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    (
                        f"Mapping failed for {marc_field.tag} ${contributor_name_subfield} "
                        f"{subfield} (Normalized: {normalized_subfield}) "
                    ),
                )
                Helper.log_data_issue(
                    legacy_id,
                    f"Mapping failed for {marc_field.tag} ${contributor_name_subfield}",
                    f'{subfield}" ({normalized_subfield}) ',
                )
            else:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    (
                        f"Contributor type name {t[1]} found for {marc_field.tag} "
                        f"${contributor_name_subfield} {normalized_subfield} ({subfield}) "
                    ),
                )
                return t[0]
        return ""

    def condition_set_holdings_type_id(self, legacy_id, value, parameter, marc_field: field.Field):
        self.mapper.migration_report.add("HoldingsTypeMapping", i18n.t("Condition in rules hit"))
        return ""

    def condition_concat_subfields_by_name(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        subfields_to_concat = parameter.get("subfieldsToConcat", [])
        subfields_to_stop_concat = parameter.get("subfieldsToStopConcat", [])
        concat_subfields = []
        subfields = marc_field.subfields
        for t in subfields:
            if t[0] in subfields_to_stop_concat:
                break
            elif t[0] in subfields_to_concat:
                concat_subfields.append(t[1])
        concat_string = " ".join(concat_subfields)
        return f"{value} {concat_string}"

    def condition_get_value_if_subfield_is_empty(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        if value.strip():
            return value.strip()
        self.mapper.migration_report.add(
            "AddedValueFromParameter",
            i18n.t(
                "Tag: %{tag}. Added value: %{value}", tag=marc_field.tag, value=parameter["value"]
            ),
        )
        return parameter["value"]

    def condition_remove_ending_punc(self, legacy_id, value, parameter, marc_field: field.Field):
        v = value
        chars = ".;:,/+=- "
        while any(v) > 0 and v[-1] in chars:
            v = v.rstrip(v[-1])
        return v

    def condition_set_instance_format_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        # This method only handles the simple case of 2-character codes of RDA in the first 338$b
        # Other cases are handled in performAddidtionalParsing in the mapper class
        try:
            t = self.get_ref_data_tuple_by_code(
                self.folio.instance_formats, "instance_formats_code", value
            )
            self.mapper.migration_report.add(
                "InstanceFormat",
                i18n.t("Successful match") + f'  - "{value}"->{t[1]}',
            )
            return t[0]
        except Exception:
            self.mapper.migration_report.add(
                "InstanceFormat",
                i18n.t('Code from 338$b NOT found in FOLIO: "%{value}"', value=value),
            )

            return ""

    def condition_remove_prefix_by_indicator(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        """Returns the index title according to the rules"""
        ind2 = marc_field.indicator2
        reg_str = r"[\s:\/]{0,3}$"
        if ind2 not in map(str, range(1, 9)):
            return re.sub(reg_str, "", value)

        num_take = int(ind2)
        return re.sub(reg_str, "", value[num_take:])

    def condition_capitalize(self, legacy_id, value, parameter, marc_field: field.Field):
        return value.capitalize()

    def condition_clean_isbn(self, legacy_id, value, parameter, marc_field: field.Field):
        return value

    def condition_set_issuance_mode_id(self, legacy_id, value, parameter, marc_field: field.Field):
        # mode of issuance is handled elsewhere in the mapping.
        return ""

    def condition_set_publisher_role(self, legacy_id, value, parameter, marc_field: field.Field):
        roles = {
            "0": "Production",
            "1": "Publication",
            "2": "Distribution",
            "3": "Manufacture",
            "4": "Copyright notice date",
        }
        role = roles.get(marc_field.indicator2, "")
        self.mapper.migration_report.add(
            "MappedPublisherRoleFromIndicator2",
            f"{marc_field.tag} ind2 {marc_field.indicator2}->{role}",
        )
        return role

    def condition_set_identifier_type_id_by_value(
        self, legacy_id, value, parameter, marc_field: field.Field
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
            self.mapper.migration_report.add(
                "MappedIdentifierTypes", f"{marc_field.tag} -> {t[1]}"
            )
            return t[0]
        identifier_type: dict = next(
            (
                f
                for f in self.folio.identifier_types  # type: ignore
                if (
                    f["name"] in parameter.get("names", "non existant")
                    or f["name"] in parameter.get("name", "non existant")
                )
            ),
            None,
        )
        self.mapper.migration_report.add("MappedIdentifierTypes", identifier_type["name"])
        my_id = identifier_type["id"]
        if not my_id:
            raise TransformationFieldMappingError(
                legacy_id,
                i18n.t("no matching identifier_types in %{names}", names=parameter["names"]),
                marc_field,
            )
        return my_id

    def condition_set_holding_note_type_id_by_name(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        self.mapper.migration_report.add(
            "Exceptions",
            (
                "Condition set_holding_note_type_id_by_name is deprecated. "
                "Use set_holdings_note_type_id instead"
            ),
        )
        return self.condition_set_holdings_note_type_id(legacy_id, value, parameter, marc_field)

    def condition_set_holdings_note_type_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.holding_note_types, "holding_note_types", parameter["name"]
            )
            self.mapper.migration_report.add("MappedNoteTypes", t[1])
            return t[0]
        except Exception as ee:
            logging.error(ee)
            raise TransformationRecordFailedError(
                legacy_id,
                f"Holdings note type mapping error.\tParameter: {parameter.get('name', '')}\t"
                f"MARC Field: {marc_field}. Is mapping rules and ref data aligned?",
                parameter.get("name", ""),
            ) from ee

    def condition_set_classification_type_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.class_types, "class_types", parameter["name"]
            )
            self.mapper.migration_report.add("MappedClassificationTypes", t[1])
            return t[0]
        except Exception as e:
            raise TransformationRecordFailedError(
                legacy_id,
                f'Classification mapping error.\tParameter: "{parameter.get("name", "")}"\t'
                f"MARC Field: {marc_field}. Is mapping rules and ref data aligned?",
                parameter.get("name", ""),
            ) from e

    def condition_char_select(self, legacy_id, value, parameter, marc_field: field.Field):
        return value[parameter["from"] : parameter["to"]]

    def condition_set_identifier_type_id_by_name(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.identifier_types, "identifier_types", parameter["name"]
            )
            self.mapper.migration_report.add(
                "MappedIdentifierTypes", f"{marc_field.tag} -> {t[1]}"
            )
            return t[0]
        except Exception as ee:
            logging.exception("Identifier type")
            raise TransformationProcessError(
                legacy_id,
                f'Unmapped identifier type : "{parameter["name"]}"\tMARC Field: {marc_field}'
                f"MARC Field: {marc_field}. Is mapping rules and ref data aligned? ",
                {parameter["name"]},
            ) from ee

    def condition_set_contributor_name_type_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.contrib_name_types, "contrib_name_types", parameter["name"]
            )
            self.mapper.migration_report.add(
                "MappedContributorNameTypes", f"{marc_field.tag} -> {t[1]}"
            )
            return t[0]
        except Exception:
            self.mapper.migration_report.add("UnmappedContributorNameTypes", parameter["name"])
            return self.default_contributor_name_type

    def condition_set_note_type_id(self, legacy_id, value, parameter, marc_field: field.Field):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.instance_note_types, "instance_not_types", parameter["name"]
            )
            self.mapper.migration_report.add(
                "MappedNoteTypes",
                f"{marc_field.tag} ({parameter.get('name', '')}) -> {t[1]}",
            )
            return t[0]
        except Exception as e:
            raise ValueError(f"Instance note type not found for {marc_field} {parameter}") from e

    def condition_set_contributor_type_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        for subfield in marc_field.get_subfields("4"):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            t = self.get_ref_data_tuple_by_code(
                self.folio.contributor_types, "contrib_types_c", normalized_subfield
            )
            if not t:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    i18n.t(
                        'Mapping failed for %{tag} "%{subfield}" (%{normalized_subfield})',
                        tag="$4",
                        subfield=subfield,
                        normalized_subfield=normalized_subfield,
                    ),
                )
                Helper.log_data_issue(
                    legacy_id,
                    "Mapping failed for $4",
                    f'{subfield}" ({normalized_subfield}) ',
                )
            else:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    i18n.t(
                        'Contributor type code "%{code}" found for $%{code_subfield}',
                        code=t[1],
                        code_subfield="4",
                        normalized_subfield=normalized_subfield,
                    )
                    + f' "%{subfield}" (%{normalized_subfield}))',
                )
                return t[0]
        subfield_code = "j" if marc_field.tag in ["111", "711"] else "e"
        for subfield in marc_field.get_subfields(subfield_code):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            t = self.get_ref_data_tuple_by_name(
                self.folio.contributor_types, "contrib_types_n", normalized_subfield
            )

            if not t:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    i18n.t(
                        'Mapping failed for %{tag} "%{subfield}" '
                        "(Normalized: %{normalized_subfield})",
                        tag=f"{marc_field.tag} $e",
                        subfield=subfield,
                        normalized_subfield=normalized_subfield,
                    ),
                )
                Helper.log_data_issue(
                    legacy_id,
                    f"Mapping failed for {marc_field.tag} $e",
                    f'{subfield}" ({normalized_subfield}) ',
                )
            else:
                self.mapper.migration_report.add(
                    "ContributorTypeMapping",
                    i18n.t(
                        "Contributor type name %{name} found for %{tag}",
                        name=t[1],
                        tag=marc_field.tag,
                    )
                    + f" $e {normalized_subfield} ({subfield}) ",
                )
                return t[0]
        return self.default_contributor_type["id"]

    def condition_set_url_relationship(self, legacy_id, value, parameter, marc_field: field.Field):
        return self._extracted_from_condition_set_electronic_access_relations_id_2("8", marc_field)

    def condition_set_call_number_type_by_indicator(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        self.mapper.migration_report.add(
            "Exceptions",
            (
                "Condition set_call_number_type_by_indicator is deprecated. "
                "Change to set_call_number_type_id"
            ),
        )
        return self.condition_set_call_number_type_id(legacy_id, value, parameter, marc_field)

    def condition_set_call_number_type_id(
        self, legacy_id, value, parameter, marc_field: field.Field
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
            self.mapper.migration_report.add(
                "CallNumberTypeMapping",
                i18n.t("Unhandled call number type in $2 (ind1 == 7)" + str(marc_field["2"])),
            )
            return self.default_call_number_type["id"]

        # Normal way. Type in ind1
        call_number_type_name_temp = first_level_map.get(marc_field.indicator1, "")
        if not call_number_type_name_temp:
            self.mapper.migration_report.add(
                "CallNumberTypeMapping",
                (
                    i18n.t(
                        'Unhandled call number type in ind1: "%{ind1}".\n Returning default '
                        "Callnumber type: %{type}",
                        ind1=marc_field.indicator1,
                        type=self.default_call_number_type["name"],
                    )
                ),
            )
            return self.default_call_number_type["id"]
        t = self.get_ref_data_tuple_by_name(
            self.folio.call_number_types, "cnt", call_number_type_name_temp
        )
        if t:
            self.mapper.migration_report.add(
                "CallNumberTypeMapping",
                i18n.t("Mapped from Indicator 1") + f" {marc_field.indicator1} -> {t[1]}",
            )
            return t[0]

        self.mapper.migration_report.add(
            "CallNumberTypeMapping",
            (
                "Mapping failed. Setting default CallNumber type: "
                f"{self.default_call_number_type['name']}"
            ),
        )

        return self.default_call_number_type["id"]

    def condition_set_contributor_type_text(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        for subfield in marc_field.get_subfields("4", "e"):
            normalized_subfield = re.sub(r"[^A-Za-z0-9 ]+", "", subfield.strip())
            for cont_type in self.folio.contributor_types:  # type: ignore
                if normalized_subfield in [cont_type["code"], cont_type["name"]]:
                    return cont_type["name"]
        try:
            return value
        except IndexError:
            logging.debug("Exception occurred: %s", traceback.format_exc())
            return ""

    def condition_set_alternative_title_type_id(self, legacy_id, value, parameter, marc_field):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.alt_title_types, "alt_title_types", parameter["name"]
            )
            self.mapper.migration_report.add("MappedAlternativeTitleTypes", t[1])
            return t[0]
        except Exception as e:
            raise TransformationProcessError(
                legacy_id,
                f"Alternative title type not found for {parameter['name']} {marc_field}",
            ) from e

    def condition_set_location_id_by_code(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        self.mapper.migration_report.add(
            "Exceptions",
            (
                "set_location_id_by_code condition used in rules. "
                "Deprecated condition. Switch to set_permanent_location_id"
            ),
        )
        return self.condition_set_permanent_location_id(legacy_id, value, parameter, marc_field)

    def condition_set_permanent_location_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        if "legacy_locations" not in self.ref_data_dicts:
            try:
                d = {lm["legacy_code"]: lm["folio_code"] for lm in self.mapper.location_map}  # type: ignore
                self.ref_data_dicts["legacy_locations"] = d
                for folio_code in d.values():
                    t = self.get_ref_data_tuple_by_code(
                        self.folio.locations, "locations", folio_code
                    )
                    if not t:
                        raise TransformationProcessError(
                            "", "No FOLIO location found for code", folio_code
                        )
                if "*" not in d:
                    raise TransformationProcessError(
                        "",
                        (
                            "Fallback location mapping missing. Add a row with a * in the "
                            "legacy_code column and a location code to map unmapped locations to"
                        ),
                        "",
                    )
            except KeyError as ke:
                if "folio_code" in str(ke):
                    raise TransformationProcessError(
                        legacy_id, "Your location map lacks the column folio_code"
                    ) from ke
                if "legacy_code" in str(ke):
                    raise TransformationProcessError(
                        legacy_id, "Your location map lacks the column legacy_code"
                    ) from ke

        # Get the right code from the location map
        mapped_code = self.ref_data_dicts["legacy_locations"].get(value.strip(), "").strip()
        if not mapped_code:
            mapped_code = self.ref_data_dicts["legacy_locations"].get("*", "").strip()
            if mapped_code:
                self.mapper.migration_report.add(
                    "LocationMapping", i18n.t("Fallback mapping") + f": {value}->{mapped_code}"
                )
        # Get the FOLIO UUID for the code and return it
        t = self.get_ref_data_tuple_by_code(self.folio.locations, "locations", mapped_code)
        if not t:
            self.mapper.migration_report.add(
                "LocationMapping", i18n.t("Unmapped code") + f": '{value}'"
            )
            raise TransformationRecordFailedError(
                legacy_id, "Could not map location from legacy code", value
            )
        self.mapper.migration_report.add("LocationMapping", f"'{value}' ({mapped_code}) -> {t[1]}")
        return t[0]

    def get_ref_data_tuple_by_code(self, ref_data, ref_name, code):
        return self.get_ref_data_tuple(ref_data, ref_name, code, "code")

    def get_ref_data_tuple_by_name(self, ref_data, ref_name, name):
        return self.get_ref_data_tuple(ref_data, ref_name, name, "name")

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        # Try get the object from the cache
        ref_object = self.ref_data_dicts.get(dict_key, {}).get(key_value.lower(), ())
        if ref_object:
            return ref_object

        # No cache, we need to add it to the cache.
        d = {r[key_type].lower(): (r["id"], r["name"]) for r in ref_data}
        self.ref_data_dicts[dict_key] = d
        return self.ref_data_dicts.get(dict_key, {}).get(key_value.lower(), ())

    def condition_remove_substring(self, legacy_id, value, parameter, marc_field: field.Field):
        return value.replace(parameter["substring"], "")

    def condition_set_instance_type_id(self, legacy_id, value, parameter, marc_field: field.Field):
        if marc_field.tag not in ["008", "336"]:
            self.mapper.migration_report.add(
                "InstanceTypeMapping",
                (
                    f"Unhandled MARC tag {marc_field.tag}. Instance Type ID is only mapped "
                    "from 336 "
                ),
            )
        return ""  # functionality moved

    def condition_set_electronic_access_relations_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        """
        This method handles the mapping of electronic access relationship IDs.
        If the record type being mapped is FOLIO holdings, it provides an (optional) alternative
        mapping based on a provided name parameter, bypassing the FOLIO MARC-to-Holdings mapping
        engine behavior. This requires use of a supplemental mapping rules file in the
        HoldingsMarcTransformer task definition containing the name parameter.
        """
        if self.object_type == "holdings" and "name" in parameter:
            try:
                t = self.get_ref_data_tuple_by_name(
                    self.folio.electronic_access_relationships,
                    "electronic_access_relationships",
                    parameter["name"],
                )
                self.mapper.migration_report.add("MappedElectronicRelationshipTypes", t[1])
                return t[0]
            except Exception as e:
                raise TransformationProcessError(
                    legacy_id,
                    f"Electronic access relationship not found for {parameter['name']} {marc_field}",  # noqa: E501
                ) from e
        return self._extracted_from_condition_set_electronic_access_relations_id_2("3", marc_field)

    # TODO Rename this here and in `condition_set_url_relationship` and `condition_set_electronic_access_relations_id` # noqa: E501
    def _extracted_from_condition_set_electronic_access_relations_id_2(self, arg0, marc_field):
        enum = {
            "0": "resource",
            "1": "version of resource",
            "2": "related resource",
            arg0: "no information provided",
        }

        ind2 = marc_field.indicator2
        name = enum.get(ind2, enum[arg0])
        if not self.folio.electronic_access_relationships:
            raise ValueError("No electronic_access_relationships setup in tenant")
        t = self.get_ref_data_tuple_by_name(
            self.folio.electronic_access_relationships,
            "electronic_access_relationships",
            name,
        )

        self.mapper.migration_report.add("MappedElectronicRelationshipTypes", t[1])

        return t[0]

    def condition_set_note_staff_only_via_indicator(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        """Returns true of false depending on the first indicator"""
        # https://www.loc.gov/marc/bibliographic/bd541.html
        ind1 = marc_field.indicator1
        self.mapper.migration_report.add(
            "StaffOnlyViaIndicator",
            f"{marc_field.tag} indicator1: {ind1} ("
            + i18n.t("0 is staff-only, all other values are public")
            + ")",
        )
        if ind1 == "0":
            return "true"
        return "false"

    def condition_set_subject_type_id(self, legacy_id, value, parameter, marc_field: field.Field):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.subject_types, "subject_types", parameter["name"]
            )
            self.mapper.migration_report.add("MappedSubjectTypes", t[1])
            return t[0]
        except Exception as e:
            raise TransformationProcessError(
                legacy_id,
                f"Subject type not found for {parameter['name']} {marc_field}",
            ) from e

    def condition_set_subject_source_id(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_name(
                self.folio.folio_get_all("/subject-sources", "subjectSources"),
                "subject_sources",
                parameter["name"],
            )
            self.mapper.migration_report.add("MappedSubjectSources", t[1])
            return t[0]
        except Exception as e:
            raise TransformationProcessError(
                legacy_id,
                f"Subject source not found for {parameter['name']} {marc_field}",
            ) from e

    def condition_set_subject_source_id_by_code(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        try:
            t = self.get_ref_data_tuple_by_code(
                self.folio.folio_get_all("/subject-sources", "subjectSources"),
                "subject_sources",
                value,
            )
            self.mapper.migration_report.add("MappedSubjectSources", t[1])
            return t[0]
        except Exception as e:
            raise TransformationProcessError(
                legacy_id,
                f"Subject source not found for {value} {marc_field}",
            ) from e

    def condition_set_receipt_status(self, legacy_id, value, parameter, marc_field: field.Field):
        """
        This method maps the receipt status based on the 008 field.
        This condition is not available in FOLIO's MARC mapping engine and
        will require use of a supplemental mapping rules file in the
        HoldingsMarcTransformer task definition.
        """
        if len(value) < 7:
            self.mapper.migration_report.add(
                "ReceiptStatusMapping", i18n.t("008 is too short") + f": {value}"
            )
            return ""

        status_map = {
            "0": "Unknown",
            "1": "Other receipt or acquisition status",
            "2": "Received and complete or ceased",
            "3": "On order",
            "4": "Currently received",
            "5": "Not currently received",
            "6": "External access",
        }

        try:
            mapped_value = status_map[value[6]]
            self.mapper.migration_report.add(
                "ReceiptStatusMapping",
                i18n.t(
                    "%{value} mapped to %{mapped_value}",
                    value=value[6],
                    mapped_value=mapped_value,
                ),
            )
            return mapped_value
        except Exception:
            self.mapper.migration_report.add(
                "ReceiptStatusMapping", i18n.t("%{value} not found in map.", value=value[6])
            )
            return ""

    def condition_set_acquisition_method(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        """
        This method maps the acquisition method based on the 008 field.
        This condition is not available in FOLIO's MARC mapping engine and
        will require use of a supplemental mapping rules file in the
        HoldingsMarcTransformer task definition.
        """
        if len(value) < 8:
            self.mapper.migration_report.add(
                "ReceiptStatusMapping", i18n.t("008 is too short") + f": {value}"
            )
            return ""

        try:
            acq_methods = {
                "c": "Cooperative or consortial purchase",
                "d": "Deposit",
                "e": "Exchange",
                "f": "Free",
                "g": "Gift",
                "l": "Legal deposit",
                "m": "Membership",
                "n": "Non-library purchase",
                "p": "Purchase",
                "q": "Lease",
                "u": "Unknown",
                "z": "Other method of acquisition",
            }
            mapped_value = acq_methods[value[7]]
            self.mapper.migration_report.add(
                "MethodOfAcquisitionMapping",
                i18n.t(
                    "%{value} mapped to %{mapped_value}", value=value[7], mapped_value=mapped_value
                ),
            )
            return mapped_value
        except Exception:
            self.mapper.migration_report.add(
                "MethodOfAcquisitionMapping", i18n.t("%{value} not found in map.", value=value[8])
            )
            return ""

    def condition_set_retention_policy(self, legacy_id, value, parameter, marc_field: field.Field):
        """
        This method maps the retention policy based on the 008 field.
        This condition is not available in FOLIO's MARC mapping engine and
        will require use of a supplemental mapping rules file in the
        HoldingsMarcTransformer task definition.
        """
        if len(value) < 13:
            self.mapper.migration_report.add(
                "RetentionPolicyMapping", i18n.t("008 is too short") + f": {value}"
            )
            return ""
        value = value.replace("|", " ").replace(
            "#", " "
        )  # Replace pipe with space for mapping consistency
        try:
            retention_policies = {
                "0": "Unknown",
                "1": "Other general retention policy",
                "2": "Retained except as replaced by updates",
                "3": "Sample issue retained",
                "4": "Retained until replaced by microform",
                "5": "Retained until replaced by cumulation, replacement volume, or revision",
                "6": "Retained for a limited period",
                "7": "Not retained",
                "8": "Permanently retained",
            }
            mapped_value = retention_policies[value[12]]
            self.mapper.migration_report.add(
                "RetentionPolicyMapping",
                i18n.t(
                    "%{value} mapped to %{mapped_value}",
                    value=value[12],
                    mapped_value=mapped_value,
                ),
            )
            if value[12] == "6":
                policy_types = {
                    "l": "Latest",
                    "p": "Previous",
                }
                unit_types = {
                    "m": "Day",
                    "w": "Month",
                    "y": "Year",
                    "e": "Edition",
                    "i": "Issue",
                    "s": "Supplement",
                }
                try:
                    specific_retention_policy = ""
                    if value[13].strip() or value[15].strip():
                        if value[14].strip() and int(value[14]) > 1:
                            specific_retention_policy = f"{policy_types.get(value[13], '')} {value[14]} {unit_types.get(value[15], '')}s retained".strip()  # noqa: E501
                        else:
                            specific_retention_policy = f"{policy_types.get(value[13], '')} {unit_types.get(value[15], '')} retained".strip()  # noqa: E501
                    if specific_retention_policy:
                        self.mapper.migration_report.add(
                            "RetentionPolicyMapping",
                            i18n.t(
                                "Retention policy 6 indicates a limited period. Specific "
                                "retention period will be mapped from 008/13-15",
                            ),
                        )
                        return specific_retention_policy
                    else:
                        raise ValueError(
                            "Specific retention policy is empty or invalid in 008/13-15"
                        )
                except ValueError:
                    self.mapper.migration_report.add(
                        "RetentionPolicyMapping",
                        i18n.t(
                            "Invalid specific retention policy in 008/13-15: %{value}",
                            value=value[13:16],
                        ),
                    )
            return mapped_value
        except Exception:
            self.mapper.migration_report.add(
                "RetentionPolicyMapping", i18n.t("%{value} not found in map.", value=value[12])
            )
            return ""

    def condition_set_ill_policy(self, legacy_id, value, parameter, marc_field: field.Field):
        """
        This method maps the ILL policy based on the 008 field.
        This condition is not available in FOLIO's MARC mapping engine and
        will require use of a supplemental mapping rules file in the
        HoldingsMarcTransformer task definition."""
        if len(value) < 21:
            self.mapper.migration_report.add(
                "ILLPolicyMapping", i18n.t("008 is too short") + f": {value}"
            )
            return ""
        try:
            ill_policies = {
                "a": "Will lend",
                "b": "Will not lend",
                "c": "Will lend hard copy only",
                "l": "Limited lending policy",
                "u": "Unknown",
            }
            mapped_value = ill_policies[value[20]]
            self.mapper.migration_report.add(
                "ILLPolicyMapping",
                i18n.t(
                    "%{value} mapped to %{mapped_value}",
                    value=value[20],
                    mapped_value=mapped_value,
                ),
            )
            ill_policy_id = self.get_ref_data_tuple_by_name(
                self.ill_policies, "ill_policies", mapped_value
            )
            return ill_policy_id[0] if ill_policy_id else ""
        except Exception:
            self.mapper.migration_report.add(
                "ILLPolicyMapping", i18n.t("%{value} not found in map.", value=value[20])
            )
            return ""

    def condition_set_digitization_policy(
        self, legacy_id, value, parameter, marc_field: field.Field
    ):
        """
        This method maps the digitization policy based on the 008 field.
        This condition is not available in FOLIO's MARC mapping engine and
        will require use of a supplemental mapping rules file in the
        HoldingsMarcTransformer task definition.
        """
        if len(value) < 22:
            self.mapper.migration_report.add(
                "DigitizationPolicyMapping", i18n.t("008 is too short") + f": {value}"
            )
            return ""
        try:
            digitization_policies = {
                "a": "Will reproduce",
                "b": "Will not reproduce",
                "u": "Unknown",
            }
            mapped_value = digitization_policies[value[21]]
            self.mapper.migration_report.add(
                "DigitizationPolicyMapping",
                i18n.t(
                    "%{value} mapped to %{mapped_value}",
                    value=value[21],
                    mapped_value=mapped_value,
                ),
            )
            return mapped_value
        except Exception:
            self.mapper.migration_report.add(
                "DigitizationPolicyMapping", i18n.t("%{value} not found in map.", value=value[21])
            )
            return ""
