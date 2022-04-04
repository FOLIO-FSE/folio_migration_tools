"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import json
import logging
import sys
import time
import typing
import uuid
import xml.etree.ElementTree as ET
from typing import Generator, List
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID

import pymarc
import requests
from folioclient import FolioClient
from folio_migration_tools.custom_exceptions import (
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import (
    FolioRelease,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)

from folio_migration_tools.report_blurbs import Blurbs
from pymarc import Field
from pymarc.record import Record


class BibsRulesMapper(RulesMapperBase):
    """Maps a MARC record to inventory instance format according to
    the FOLIO community convention"""

    def __init__(
        self,
        folio_client,
        library_configuration: LibraryConfiguration,
        task_configuration,
    ):
        super().__init__(
            folio_client,
            library_configuration,
            Conditions(folio_client, self, "bibs"),
        )
        self.folio = folio_client
        self.task_configuration = task_configuration
        self.record_status = {}
        self.unique_001s = set()
        self.holdings_map = {}
        self.id_map = {}
        self.srs_recs = []
        self.schema = self.instance_json_schema
        self.contrib_name_types = {}
        self.mapped_folio_fields = {}
        self.unmapped_folio_fields = {}
        self.alt_title_map = {}
        logging.info(
            f"HRID handling is set to: '{self.task_configuration.hrid_handling}'"
        )
        self.hrid_handling: HridHandling = self.task_configuration.hrid_handling
        logging.info("Fetching mapping rules from the tenant")
        rules_endpoint = (
            "/mapping-rules"
            if self.library_configuration.folio_release == FolioRelease.juniper
            else "/mapping-rules/marc-bib"
        )
        self.mappings = self.folio.folio_get_single_object(rules_endpoint)
        logging.info("Fetching valid language codes...")
        self.language_codes = list(self.fetch_language_codes())
        self.unmapped_tags = {}
        self.unmapped_conditions = {}
        self.instance_relationships = {}
        self.instance_relationship_types = {}
        self.other_mode_of_issuance_id = get_unspecified_mode_of_issuance(self.folio)
        self.start = time.time()

    def perform_initial_preparation(self, marc_record: pymarc.Record, legacy_ids):
        folio_instance = {
            "metadata": self.folio.get_metadata_construct(),
        }
        folio_instance["id"] = str(
            FolioUUID(
                str(self.folio_client.okapi_url),
                FOLIONamespaces.instances,
                str(legacy_ids[-1]),
            )
        )
        leader_05 = marc_record.leader[5]
        self.migration_report.add(Blurbs.RecordStatus, leader_05 or "Empty")
        self.handle_hrid(folio_instance, marc_record, legacy_ids)
        if leader_05 == "d":
            Helper.log_data_issue(
                legacy_ids, "d in leader. Is this correct?", marc_record.leader
            )
        return folio_instance

    def parse_bib(self, legacy_ids, marc_record: pymarc.Record, suppressed: bool):
        """Parses a bib recod into a FOLIO Inventory instance object
        Community mapping suggestion: https://bit.ly/2S7Gyp3
         This is the main function"""
        self.print_progress()
        ignored_subsequent_fields = set()
        bad_tags = set(self.task_configuration.tags_to_delete)  # "907"
        folio_instance = self.perform_initial_preparation(marc_record, legacy_ids)
        for marc_field in marc_record:
            self.report_marc_stats(
                marc_field, bad_tags, legacy_ids, ignored_subsequent_fields
            )
            if marc_field.tag not in ignored_subsequent_fields:
                self.process_marc_field(
                    folio_instance,
                    marc_field,
                    ignored_subsequent_fields,
                    legacy_ids,
                )

        self.perform_additional_parsing(
            folio_instance, marc_record, legacy_ids, suppressed
        )
        clean_folio_instance = self.validate_required_properties(
            "-".join(legacy_ids), folio_instance, self.schema, FOLIONamespaces.instances
        )
        self.dedupe_rec(clean_folio_instance)
        marc_record.remove_fields(*list(bad_tags))
        self.report_folio_mapping(clean_folio_instance, self.instance_json_schema)
        if clean_folio_instance["discoverySuppress"]:
            self.migration_report.add_general_statistics("Suppressed from discovery")
        # TODO: trim away multiple whitespace and newlines..
        # TODO: createDate and update date and catalogeddate
        return clean_folio_instance

    def process_marc_field(
        self,
        folio_instance,
        marc_field,
        ignored_subsequent_fields,
        legacy_ids,
    ):
        if marc_field.tag == "880" and "6" in marc_field:
            mappings = self.perform_proxy_mapping(marc_field)
        else:
            tags_to_ignore = {"880", "001", "008"}
            mappings = (
                self.mappings.get(marc_field.tag, {})
                if marc_field.tag not in tags_to_ignore
                else []
            )
        if mappings:
            try:
                self.map_field_according_to_mapping(
                    marc_field, mappings, folio_instance, legacy_ids
                )
                if any(m.get("ignoreSubsequentFields", False) for m in mappings):
                    ignored_subsequent_fields.add(marc_field.tag)
            except Exception as ee:
                logging.error(
                    f"map_field_according_to_mapping {marc_field.tag} {marc_field.format_field()} {json.dumps(mappings)}"
                )
                raise ee

    def report_marc_stats(
        self, marc_field, bad_tags, legacy_ids, ignored_subsequent_fields
    ):
        self.migration_report.add_general_statistics("Total number of Tags processed")
        self.report_bad_tags(marc_field, bad_tags, legacy_ids)
        mapped = marc_field.tag in self.mappings
        if marc_field.tag in ignored_subsequent_fields:
            mapped = False
        self.report_legacy_mapping(marc_field.tag, True, mapped)

    def perform_proxy_mapping(self, marc_field):
        proxy_mapping = next(iter(self.mappings.get("880", [])), [])
        if proxy_mapping and "fieldReplacementRule" in proxy_mapping:
            target_field = next(
                (
                    r["targetField"]
                    for r in proxy_mapping["fieldReplacementRule"]
                    if r["sourceDigits"] == marc_field["6"][:3]
                ),
                "",
            )
            mappings = self.mappings.get(target_field, {})

            self.migration_report.add(
                Blurbs.Field880Mappings,
                f"Source digits: {marc_field['6'][:3]} Target field: {target_field}",
            )
        else:
            raise TransformationProcessError(
                "", "Mapping rules for 880 is missing. Halting"
            )
        return mappings

    def perform_additional_parsing(
        self,
        folio_instance: dict,
        marc_record: Record,
        legacy_ids: List[str],
        suppressed: bool,
    ):
        """Do stuff not easily captured by the mapping rules"""
        folio_instance["source"] = "MARC"
        folio_instance["instanceFormatIds"] = list(
            set(self.get_instance_format_ids(marc_record, legacy_ids))
        )

        folio_instance["instanceTypeId"] = self.get_instance_type_id(
            marc_record, legacy_ids
        )

        folio_instance["modeOfIssuanceId"] = self.get_mode_of_issuance_id(
            marc_record, legacy_ids
        )
        if "languages" in folio_instance:
            folio_instance["languages"].extend(
                self.get_languages(marc_record, legacy_ids)
            )
        else:
            folio_instance["languages"] = self.get_languages(marc_record, legacy_ids)
        folio_instance["languages"] = list(
            self.filter_langs(folio_instance["languages"], marc_record, legacy_ids)
        )
        folio_instance["discoverySuppress"] = suppressed
        folio_instance["staffSuppress"] = False
        self.handle_holdings(marc_record)

    def handle_holdings(self, marc_record: Record):
        if "852" in marc_record:
            holdingsfields = marc_record.get_fields(
                "852", "866", "867", "868", "865", "864", "863"
            )
            f852s = (f for f in holdingsfields if f.tag == "852")
            f86xs = (
                f
                for f in holdingsfields
                if f.tag in ["866", "867", "868", "865", "864", "863"]
            )
            if f852s and not f86xs:
                self.migration_report.add(
                    Blurbs.HoldingsGenerationFromBibs,
                    "Records with 852s but no 86X",
                )
            elif any(f852s):
                self.migration_report.add(
                    Blurbs.HoldingsGenerationFromBibs,
                    "Records with both 852s and at least one 86X",
                )

            elif any(f86xs):
                self.migration_report.add(
                    Blurbs.HoldingsGenerationFromBibs,
                    "Records without 852s but with 86X",
                )

    def wrap_up(self):
        logging.info("Mapper wrapping up")
        self.store_hrid_settings()

    def report_bad_tags(self, marc_field, bad_tags, legacy_ids):
        if (
            (not marc_field.tag.isnumeric())
            and marc_field.tag != "LDR"
            and marc_field.tag not in bad_tags
        ):
            self.migration_report.add(Blurbs.NonNumericTagsInRecord, marc_field.tag)
            message = "Non-numeric tags in records"
            Helper.log_data_issue(legacy_ids, message, marc_field.tag)
            bad_tags.add(marc_field.tag)

    def get_instance_type_id(self, marc_record, legacy_id):
        return_id = ""

        def get_folio_id_by_name(f336a: str):
            match_template = f336a.lower().replace(" ", "")
            match = next(
                (
                    f["id"]
                    for f in self.folio.instance_types
                    if f["name"].lower().replace(" ", "") == match_template
                ),
                "",
            )
            if match:
                self.migration_report.add(
                    Blurbs.RecourceTypeMapping,
                    f"336$a - Successful matching on  {match_template} ({f336a})",
                )
            else:
                self.migration_report.add(
                    Blurbs.RecourceTypeMapping,
                    f"336$a - Unsuccessful matching on  {match_template} ({f336a})",
                )
                Helper.log_data_issue(
                    legacy_id,
                    "instance type name (336$a) -Unsuccessful matching",
                    f336a,
                )
            return match

        if not self.folio.instance_types:
            raise TransformationProcessError("", "No instance_types setup in tenant")

        if "336" in marc_record and "b" not in marc_record["336"]:
            self.migration_report.add(
                Blurbs.RecourceTypeMapping, "Subfield b not in 336"
            )
            Helper.log_data_issue(
                legacy_id,
                "Subfield b not in 336",
                "",
            )
            if "a" in marc_record["336"]:
                return_id = get_folio_id_by_name(marc_record["336"]["a"])

        if "336" in marc_record and "b" in marc_record["336"]:
            f336_b = marc_record["336"]["b"].lower().replace(" ", "")
            f336_b_norm = f336_b.lower().replace(" ", "")
            t = self.conditions.get_ref_data_tuple_by_code(
                self.folio.instance_types,
                "instance_types",
                f336_b_norm,
            )
            if not t:
                self.migration_report.add(
                    Blurbs.RecourceTypeMapping,
                    f"336$b - Code {f336_b_norm} ('{f336_b}') not found in FOLIO ",
                )
                Helper.log_data_issue(
                    legacy_id,
                    "instance type code (336$b) not found in FOLIO",
                    f336_b,
                )
            else:
                self.migration_report.add(
                    Blurbs.RecourceTypeMapping,
                    f'336$b {t[1]} mapped from {marc_record["336"]["b"]}',
                )
                return_id = t[0]

        if not return_id:
            t = self.conditions.get_ref_data_tuple_by_code(
                self.folio.instance_types, "instance_types", "zzz"
            )
            return_id = t[0]
        return return_id

    def get_instance_format_ids(self, marc_record, legacy_id):
        # Lambdas
        def get_folio_id(code: str):
            try:
                match = next(
                    f for f in self.folio.instance_formats if f["code"] == code
                )
                self.migration_report.add(
                    Blurbs.InstanceFormat,
                    f"Successful match  - {code}->{match['name']}",
                )
                return match["id"]
            except Exception:
                # TODO: Distinguish between generated codes and proper 338bs
                Helper.log_data_issue(
                    legacy_id, "Instance format Code not found in FOLIO", code
                )
                self.migration_report.add(
                    Blurbs.InstanceFormat,
                    f"Code '{code}' not found in FOLIO",
                )
                return ""

        def get_folio_id_by_name(f337a: str, f338a: str, legacy_id: str):
            f337a = f337a.lower().replace(" ", "")
            f338a = f338a.lower().replace(" ", "")
            match_template = f"{f337a} -- {f338a}"
            try:
                match = next(
                    f
                    for f in self.folio.instance_formats
                    if f["name"].lower().replace(" ", "") == match_template
                )
                self.migration_report.add(
                    Blurbs.InstanceFormat,
                    f"Successful matching on 337$a & 338$a - {match_template}->{match['name']}",
                )
                return match["id"]
            except Exception:
                Helper.log_data_issue(
                    legacy_id,
                    "Unsuccessful matching on 337$a and 338$a",
                    match_template,
                )
                self.migration_report.add(
                    Blurbs.InstanceFormat,
                    f"Unsuccessful matching on 337$a and 338$a - {match_template}",
                )
                return ""

        all_337s = marc_record.get_fields("337")
        all_338s = marc_record.get_fields("338")
        for fidx, f in enumerate(all_338s):
            source = f["2"] if "2" in f else "Not set"
            if not source.strip().startswith("rdacarrier"):
                self.migration_report.add(
                    Blurbs.InstanceFormat,
                    (
                        "InstanceFormat not mapped since 338$2 (Source) "
                        f"is set to {source}. "
                    ),
                )
            else:
                if "b" not in f and "a" in f:
                    self.migration_report.add(
                        Blurbs.InstanceFormat,
                        "338$b is missing. Will try parse from 337$a and 338$b",
                    )
                    for sfidx, a in enumerate(f.get_subfields("a")):
                        corresponding_337 = (
                            all_337s[fidx] if fidx < len(all_337s) else None
                        )
                        if corresponding_337 and "a" in corresponding_337:
                            fmt_id = get_folio_id_by_name(
                                corresponding_337["a"], a, legacy_id
                            )
                            if fmt_id:
                                yield fmt_id

                for sfidx, b in enumerate(f.get_subfields("b")):
                    b = b.replace(" ", "")
                    if len(b) == 2:  # Normal 338b. should be able to map this
                        yield get_folio_id(b)
                    elif len(b) == 1:
                        corresponding_337 = (
                            all_337s[fidx] if fidx < len(all_337s) else None
                        )
                        if (
                            not corresponding_337
                        ):  # No matching 337. No use mapping the 338
                            s = "No corresponding 337 to 338 even though 338$b was one charachter code"
                            Helper.log_data_issue(legacy_id, s, b)
                            self.migration_report.add(
                                Blurbs.InstanceFormat,
                                s,
                            )
                        else:  # Corresponding 337. Try to combine the codes.
                            corresponding_b = (
                                corresponding_337.get_subfields("b")[sfidx]
                                if sfidx < len(corresponding_337.get_subfields("b"))
                                else None
                            )
                            if not corresponding_b:
                                s = "No corresponding $b in corresponding 338"
                                Helper.log_data_issue(legacy_id, s, "")
                                self.migration_report.add(Blurbs.InstanceFormat, s)
                            else:
                                combined_code = (corresponding_b + b).strip()
                                if len(combined_code) == 2:
                                    yield get_folio_id(combined_code)

    def handle_hrid(self, folio_instance, marc_record: Record, legacy_ids) -> None:
        """Create HRID if not mapped. Add hrid as MARC record 001"""
        if self.hrid_handling == HridHandling.default or "001" not in marc_record:
            num_part = str(self.instance_hrid_counter).zfill(11)
            folio_instance["hrid"] = f"{self.instance_hrid_prefix}{num_part}"
            new_001 = Field(tag="001", data=folio_instance["hrid"])
            try:
                f_001 = marc_record["001"].value()
                f_003 = (
                    marc_record["003"].value().strip() if "003" in marc_record else ""
                )
                self.migration_report.add(
                    Blurbs.HridHandling, f'Values in 003: {f_003 or "Empty"}'
                )

                if self.task_configuration.deactivate035_from001:
                    self.migration_report.add(
                        Blurbs.HridHandling, "035 generation from 001 turned off"
                    )
                else:
                    str_035 = f"({f_003}){f_001}" if f_003 else f"{f_001}"
                    new_035 = Field(
                        tag="035",
                        indicators=[" ", " "],
                        subfields=["a", str_035],
                    )
                    marc_record.add_ordered_field(new_035)
                    self.migration_report.add(Blurbs.HridHandling, "Added 035 from 001")
                marc_record.remove_fields("001")

            except Exception:
                if "001" in marc_record:
                    s = "Failed to create 035 from 001"
                    self.migration_report.add(Blurbs.HridHandling, s)
                    Helper.log_data_issue(legacy_ids, s, marc_record["001"])
                else:
                    self.migration_report.add(
                        Blurbs.HridHandling, "Legacy bib records without 001"
                    )
            marc_record.add_ordered_field(new_001)
            self.migration_report.add(
                Blurbs.HridHandling, "Created HRID using default settings"
            )
            self.instance_hrid_counter += 1
        elif self.hrid_handling == HridHandling.preserve001:
            value = marc_record["001"].value()
            if value in self.unique_001s:
                self.migration_report.add(
                    Blurbs.HridHandling, "Duplicate 001. Creating HRID instead"
                )
                Helper.log_data_issue(
                    legacy_ids,
                    "Duplicate 001 for record. HRID created for record",
                    value,
                )
                num_part = str(self.instance_hrid_counter).zfill(11)
                folio_instance["hrid"] = f"{self.instance_hrid_prefix}{num_part}"
                new_001 = Field(tag="001", data=folio_instance["hrid"])
                marc_record.add_ordered_field(new_001)
                self.instance_hrid_counter += 1
            else:
                self.unique_001s.add(value)
                folio_instance["hrid"] = value
                self.migration_report.add(Blurbs.HridHandling, "Took HRID from 001")
        else:
            raise TransformationProcessError(
                "", f"Unknown HRID handling: {self.hrid_handling}"
            )

    def get_mode_of_issuance_id(self, marc_record: Record, legacy_id: str) -> str:
        level = marc_record.leader[7]
        try:
            name = "unspecified"
            if level in ["a", "c", "d", "m"]:
                name = "single unit"
            if level in ["b", "s"]:
                name = "serial"
            if level == "i":
                name = "integrating resource"
            ret = next(
                (
                    i["id"]
                    for i in self.folio.modes_of_issuance
                    if str(name).lower() == i["name"].lower()
                ),
                "",
            )
            self.migration_report.add(
                Blurbs.MatchedModesOfIssuanceCode, f"{name} -- {ret}"
            )
            if not ret:
                self.migration_report.add(
                    Blurbs.MatchedModesOfIssuanceCode,
                    f"Unmatched level: {level}",
                )
                return self.other_mode_of_issuance_id
            return ret
        except IndexError:
            self.migration_report.add(
                Blurbs.PossibleCleaningTasks, f"No Leader[7] in {legacy_id}"
            )
            return self.other_mode_of_issuance_id
        except StopIteration as ee:
            logging.exception(
                f"{marc_record.leader} {list(self.folio.modes_of_issuance)}"
            )
            raise ee from ee

    def get_nature_of_content(self, marc_record: Record) -> List[str]:
        return ["81a3a0e2-b8e5-4a7a-875d-343035b4e4d7"]

    def get_languages_008(self, marc_record: Record):
        if "008" in marc_record and len(marc_record["008"].data) > 38:
            return "".join(marc_record["008"].data[35:38])
        return ""

    def get_languages_041(self, marc_record, legacy_id):
        languages = set()
        lang_fields = marc_record.get_fields("041")
        if not any(lang_fields):
            return set()
        subfields = "abdefghjkmn"
        for lang_tag in lang_fields:
            if "2" in lang_tag:
                self.migration_report.add(Blurbs.LanguageCodeSources, lang_tag["2"])
                logging.info(
                    "Field with other Language code\t%s\t%s",
                    marc_record["001"],
                    lang_tag.value(),
                )
            lang_codes = lang_tag.get_subfields(*list(subfields))
            for lang_code in lang_codes:
                lang_code = str(lang_code).lower().replace(" ", "")
                langlength = len(lang_code)
                if langlength == 3:
                    languages.add(lang_code.replace(" ", ""))
                elif langlength > 3 and langlength % 3 == 0:
                    lc = lang_code.replace(" ", "")
                    new_codes = (lc[i : i + 3] for i in range(0, len(lc), 3))
                    languages.update(new_codes)
                    languages.discard(lang_code)
            languages.update()
        languages = set(
            self.filter_langs(filter(None, languages), marc_record, legacy_id)
        )
        return languages

    def get_languages(self, marc_record: Record, legacy_id: str) -> List[str]:
        """Get languages and tranforms them to correct codes"""
        languages = self.get_languages_041(marc_record, legacy_id)
        languages.add(self.get_languages_008(marc_record))
        for lang in languages:
            self.migration_report.add(Blurbs.LanguagesInRecords, lang)
        return list(languages)

    def fetch_language_codes(self) -> Generator[str, None, None]:
        """fetches the list of standardized language codes from LoC"""
        url = "https://www.loc.gov/standards/codelists/languages.xml"
        tree = ET.fromstring(requests.get(url).content)
        name_space = "{info:lc/xmlns/codelist-v1}"
        xpath_expr = "{0}languages/{0}language/{0}code".format(name_space)
        for code in tree.findall(xpath_expr):
            yield code.text

    def filter_langs(
        self, language_values: List[str], marc_record: Record, index_or_legacy_id
    ) -> typing.Generator:
        forbidden_values = ["###", "zxx", "n/a", "N/A", "|||"]
        for language_value in language_values:
            if (
                language_value in self.language_codes
                and language_value not in forbidden_values
            ):
                yield language_value
            elif language_value == "jap":
                yield "jpn"
            elif language_value == "fra":
                yield "fre"
            elif language_value == "sve":
                yield "swe"
            elif language_value == "tys":
                yield "ger"
            elif not language_value:
                continue
            elif not language_value.strip():
                continue
            elif language_value not in forbidden_values:
                m = "Unrecognized language codes in record"
                Helper.log_data_issue(index_or_legacy_id, m, language_value)
                self.migration_report.add(
                    Blurbs.UnrecognizedLanguageCodes,
                    f"{m}: {language_value}",
                )

    def get_legacy_ids(
        self, marc_record: Record, ils_flavour: IlsFlavour, index_or_legacy_id: str
    ) -> List[str]:
        if ils_flavour in {IlsFlavour.sierra, IlsFlavour.millennium}:
            return get_iii_bib_id(marc_record)
        elif ils_flavour == IlsFlavour.tag907y:
            try:
                return list(set(marc_record["907"].get_subfields("a", "y")))
            except Exception as e:
                raise TransformationRecordFailedError(
                    index_or_legacy_id,
                    (
                        "907 $y and $a is missing is missing, although they is "
                        "required for this legacy ILS choice"
                    ),
                    marc_record.as_json(),
                ) from e
        elif ils_flavour == IlsFlavour.tagf990a:
            res = {f["a"].strip() for f in marc_record.get_fields("990") if "a" in f}
            if marc_record["001"].format_field().strip():
                res.add(marc_record["001"].format_field().strip())
            if any(res):
                self.migration_report.add_general_statistics("legacy id from 990$a")
                return list(res)
        elif ils_flavour == IlsFlavour.aleph:
            return self.get_aleph_bib_id(marc_record)
        elif ils_flavour in {IlsFlavour.voyager, "voyager", IlsFlavour.tag001}:
            try:
                return [marc_record["001"].format_field().strip()]
            except Exception as e:
                raise TransformationRecordFailedError(
                    index_or_legacy_id,
                    "001 is missing, although it is required for Voyager migrations",
                    marc_record.as_json(),
                ) from e
        elif ils_flavour == IlsFlavour.koha:
            try:
                return [marc_record["999"]["c"]]
            except Exception as e:
                raise TransformationRecordFailedError(
                    index_or_legacy_id,
                    "999 $c is missing, although it is required for this legacy ILS choice",
                    marc_record.as_json(),
                ) from e
        elif ils_flavour == IlsFlavour.none:
            return [str(uuid.uuid4())]
        else:
            raise TransformationProcessError("", f"ILS {ils_flavour} not configured")

    def get_aleph_bib_id(self, marc_record: Record):
        res = {f["b"].strip() for f in marc_record.get_fields("998") if "b" in f}
        if any(res):
            self.migration_report.add_general_statistics("legacy id from 998$b")
            return list(res)
        else:
            try:
                ret = [marc_record["001"].format_field().strip()]
                self.migration_report.add_general_statistics("legacy id from 001")
                return ret
            except Exception as e:
                raise TransformationRecordFailedError(
                    "unknown identifier",
                    "001 is missing.although that or 998$b is required for Aleph migrations",
                    marc_record.as_json(),
                ) from e


def get_unspecified_mode_of_issuance(folio_client: FolioClient):
    m_o_is = list(folio_client.modes_of_issuance)
    if not any(m_o_is):
        logging.critical("No Modes of issuance set up in tenant. Quitting...")
        sys.exit()
    if not any(i for i in m_o_is if i["name"].lower() == "unspecified"):
        logging.critical(
            "Mode of issuance 'unspecified' missing in tenant "
            "configuration. Please add this to continue. Quitting..."
        )
        sys.exit()
    return next(i["id"] for i in m_o_is if i["name"].lower() == "unspecified")


def get_iii_bib_id(marc_record: Record):
    try:
        return [marc_record["907"]["a"]]
    except Exception as e:
        raise TransformationRecordFailedError(
            "unknown identifier",
            "907 $a is missing, although it is required for Sierra/iii migrations",
            marc_record.as_json(),
        ) from e
