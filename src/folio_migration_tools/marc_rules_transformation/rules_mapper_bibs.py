"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import logging
import sys
import time
import typing
import uuid
from pathlib import Path
from typing import Generator
from typing import List

import pymarc
from defusedxml.ElementTree import fromstring
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient
from pymarc.record import Record

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.report_blurbs import Blurbs


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
            task_configuration,
            self.get_instance_schema(),
            Conditions(folio_client, self, "bibs", library_configuration.folio_release),
        )
        logging.info("Fetching mapping rules from the tenant")
        rules_endpoint = "/mapping-rules/marc-bib"
        self.mappings = self.folio_client.folio_get_single_object(rules_endpoint)
        logging.info("Fetching valid language codes...")
        self.language_codes = list(self.fetch_language_codes())
        self.instance_relationships: dict = {}
        self.instance_relationship_types: dict = {}
        self.other_mode_of_issuance_id = get_unspecified_mode_of_issuance(self.folio_client)

        self.start = time.time()

    def perform_initial_preparation(self, marc_record: pymarc.Record, legacy_ids):
        folio_instance = {
            "metadata": self.folio_client.get_metadata_construct(),
        }
        folio_instance["id"] = str(
            FolioUUID(
                str(self.folio_client.okapi_url),
                FOLIONamespaces.instances,
                str(legacy_ids[-1]),
            )
        )
        self.hrid_handler.handle_hrid(
            FOLIONamespaces.instances,
            folio_instance,
            marc_record,
            legacy_ids,
        )
        self.handle_leader_05(marc_record, legacy_ids)
        if self.task_configuration.add_administrative_notes_with_legacy_ids:
            for legacy_id in legacy_ids:
                self.add_legacy_id_to_admin_note(folio_instance, legacy_id)

        return folio_instance

    def handle_leader_05(self, marc_record, legacy_ids):
        leader_05 = marc_record.leader[5] or "Empty"
        self.migration_report.add(Blurbs.RecordStatus, f"Original value: {leader_05}")
        if leader_05 not in ["a", "c", "d", "n", "p"]:
            marc_record.leader = f"{marc_record.leader[:5]}c{marc_record.leader[6:]}"
            self.migration_report.add(Blurbs.RecordStatus, f"Changed {leader_05} to c")
        if leader_05 == "d":
            Helper.log_data_issue(legacy_ids, "d in leader. Is this correct?", marc_record.leader)

    def parse_record(
        self, marc_record: pymarc.Record, file_def: FileDefinition, legacy_ids: List[str]
    ) -> dict:
        """Parses a bib recod into a FOLIO Inventory instance object
        Community mapping suggestion: https://bit.ly/2S7Gyp3
         This is the main function

        Args:
            marc_record (pymarc.Record): _description_
            file_def (FileDefinition): _description_
            legacy_ids (List[str]): List of legacy ids in record

        Returns:
            dict: _description_
        """
        self.print_progress()
        ignored_subsequent_fields: set = set()
        bad_tags = set(self.task_configuration.tags_to_delete)  # "907"
        folio_instance = self.perform_initial_preparation(marc_record, legacy_ids)
        for marc_field in marc_record:
            self.report_marc_stats(marc_field, bad_tags, legacy_ids, ignored_subsequent_fields)
            if marc_field.tag not in ignored_subsequent_fields:
                self.process_marc_field(
                    folio_instance,
                    marc_field,
                    ignored_subsequent_fields,
                    legacy_ids,
                )

        self.perform_additional_parsing(folio_instance, marc_record, legacy_ids, file_def)
        clean_folio_instance = self.validate_required_properties(
            "-".join(legacy_ids), folio_instance, self.schema, FOLIONamespaces.instances
        )
        self.dedupe_rec(clean_folio_instance)
        marc_record.remove_fields(*list(bad_tags))
        self.report_folio_mapping(clean_folio_instance, self.schema)
        return clean_folio_instance

    def perform_additional_parsing(
        self,
        folio_instance: dict,
        marc_record: Record,
        legacy_ids: List[str],
        file_def: FileDefinition,
    ) -> None:
        """Do stuff not easily captured by the mapping rules

        Args:
            folio_instance (dict): _description_
            marc_record (Record): _description_
            legacy_ids (List[str]): _description_
            file_def (FileDefinition): _description_
        """
        folio_instance["source"] = "MARC"
        folio_instance["instanceFormatIds"] = list(
            set(self.get_instance_format_ids(marc_record, legacy_ids))
        )
        folio_instance["instanceTypeId"] = self.get_instance_type_id(marc_record, legacy_ids)

        folio_instance["modeOfIssuanceId"] = self.get_mode_of_issuance_id(marc_record, legacy_ids)
        self.handle_languages(folio_instance, marc_record, legacy_ids)
        self.handle_suppression(folio_instance, file_def)
        self.handle_holdings(marc_record)
        if prec_titles := folio_instance.get("precedingTitles", []):
            self.migration_report.add(Blurbs.PrecedingSuccedingTitles, f"{len(prec_titles)}")
            del folio_instance["precedingTitles"]
        if succ_titles := folio_instance.get("succeedingTitles", []):
            del folio_instance["succeedingTitles"]
            self.migration_report.add(Blurbs.PrecedingSuccedingTitles, f"{len(succ_titles)}")

    def handle_languages(self, folio_instance, marc_record, legacy_ids):
        if "languages" in folio_instance:
            folio_instance["languages"].extend(self.get_languages(marc_record, legacy_ids))
        else:
            folio_instance["languages"] = self.get_languages(marc_record, legacy_ids)
        folio_instance["languages"] = list(
            self.filter_langs(folio_instance["languages"], marc_record, legacy_ids)
        )

    @staticmethod
    def get_instance_schema():
        logging.info("Fetching Instance schema...")
        instance_schema = FolioClient.get_latest_from_github(
            "folio-org", "mod-inventory-storage", "ramls/instance.json"
        )
        return instance_schema

    def handle_holdings(self, marc_record: Record):
        if "852" in marc_record:
            holdingsfields = marc_record.get_fields(
                "852", "866", "867", "868", "865", "864", "863"
            )
            f852s = (f for f in holdingsfields if f.tag == "852")
            f86xs = (
                f for f in holdingsfields if f.tag in ["866", "867", "868", "865", "864", "863"]
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
        if not self.task_configuration.never_update_hrid_settings:
            self.store_hrid_settings()

    def get_instance_type_id(self, marc_record, legacy_id):
        return_id = ""

        def get_folio_id_by_name(f336a: str):
            match_template = f336a.lower().replace(" ", "")
            match = next(
                (
                    f["id"]
                    for f in self.folio_client.instance_types
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

        if not self.folio_client.instance_types:
            raise TransformationProcessError("", "No instance_types setup in tenant")

        if "336" in marc_record and "b" not in marc_record["336"]:
            self.migration_report.add(Blurbs.RecourceTypeMapping, "Subfield b not in 336")
            if "a" in marc_record["336"]:
                return_id = get_folio_id_by_name(marc_record["336"]["a"])

        if "336" in marc_record and "b" in marc_record["336"]:
            f336_b = marc_record["336"]["b"].lower().replace(" ", "")
            f336_b_norm = f336_b.lower().replace(" ", "")
            t = self.conditions.get_ref_data_tuple_by_code(
                self.folio_client.instance_types,
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
                self.folio_client.instance_types, "instance_types", "zzz"
            )
            return_id = t[0]
        return return_id

    def get_instance_format_id_by_code(self, legacy_id: str, code: str):
        try:
            match = next(f for f in self.folio_client.instance_formats if f["code"] == code)
            self.migration_report.add(
                Blurbs.InstanceFormat,
                f"Successful match  - {code}->{match['name']}",
            )
            return match["id"]
        except Exception:
            # TODO: Distinguish between generated codes and proper 338bs
            Helper.log_data_issue(legacy_id, "Instance format Code not found in FOLIO", code)
            self.migration_report.add(
                Blurbs.InstanceFormat,
                f"Code '{code}' not found in FOLIO",
            )
            return ""

    def get_instance_format_id_by_name(self, f337a: str, f338a: str, legacy_id: str):
        f337a = f337a.lower().strip()
        f338a = f338a.lower().strip()
        match_template = f"{f337a} -- {f338a}"
        try:
            match = next(
                f
                for f in self.folio_client.instance_formats
                if f["name"].lower() == match_template
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

    def f338_source_is_rda_carrier(self, field: pymarc.Field):
        if "2" not in field:
            self.migration_report.add(
                Blurbs.InstanceFormat,
                ("Instance Format not mapped from field since 338$2 is missing"),
            )
            return False
        elif field["2"].strip().startswith("rdacarrier"):
            return True
        self.migration_report.add(
            Blurbs.InstanceFormat,
            ("InstanceFormat not mapped since 338$2 (Source) " f"is set to {field['2']}. "),
        )
        return False

    def get_instance_format_ids_from_a(
        self, field_index, f_338: pymarc.Field, all_337s, legacy_id
    ):
        self.migration_report.add(
            Blurbs.InstanceFormat,
            "338$b is missing. Will try parse from 337$a and 338$a",
        )
        for a in f_338.get_subfields("a"):
            corresponding_337 = all_337s[field_index] if field_index < len(all_337s) else None
            if corresponding_337 and "a" in corresponding_337:
                if fmt_id := self.get_instance_format_id_by_name(
                    corresponding_337["a"], a, legacy_id
                ):
                    yield fmt_id

    def get_instance_format_ids(self, marc_record, legacy_id):
        all_337s = marc_record.get_fields("337")
        all_338s = marc_record.get_fields("338")
        for fidx, f_338 in enumerate(all_338s):
            if self.f338_source_is_rda_carrier(f_338):
                if "b" not in f_338 and "a" in f_338:
                    yield from self.get_instance_format_ids_from_a(
                        fidx, f_338, all_337s, legacy_id
                    )

                for sfidx, b in enumerate(f_338.get_subfields("b")):
                    b = b.replace(" ", "")
                    if len(b) == 2:
                        # Normal 338b. should be able to map this
                        yield self.get_instance_format_id_by_code(legacy_id, b)
                    elif len(b) == 1:
                        corresponding_337 = all_337s[fidx] if fidx < len(all_337s) else None
                        if not corresponding_337:
                            # No matching 337. No use mapping the 338
                            s = "No corresponding 337 to 338 even though 338$b was one character"
                            Helper.log_data_issue(legacy_id, s, b)
                            self.migration_report.add(
                                Blurbs.InstanceFormat,
                                s,
                            )
                        else:
                            # Corresponding 337. Try to combine the codes.
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
                                    yield self.get_instance_format_id_by_code(
                                        legacy_id, combined_code
                                    )

    def get_mode_of_issuance_id(self, marc_record: Record, legacy_id: List[str]) -> str:
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
                    for i in self.folio_client.modes_of_issuance
                    if name.lower() == i["name"].lower()
                ),
                "",
            )

            self.migration_report.add(Blurbs.MatchedModesOfIssuanceCode, f"{name} -- {ret}")

            if not ret:
                self.migration_report.add(
                    Blurbs.MatchedModesOfIssuanceCode, f"Unmatched level: {level}"
                )

                return self.other_mode_of_issuance_id
            return ret
        except IndexError:
            self.migration_report.add(Blurbs.PossibleCleaningTasks, f"No Leader[7] in {legacy_id}")

            return self.other_mode_of_issuance_id
        except StopIteration as ee:
            logging.exception(f"{marc_record.leader} {list(self.folio_client.modes_of_issuance)}")
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
                Helper.log_data_issue(
                    legacy_id, "Field with other Language code", lang_tag.value()
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
        languages = set(self.filter_langs(filter(None, languages), marc_record, legacy_id))
        return languages

    def get_languages(self, marc_record: Record, legacy_id: str) -> List[str]:
        """Get languages and tranforms them to correct codes

        Args:
            marc_record (Record): _description_
            legacy_id (str): _description_

        Returns:
            List[str]: _description_
        """
        languages = self.get_languages_041(marc_record, legacy_id)
        languages.add(self.get_languages_008(marc_record))
        for lang in languages:
            self.migration_report.add(Blurbs.LanguagesInRecords, lang)
        return list(languages)

    def fetch_language_codes(self) -> Generator[str, None, None]:
        """Loads the  list of standardized language codes from LoC

        Yields:
            Generator[str, None, None]: _description_
        """
        path = Path(__file__).parent / "loc_language_codes.xml"
        with open(path) as f:
            lines = "".join(f.readlines())
        tree = fromstring(lines)
        name_space = "{info:lc/xmlns/codelist-v1}"
        xpath_expr = "{0}languages/{0}language/{0}code".format(name_space)
        for code in tree.findall(xpath_expr):
            yield code.text

    def filter_langs(
        self, language_values: List[str], marc_record: Record, index_or_legacy_id
    ) -> typing.Generator:
        forbidden_values = ["###", "zxx", "n/a", "N/A", "|||"]
        for language_value in language_values:
            if language_value in self.language_codes and language_value not in forbidden_values:
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

    def get_legacy_ids(self, marc_record: Record, idx: int) -> List[str]:
        ils_flavour: IlsFlavour = self.task_configuration.ils_flavour
        if ils_flavour in {IlsFlavour.sierra, IlsFlavour.millennium}:
            return get_iii_bib_id(marc_record)
        elif ils_flavour == IlsFlavour.tag907y:
            return RulesMapperBase.get_bib_id_from_907y(marc_record, idx)
        elif ils_flavour == IlsFlavour.tagf990a:
            return RulesMapperBase.get_bib_id_from_990a(marc_record, idx)
        elif ils_flavour == IlsFlavour.aleph:
            return self.get_aleph_bib_id(marc_record)
        elif ils_flavour in {IlsFlavour.voyager, "voyager", IlsFlavour.tag001}:
            return RulesMapperBase.get_bib_id_from_001(marc_record, idx)
        elif ils_flavour == IlsFlavour.koha:
            try:
                return [marc_record["999"]["c"]]
            except Exception as e:
                raise TransformationRecordFailedError(
                    idx,
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
        sys.exit(1)
    if not any(i for i in m_o_is if i["name"].lower() == "unspecified"):
        logging.critical(
            "Mode of issuance 'unspecified' missing in tenant "
            "configuration. Please add this to continue. Quitting..."
        )
        sys.exit(1)
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
