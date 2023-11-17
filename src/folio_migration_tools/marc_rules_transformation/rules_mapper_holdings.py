import copy
import json
import logging
from typing import List

import i18n
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient
from pymarc.field import Field
from pymarc.record import Record

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.helper import Helper
from folio_migration_tools.holdings_helper import HoldingsHelper
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)


class RulesMapperHoldings(RulesMapperBase):
    def __init__(
        self,
        folio_client,
        location_map,
        task_configuration,
        library_configuration: LibraryConfiguration,
        parent_id_map: dict,
        boundwith_relationship_map,
    ):
        self.task_configuration = task_configuration
        self.conditions = Conditions(
            folio_client,
            self,
            "holdings",
            library_configuration.folio_release,
            self.task_configuration.default_call_number_type_name,
        )
        self.folio = folio_client
        super().__init__(
            folio_client,
            library_configuration,
            task_configuration,
            self.fetch_holdings_schema(folio_client),
            self.conditions,
            parent_id_map,
        )
        self.boundwith_relationship_map = self.setup_boundwith_relationship_map(
            boundwith_relationship_map
        )
        self.location_map = location_map
        self.holdings_id_map: dict = {}
        self.ref_data_dicts: dict = {}
        self.fallback_holdings_type_id = self.task_configuration.fallback_holdings_type_id
        self.setup_holdings_sources()
        logging.info("Fetching mapping rules from the tenant")
        rules_endpoint = "/mapping-rules/marc-holdings"
        self.mappings = self.folio_client.folio_get_single_object(rules_endpoint)
        self.fix_853_bug_in_rules()

    def fix_853_bug_in_rules(self):
        f852_mappings = self.mappings["852"]
        new_852_mapping = []
        for mapping in f852_mappings:
            if "entity" in mapping:
                for entity_mapping in mapping["entity"]:
                    if "." not in entity_mapping["target"]:
                        new_852_mapping.append(entity_mapping)
                    else:
                        raise TransformationProcessError(
                            "",
                            (
                                "Actual entity mapping found in 852 mappings. "
                                "Report this to the maintainers of this codebase"
                            ),
                            json.dumps(entity_mapping),
                        )
        self.mappings["852"] = new_852_mapping

    def parse_record(
        self, marc_record: Record, file_def: FileDefinition, legacy_ids: List[str]
    ) -> list[dict]:
        """Parses a mfhd recod into a FOLIO Inventory holdings object
        Community mapping suggestion: https://tinyurl.com/3rh52e2x
         This is the main function

        Args:
            marc_record (Record): _description_
            file_def (FileDefinition): _description_
            legacy_ids (List[str]): _description_

        Raises:
            TransformationRecordFailedError: _description_

        Returns:
            dict: _description_
        """

        self.print_progress()
        folio_holding = self.perform_initial_preparation(marc_record, legacy_ids)

        self.migration_report.add("RecordStatus", marc_record.leader[5])
        ignored_subsequent_fields: set = set()
        num_852s = 0
        for marc_field in marc_record:
            try:
                if marc_field.tag == "852":
                    num_852s += 1
                    if num_852s > 1:
                        continue
                self.process_marc_field(
                    folio_holding,
                    marc_field,
                    ignored_subsequent_fields,
                    legacy_ids,
                )
            except TransformationFieldMappingError as tfme:
                tfme.log_it()
        if num_852s > 1:
            Helper.log_data_issue(legacy_ids, "More than 1 852 found", "")

        self.perform_additional_mapping(marc_record, folio_holding, legacy_ids, file_def)
        cleaned_folio_holding = self.validate_required_properties(
            "-".join(folio_holding.get("formerIds")),
            folio_holding,
            self.schema,
            FOLIONamespaces.holdings,
        )
        if not folio_holding.get("instanceId", ""):
            raise TransformationRecordFailedError(
                legacy_ids,
                "No Instance id mapped. ",
                folio_holding.get("formerIds", ["No former ids"]),
            )
        props_to_not_dedupe = (
            []
            if self.task_configuration.deduplicate_holdings_statements
            else [
                "holdingsStatements",
                "holdingsStatementsForIndexes",
                "holdingsStatementsForSupplements",
            ]
        )
        self.dedupe_rec(cleaned_folio_holding, props_to_not_dedupe)
        self.report_folio_mapping(cleaned_folio_holding, self.schema)
        if bw_instance_ids := self.boundwith_relationship_map.get(cleaned_folio_holding["id"], []):
            return list(
                self.create_bound_with_holdings(
                    cleaned_folio_holding,
                    bw_instance_ids,
                    self.task_configuration.holdings_type_uuid_for_boundwiths,
                )
            )
        return [cleaned_folio_holding]

    def set_instance_id_by_map(self, legacy_ids: list, folio_holding: dict, marc_record: Record):
        if "004" not in marc_record:
            raise TransformationProcessError(
                "",
                ("No 004 in record. The tools only support bib-mfhd linking throuh 004"),
                legacy_ids,
            )
        legacy_instance_id = marc_record["004"].data.strip()
        folio_holding["formerIds"].append(f"{self.bib_id_template}{legacy_instance_id}")
        if legacy_instance_id in self.parent_id_map:
            folio_holding["instanceId"] = self.parent_id_map[legacy_instance_id][1]
        else:
            raise TransformationRecordFailedError(
                legacy_ids,
                "Old instance id not in map",
                marc_record["004"],
            )

    def perform_initial_preparation(self, marc_record: Record, legacy_ids):
        folio_holding: dict = {}
        folio_holding["id"] = str(
            FolioUUID(
                str(self.folio_client.okapi_url),
                FOLIONamespaces.holdings,
                str(legacy_ids[0]),
            )
        )
        for legacy_id in legacy_ids:
            self.add_legacy_id_to_admin_note(folio_holding, legacy_id)
        folio_holding["formerIds"] = copy.copy(legacy_ids)
        self.set_instance_id_by_map(legacy_ids, folio_holding, marc_record)
        return folio_holding

    def setup_holdings_sources(self):
        holdings_sources = list(
            self.folio_client.folio_get_all("/holdings-sources", "holdingsRecordsSources")
        )
        logging.info("Fetched %s holdingsRecordsSources from tenant", len(holdings_sources))
        self.holdingssources = {n["name"].upper(): n["id"] for n in holdings_sources}
        if "FOLIO" not in self.holdingssources:
            raise TransformationProcessError("", "No holdings source with name FOLIO in tenant")
        if "MARC" not in self.holdingssources:
            raise TransformationProcessError("", "No holdings source with name MARC in tenant")

    def process_marc_field(
        self,
        folio_holding: dict,
        marc_field: Field,
        ignored_subsequent_fields,
        index_or_legacy_ids,
    ):
        """This overwrites the implementation for Auth and instances

        Args:
            folio_holding (dict): _description_
            marc_field (Field): _description_
            ignored_subsequent_fields (_type_): _description_
            index_or_legacy_ids (_type_): _description_
        """
        self.migration_report.add("Trivia", i18n.t("Total number of Tags processed"))
        if marc_field.tag not in self.mappings:
            self.report_legacy_mapping(marc_field.tag, True, False)
        elif marc_field.tag not in ignored_subsequent_fields:
            mappings = self.mappings[marc_field.tag]
            self.map_field_according_to_mapping(
                marc_field, mappings, folio_holding, index_or_legacy_ids
            )
            self.report_legacy_mapping(marc_field.tag, True, True)
            if any(m.get("ignoreSubsequentFields", False) for m in mappings):
                ignored_subsequent_fields.add(marc_field.tag)

    def perform_additional_mapping(
        self, marc_record: Record, folio_holding, legacy_ids: List[str], file_def: FileDefinition
    ):
        """_summary_

        Args:
            marc_record (Record): _description_
            folio_holding (_type_): _description_
            legacy_ids (List[str]): _description_
            file_def (FileDefinition): _description_

        Raises:
            TransformationRecordFailedError: _description_
        """
        self.set_holdings_type(marc_record, folio_holding, legacy_ids)
        self.set_default_call_number_type_if_empty(folio_holding)
        self.pick_first_location_if_many(folio_holding, legacy_ids)
        self.parse_coded_holdings_statements(marc_record, folio_holding, legacy_ids)
        HoldingsHelper.handle_notes(folio_holding)
        if (
            self.task_configuration.create_source_records
            or self.task_configuration.hrid_handling == HridHandling.preserve001
        ):
            self.hrid_handler.handle_hrid(
                FOLIONamespaces.holdings, folio_holding, marc_record, legacy_ids
            )
        else:
            del folio_holding["hrid"]
        if not folio_holding.get("instanceId", ""):
            raise TransformationRecordFailedError(
                "".join(folio_holding.get("formerIds", [])),
                "Missing instance ids. Something is wrong.",
                "",
            )
        self.handle_suppression(folio_holding, file_def, True)
        self.set_source_id(self.task_configuration, folio_holding, self.holdingssources)

    def pick_first_location_if_many(self, folio_holding, legacy_ids: List[str]):
        if " " in folio_holding.get("permanentLocationId", ""):
            Helper.log_data_issue(
                legacy_ids,
                "Space in permanentLocationId. Was this MFHD attached to multiple holdings?",
                folio_holding["permanentLocationId"],
            )
            folio_holding["permanentLocationId"] = folio_holding["permanentLocationId"].split(" ")[
                0
            ]

    @staticmethod
    def set_source_id(task_configuration, folio_rec, holdingssources):
        if task_configuration.create_source_records:
            folio_rec["sourceId"] = holdingssources.get("MARC")
        else:
            folio_rec["sourceId"] = holdingssources.get("FOLIO")

    def parse_coded_holdings_statements(
        self, marc_record: Record, folio_holding, legacy_ids: List[str]
    ):
        # TODO: Should one be able to switch these things off?
        a = {
            "holdingsStatements": ("853", "863", "866"),
            "holdingsStatementsForIndexes": ("855", "865", "868"),
            "holdingsStatementsForSupplements": ("854", "864", "867"),
        }
        for key, v in a.items():
            try:
                res = HoldingsStatementsParser.get_holdings_statements(
                    marc_record,
                    v[0],
                    v[1],
                    v[2],
                    legacy_ids,
                    self.task_configuration.deduplicate_holdings_statements,
                )
                if res["statements"]:
                    folio_holding[key] = res["statements"]
                for mr in res["migration_report"]:
                    self.migration_report.add("HoldingsStatementsParsing", f"{mr[0]} -- {mr[1]}")
            except TransformationFieldMappingError as tfme:
                Helper.log_data_issue(tfme.index_or_id, tfme.message, tfme.data_value)
                self.migration_report.add("FieldMappingErrors", tfme.message)

    def wrap_up(self):
        logging.info("Mapper wrapping up")
        if self.task_configuration.update_hrid_settings:
            if self.task_configuration.create_source_records:
                self.hrid_handler.store_hrid_settings()
            else:
                logging.info("NOT storing HRID settings since that is managed by FOLIO")

    def fetch_holdings_schema(self, folio_client: FolioClient):
        logging.info("Fetching HoldingsRecord schema...")
        return folio_client.get_from_github(
            "folio-org", "mod-inventory-storage", "ramls/holdingsrecord.json"
        )

    def set_holdings_type(self, marc_record: Record, folio_holding, legacy_ids: List[str]):
        # Holdings type mapping
        ldr06 = marc_record.leader[6]
        # TODO: map this better
        # type = type_map.get(ldr06, "Unknown")
        if folio_holding.get("holdingsTypeId", ""):
            self.migration_report.add(
                "HoldingsTypeMapping",
                i18n.t(
                    "Already set to %{value}. %{leader_key} was %{leader}",
                    value=folio_holding.get("holdingsTypeId"),
                    leader_key="LDR[06]",
                    leader=ldr06,
                ),
            )
        else:
            holdings_type = self.conditions.holdings_type_map.get(ldr06, "")
            if t := self.conditions.get_ref_data_tuple_by_name(
                self.conditions.holdings_types, "hold_types", holdings_type
            ):
                folio_holding["holdingsTypeId"] = t[0]
                self.migration_report.add(
                    "HoldingsTypeMapping",
                    f"{ldr06} -> {holdings_type} -> {t[1]} ({t[0]}",
                )
                if holdings_type == "Unknown":
                    Helper.log_data_issue(
                        legacy_ids,
                        (
                            i18n.t("blurbs.HoldingsTypeMapping.title") + " is 'unknown'. "
                            "(leader 06 is set to 'u') Check if this is correct"
                        ),
                        ldr06,
                    )
            else:
                if not self.fallback_holdings_type_id:
                    raise TransformationProcessError(
                        "",
                        "No fallbackHoldingsTypeId set up. Add to task configuration",
                    )
                folio_holding["holdingsTypeId"] = self.fallback_holdings_type_id
                self.migration_report.add(
                    "HoldingsTypeMapping",
                    i18n.t("An Unmapped")
                    + f" {ldr06} -> {holdings_type} -> "
                    + i18n.t("Unmapped"),
                )
                Helper.log_data_issue(
                    legacy_ids,
                    (
                        i18n.t("blurbs.HoldingsTypeMapping.title", locale="en")
                        + ". leader 06 was unmapped."
                    ),
                    ldr06,
                )

    def set_default_call_number_type_if_empty(self, folio_holding):
        if not folio_holding.get("callNumberTypeId", ""):
            folio_holding["callNumberTypeId"] = self.conditions.default_call_number_type["id"]

    def get_legacy_ids(self, marc_record: Record, idx: int) -> List[str]:
        marc_path = self.task_configuration.legacy_id_marc_path
        split = marc_path.split("$", maxsplit=1)
        results = []
        if not (split[0].isnumeric() and len(split[0]) == 3):
            raise TransformationProcessError(
                "",
                (
                    "the marc field used for determining the legacy id is not numeric "
                    "or does not have the stipulated lenght of 3."
                    "Make sure the task configuration setting for 'legacyIdMarcPath' "
                    "is correct or make this piece of code more allowing"
                ),
                marc_path,
            )
        elif len(split) == 1:
            results.append(marc_record[split[0]].value())
        elif len(split) == 2 and len(split[1]) == 1:
            for field in marc_record.get_fields(split[0]):
                if sf := field.get_subfields(split[1]):
                    results.append(sf[0])
        else:
            raise TransformationProcessError(
                "",
                ("Something is wrong with 'legacyIdMarcPath' property in the settings"),
                marc_path,
            )
        if not any(results):
            raise TransformationRecordFailedError(
                idx, f"No legacy id found in record from {marc_path}", ""
            )
        return results
