import uuid
from typing import List

from pymarc.field import Field
from pymarc.record import Record
from migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from migration_tools.helper import Helper
from migration_tools.marc_rules_transformation.conditions import Conditions
from migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from migration_tools.marc_rules_transformation.rules_mapper_base import RulesMapperBase
from migration_tools.report_blurbs import Blurbs


class RulesMapperHoldings(RulesMapperBase):
    def __init__(
        self,
        folio,
        instance_id_map,
        location_map,
        default_location_code,
        default_call_number_type_id,
    ):
        self.instance_id_map = instance_id_map
        self.conditions = Conditions(
            folio, self, "holdings", default_location_code, default_call_number_type_id
        )
        self.folio = folio
        super().__init__(folio, self.conditions)
        self.location_map = location_map
        self.schema = self.holdings_json_schema
        self.holdings_id_map = {}
        self.ref_data_dicts = {}

    def parse_hold(self, marc_record, index_or_legacy_id, inventory_only=False):
        """Parses a mfhd recod into a FOLIO Inventory instance object
        Community mapping suggestion: https://docs.google.com/spreadsheets/d/1ac95azO1R41_PGkeLhc6uybAKcfpe6XLyd9-F4jqoTo/edit#gid=301923972
         This is the main function"""
        self.print_progress()
        folio_holding = {
            "id": str(uuid.uuid4()),
            "metadata": self.folio_client.get_metadata_construct(),
        }
        self.migration_report.add(Blurbs.RecordStatus, marc_record.leader[5])
        ignored_subsequent_fields = set()

        for marc_field in marc_record:
            try:
                self.process_marc_field(
                    marc_field,
                    ignored_subsequent_fields,
                    folio_holding,
                    index_or_legacy_id,
                )
            except TransformationFieldMappingError as tfme:
                tfme.log_it()
        if not folio_holding.get("formerIds", []):
            raise TransformationProcessError(
                self.parsed_records,
                (
                    "No former ids mapped. Update mapping file so "
                    "that a field is mapped to the formerIds"
                ),
                "",
            )
        if not folio_holding.get("instanceId", ""):
            raise TransformationRecordFailedError(
                self.parsed_records,
                "No Instance id mapped. ",
                folio_holding["formerIds"],
            )
        self.perform_additional_mapping(
            marc_record, folio_holding, folio_holding["formerIds"]
        )
        self.dedupe_rec(folio_holding)
        for identifier in folio_holding["formerIds"]:
            self.holdings_id_map[identifier] = {"id": folio_holding["id"]}
        self.report_folio_mapping(folio_holding, self.schema)
        return folio_holding

    def process_marc_field(
        self,
        marc_field: Field,
        ignored_subsequent_fields,
        folio_holding,
        index_or_legacy_id,
    ):
        self.migration_report.add_general_statistics("Total number of Tags processed")
        if marc_field.tag not in self.mappings:
            self.report_legacy_mapping(marc_field.tag, True, False)
        elif marc_field.tag not in ignored_subsequent_fields:
            mappings = self.mappings[marc_field.tag]
            self.map_field_according_to_mapping(
                marc_field, mappings, folio_holding, index_or_legacy_id
            )
            self.report_legacy_mapping(marc_field.tag, True, True)
            if any(m.get("ignoreSubsequentFields", False) for m in mappings):
                ignored_subsequent_fields.add(marc_field.tag)

    def perform_additional_mapping(
        self, marc_record: Record, folio_holding, legacy_ids: List[str]
    ):
        """Perform additional tasks not easily handled in the mapping rules"""
        self.set_holdings_type(marc_record, folio_holding, legacy_ids)
        self.set_default_call_number_type_if_empty(folio_holding)
        self.set_default_location_if_empty(folio_holding)
        self.pick_first_location_if_many(folio_holding, legacy_ids)
        self.parse_coded_holdings_statements(marc_record, folio_holding)

    def pick_first_location_if_many(self, folio_holding, legacy_ids):
        if " " in folio_holding["permanentLocationId"]:
            Helper.log_data_issue(
                "".join(legacy_ids),
                "Space in permanentLocationId. Was this MFHD attached to multiple holdings?",
                folio_holding["permanentLocationId"],
            )
            folio_holding["permanentLocationId"] = folio_holding[
                "permanentLocationId"
            ].split(" ")[0]

    def parse_coded_holdings_statements(self, marc_record: Record, folio_holding):
        # TODO: Should one be able to switch these things off?
        a = {
            "holdingsStatements": ("853", "863", "866"),
            "holdingsStatementsForIndexes": ("855", "865", "868"),
            "holdingsStatementsForSupplements": ("854", "864", "867"),
        }
        for key, v in a.items():
            try:
                res = HoldingsStatementsParser.get_holdings_statements(
                    marc_record, v[0], v[1], v[2]
                )
                folio_holding[key] = res["statements"]
                for mr in res["migration_report"]:
                    self.migration_report.add(
                        Blurbs.HoldingsStatementsParsing, f"{mr[0]} -- {mr[1]}"
                    )
            except TransformationFieldMappingError as tfme:
                Helper.log_data_issue(tfme.index_or_id, tfme.message, tfme.data_value)
                self.migration_report.add(Blurbs.FieldMappingErrors, tfme.message)

    def set_holdings_type(self, marc_record: Record, folio_holding, legacy_ids):
        # Holdings type mapping
        ldr06 = marc_record.leader[6]
        # TODO: map this better
        # type = type_map.get(ldr06, "Unknown")
        if folio_holding.get("holdingsTypeId", ""):
            self.migration_report.add(
                Blurbs.HoldingsTypeMapping,
                f"Already set to {folio_holding.get('holdingsTypeId')}. LDR[06] was {ldr06}",
            )
        else:
            holdings_type_map = {
                "u": "Unknown",
                "v": "Multi-part monograph",
                "x": "Monograph",
                "y": "Serial",
            }
            holdings_type = holdings_type_map.get(ldr06, "")
            t = self.conditions.get_ref_data_tuple_by_name(
                self.conditions.holdings_types, "hold_types", holdings_type
            )
            if t:
                folio_holding["holdingsTypeId"] = t[0]
                self.migration_report.add(
                    Blurbs.HoldingsTypeMapping,
                    f"{ldr06} -> {holdings_type} -> {t[1]} ({t[0]}",
                )
                if holdings_type == "Unknown":
                    Helper.log_data_issue(
                        legacy_ids,
                        (
                            f"{Blurbs.HoldingsTypeMapping[0]} is 'unknown'. (leader 06 is set to 'u') "
                            "Check if this is correct"
                        ),
                        ldr06,
                    )
            else:
                folio_holding[
                    "holdingsTypeId"
                ] = self.conditions.default_holdings_type_id
                self.migration_report.add(
                    Blurbs.HoldingsTypeMapping,
                    f"A Unmapped {ldr06} -> {holdings_type} -> Unmapped",
                )
                Helper.log_data_issue(
                    legacy_ids,
                    (f"{Blurbs.HoldingsTypeMapping[0]}. leader 06 was unmapped."),
                    ldr06,
                )

    def set_default_call_number_type_if_empty(self, folio_holding):
        if not folio_holding.get("callNumberTypeId", ""):
            folio_holding[
                "callNumberTypeId"
            ] = self.conditions.default_call_number_type_id

    def set_default_location_if_empty(self, folio_holding):
        if not folio_holding.get("permanentLocationId", ""):
            Helper.log_data_issue("", Blurbs.LocationMapping, "")
            folio_holding["permanentLocationId"] = self.conditions.default_location_id
        # special weird case. Likely needs fixing in the mapping rules.

    def remove_from_id_map(self, former_ids):
        """removes the ID from the map in case parsing failed"""
        for former_id in [id for id in former_ids if id]:
            if former_id in self.holdings_id_map:
                del self.holdings_id_map[former_id]
