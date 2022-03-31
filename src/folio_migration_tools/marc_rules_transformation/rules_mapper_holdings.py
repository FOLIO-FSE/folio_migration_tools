import logging

from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folio_migration_tools.custom_exceptions import (
    TransformationFieldMappingError,
    TransformationProcessError,
    TransformationRecordFailedError,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.library_configuration import (
    HridHandling,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.holdings_statementsparser import (
    HoldingsStatementsParser,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.report_blurbs import Blurbs
from pymarc.field import Field
from pymarc.record import Record


class RulesMapperHoldings(RulesMapperBase):
    def __init__(
        self,
        folio,
        instance_id_map,
        location_map,
        task_configuration,
        library_configuration: LibraryConfiguration,
    ):
        self.instance_id_map = instance_id_map
        self.task_configuration = task_configuration
        self.conditions = Conditions(
            folio,
            self,
            "holdings",
            self.task_configuration.default_call_number_type_name,
        )
        self.folio = folio
        super().__init__(folio, library_configuration, self.conditions)
        self.location_map = location_map
        self.schema = self.holdings_json_schema
        self.holdings_id_map = {}
        self.ref_data_dicts = {}
        self.fallback_holdings_type_id = (
            self.task_configuration.fallback_holdings_type_id
        )

    def parse_hold(self, marc_record, legacy_id):
        """Parses a mfhd recod into a FOLIO Inventory instance object
        Community mapping suggestion: https://tinyurl.com/3rh52e2x
         This is the main function"""
        self.print_progress()
        folio_holding = {
            "metadata": self.folio_client.get_metadata_construct(),
        }
        self.migration_report.add(Blurbs.RecordStatus, marc_record.leader[5])
        ignored_subsequent_fields = set()
        num_852s = 0
        for marc_field in marc_record:
            try:
                if marc_field.tag == "852":
                    num_852s += 1
                self.process_marc_field(
                    marc_field,
                    ignored_subsequent_fields,
                    folio_holding,
                    legacy_id,
                )
            except TransformationFieldMappingError as tfme:
                tfme.log_it()
        if num_852s > 1:
            Helper.log_data_issue(legacy_id, "More than 1 852 found", "")

        folio_holding["id"] = str(
            FolioUUID(
                self.folio_client.okapi_url,
                FOLIONamespaces.holdings,
                legacy_id,
            )
        )

        if not folio_holding.get("instanceId", ""):
            raise TransformationRecordFailedError(
                legacy_id,
                "No Instance id mapped. ",
                folio_holding["formerIds"],
            )
        self.perform_additional_mapping(marc_record, folio_holding, legacy_id)
        cleaned_folio_holding = self.validate_required_properties(
            "-".join(folio_holding.get("formerIds")),
            folio_holding,
            self.holdings_json_schema,
            FOLIONamespaces.holdings,
        )
        self.dedupe_rec(cleaned_folio_holding)
        self.holdings_id_map[legacy_id] = self.get_id_map_dict(
            legacy_id, cleaned_folio_holding
        )

        self.report_folio_mapping(cleaned_folio_holding, self.schema)
        return cleaned_folio_holding

    def process_marc_field(
        self,
        marc_field: Field,
        ignored_subsequent_fields,
        folio_holding,
        index_or_legacy_ids,
    ):
        self.migration_report.add_general_statistics("Total number of Tags processed")
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
        self, marc_record: Record, folio_holding, legacy_id: str
    ):
        """Perform additional tasks not easily handled in the mapping rules"""
        self.set_holdings_type(marc_record, folio_holding, legacy_id)
        self.set_default_call_number_type_if_empty(folio_holding)
        self.pick_first_location_if_many(folio_holding, legacy_id)
        self.parse_coded_holdings_statements(marc_record, folio_holding, legacy_id)

    def pick_first_location_if_many(self, folio_holding, legacy_id: str):
        if " " in folio_holding.get("permanentLocationId", ""):
            Helper.log_data_issue(
                legacy_id,
                "Space in permanentLocationId. Was this MFHD attached to multiple holdings?",
                folio_holding["permanentLocationId"],
            )
            folio_holding["permanentLocationId"] = folio_holding[
                "permanentLocationId"
            ].split(" ")[0]

    def parse_coded_holdings_statements(
        self, marc_record: Record, folio_holding, legacy_id
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
                    marc_record, v[0], v[1], v[2], legacy_id
                )
                folio_holding[key] = res["statements"]
                for mr in res["migration_report"]:
                    self.migration_report.add(
                        Blurbs.HoldingsStatementsParsing, f"{mr[0]} -- {mr[1]}"
                    )
            except TransformationFieldMappingError as tfme:
                Helper.log_data_issue(tfme.index_or_id, tfme.message, tfme.data_value)
                self.migration_report.add(Blurbs.FieldMappingErrors, tfme.message)

    def wrap_up(self):
        logging.info("Mapper wrapping up")
        if self.task_configuration.hrid_handling == HridHandling.preserve001:
            self.store_hrid_settings()
        else:
            logging.info("NOT storing HRID settings since that is managed by FOLIO")

    def set_holdings_type(self, marc_record: Record, folio_holding, legacy_id: str):
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
            if t := self.conditions.get_ref_data_tuple_by_name(
                self.conditions.holdings_types, "hold_types", holdings_type
            ):
                folio_holding["holdingsTypeId"] = t[0]
                self.migration_report.add(
                    Blurbs.HoldingsTypeMapping,
                    f"{ldr06} -> {holdings_type} -> {t[1]} ({t[0]}",
                )
                if holdings_type == "Unknown":
                    Helper.log_data_issue(
                        legacy_id,
                        (
                            f"{Blurbs.HoldingsTypeMapping[0]} is 'unknown'. (leader 06 is set to 'u') "
                            "Check if this is correct"
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
                    Blurbs.HoldingsTypeMapping,
                    f"A Unmapped {ldr06} -> {holdings_type} -> Unmapped",
                )
                Helper.log_data_issue(
                    legacy_id,
                    (f"{Blurbs.HoldingsTypeMapping[0]}. leader 06 was unmapped."),
                    ldr06,
                )

    def set_default_call_number_type_if_empty(self, folio_holding):
        if not folio_holding.get("callNumberTypeId", ""):
            folio_holding[
                "callNumberTypeId"
            ] = self.conditions.default_call_number_type["id"]

    def remove_from_id_map(self, former_ids):
        """removes the ID from the map in case parsing failed"""
        for former_id in [id for id in former_ids if id]:
            if former_id in self.holdings_id_map:
                del self.holdings_id_map[former_id]
