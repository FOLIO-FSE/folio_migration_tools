import json
import logging
from marc_to_folio.conditions import Conditions
import uuid
import requests
from marc_to_folio.rules_mapper_base import RulesMapperBase


class RulesMapperHoldings(RulesMapperBase):
    def __init__(
        self, folio, instance_id_map, location_map, default_location_code, args
    ):
        super().__init__(folio, Conditions(folio,self)
        )
        print("Init RulesMapperHoldings")
        self.instance_id_map = instance_id_map
        self.location_map = location_map
        self.schema = self.holdings_json_schema
        self.holdings_id_map = {}
        self.ref_data_dicts = {}
        print(any(self.location_map))
        self.holdings_types = list(
            folio.folio_get_all("/holdings-types", "holdingsTypes")
        )
        self.default_call_number_type_id = "0b099785-75b4-4f6d-a027-4f113b58ee23"
        print(f"Fetched {len(self.holdings_id_map)} holdings types")
        self.default_holdings_type_id = self.get_ref_data_tuple(
            self.holdings_types, "holdings_types", "Monograph", "name"
        )[0]

        self.default_location_id = self.get_ref_data_tuple(
            self.conditions.locations, "locations", default_location_code, "code"
        )[0]
        print(f"Default location code is {self.default_location_id}")

    def parse_hold(self, marc_record, inventory_only=False):
        """ Parses a mfhd recod into a FOLIO Inventory instance object
            Community mapping suggestion: https://docs.google.com/spreadsheets/d/1ac95azO1R41_PGkeLhc6uybAKcfpe6XLyd9-F4jqoTo/edit#gid=301923972 
             This is the main function"""
        self.print_progress()
        legacy_id = get_legacy_id(marc_record)
        folio_holding = {
            "id": str(uuid.uuid4()),
            "metadata": self.folio_client.get_metadata_construct(),
        }
        self.add_to_migration_report(
            "Record status (leader pos 5)", marc_record.leader[5]
        )
        temp_inst_type = ""
        ignored_subsequent_fields = set()
        bad_tags = []  # "907"

        for marc_field in marc_record:
            self.add_stats(self.stats, "Total number of Tags processed")

            # if (not marc_field.tag.isnumeric()) and marc_field.tag != "LDR":
            #    bad_tags.append(marc_field.tag)

            if marc_field.tag not in self.mappings:
                self.report_legacy_mapping(marc_field.tag, True, False, False)
            else:
                if marc_field.tag not in ignored_subsequent_fields:
                    mappings = self.mappings[marc_field.tag]
                    # print(mappings)
                    self.map_field_according_to_mapping(
                        marc_field, mappings, folio_holding
                    )
                    self.report_legacy_mapping(marc_field.tag, True, True, False)
                    if any(m.get("ignoreSubsequentFields", False) for m in mappings):
                        ignored_subsequent_fields.add(marc_field.tag)
                    self.perform_additional_mapping(marc_record, folio_holding)
        self.holdings_id_map[marc_record["001"].format_field()] = folio_holding["id"]
        self.dedupe_rec(folio_holding)
        self.count_unmapped_fields(self.schema, folio_holding)
        try:
            self.count_mapped_fields(folio_holding)
        except:
            print(folio_holding)
        for id in legacy_id:
            self.holdings_id_map[id] = {"id": folio_holding["id"]}

        return folio_holding

    def perform_additional_mapping(self, marc_record, folio_holding):
        ldr06 = marc_record.leader[6]
        self.add_to_migration_report("Leader 06 (Holdings type)", ldr06)
        # TODO: map this better
        # type = type_map.get(ldr06, "Unknown")
        folio_holding["holdingsTypeId"] = self.default_holdings_type_id
        folio_holding["callNumberTypeId"] = self.default_call_number_type_id
        if not folio_holding.get("permanentLocationId", ""):
            folio_holding["permanentLocationId"] = self.default_location_id

    def get_ref_data_tuple(self, ref_data, ref_name, key_value, key_type):
        dict_key = f"{ref_name}{key_type}"
        if dict_key not in self.ref_data_dicts:
            d = {}
            for r in ref_data:
                d[r[key_type].lower()] = (r["id"], r["name"])
            self.ref_data_dicts[dict_key] = d
        ref_object = (
            self.ref_data_dicts[dict_key][key_value.lower()]
            if key_value.lower() in self.ref_data_dicts[dict_key]
            else None
        )
        if not ref_object:
            logging.debug(f"No matching element for {key_value} in {list(ref_data)}")
            return None
        return ref_object

    def remove_from_id_map(self, marc_record):
        """ removes the ID from the map in case parsing failed"""
        id_key = marc_record["001"].format_field()
        if id_key in self.holdings_id_map:
            del self.holdings_id_map[id_key]


def get_legacy_id(marc_record, ils_flavour=""):
    return [marc_record["001"].format_field().strip()]

