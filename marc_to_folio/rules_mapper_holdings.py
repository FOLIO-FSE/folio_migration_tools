import json
import logging
from marc_to_folio.conditions import Conditions
import uuid
import requests
from marc_to_folio.rules_mapper_base import RulesMapperBase


class RulesMapperHoldings(RulesMapperBase):
    def __init__(self, folio, instance_id_map, location_map, default_location_code, args):
        self.conditions = Conditions(folio,self, default_location_code)
        self.folio = folio
        super().__init__(folio, self.conditions)
        self.instance_id_map = instance_id_map
        self.location_map = location_map
        self.schema = self.holdings_json_schema
        self.holdings_id_map = {}
        self.ref_data_dicts = {}


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
        ignored_subsequent_fields = set()
        
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
                    self.perform_additional_mapping(marc_record, folio_holding, legacy_id)
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

    def perform_additional_mapping(self, marc_record, folio_holding, legacy_id):
        """Perform additional tasks not easily handled in the mapping rules"""
        
        # Holdings type mapping
        ldr06 = marc_record.leader[6]
        self.add_to_migration_report("Leader 06 (Holdings type)", ldr06)
        # TODO: map this better
        # type = type_map.get(ldr06, "Unknown")
        if not folio_holding.get("holdingsTypeId", ""):
            htype_map = {"u":"Unknown", "v":"Multi-part monograph", "x":"Monographic", "y":"Serial"}
            htype = htype_map.get(ldr06, "")
            t = self.conditions.get_ref_data_tuple_by_name(self.conditions.holdings_types, "hold_types", htype)
            if t:
                folio_holding["holdingsTypeId"] = t[0]
                self.add_to_migration_report("Holdings type mapping", t[1])
            else:
                folio_holding["holdingsTypeId"] = self.folio.default_holdings_type_id
                self.add_to_migration_report("Holdings type mapping", "Unknown")
            
        
        folio_holding["callNumberTypeId"] = self.conditions.default_call_number_type_id
        if not folio_holding.get("permanentLocationId", ""):
            folio_holding["permanentLocationId"] = self.conditions.default_location_id
        
        # special weird case. Likely needs fixing in the mapping rules.
        if " " in folio_holding["permanentLocationId"]:
            print(f'Space in permanentLocationId for {legacy_id} ({folio_holding["permanentLocationId"]}). Taking the first one')
            folio_holding["permanentLocationId"] = folio_holding["permanentLocationId"].split(" ")[0]

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
            print(f"No matching element for {key_value} in {list(ref_data)}")
            return None
        return ref_object

    def remove_from_id_map(self, marc_record):
        """ removes the ID from the map in case parsing failed"""
        id_key = marc_record["001"].format_field()
        if id_key in self.holdings_id_map:
            del self.holdings_id_map[id_key]


def get_legacy_id(marc_record, ils_flavour=""):
    return [marc_record["001"].format_field().strip()]

