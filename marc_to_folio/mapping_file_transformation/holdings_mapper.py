import json
import logging
from marc_to_folio.mapping_file_transformation.ref_data_mapping import RefDataMapping
import re
from marc_to_folio.custom_exceptions import TransformationProcessError
from folioclient import FolioClient
from marc_to_folio.mapping_file_transformation.mapper_base import MapperBase
from datetime import datetime
import ast


class HoldingsMapper(MapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        holdings_map,
        location_map,
        call_number_type_map,
        instance_id_map,
    ):
        holdings_schema = folio_client.get_holdings_schema()
        self.instance_id_map = instance_id_map
        super().__init__(folio_client, holdings_schema, holdings_map)
        self.holdings_map = holdings_map

        self.location_mapping = RefDataMapping(
            self.folio_client, "/locations", "locations", location_map, "code"
        )
        if call_number_type_map:
            self.call_number_mapping = RefDataMapping(
                self.folio_client,
                "/call-number-types",
                "callNumberTypes",
                call_number_type_map,
                "name",
            )

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        if not self.use_map:
            return legacy_item[folio_prop_name]
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
        legacy_values = MapperBase.get_legacy_vals(legacy_item, legacy_item_keys)
        legacy_value = " ".join(legacy_values).strip()
        if folio_prop_name == "permanentLocationId":
            return self.get_location_id(legacy_item, index_or_id)
        elif folio_prop_name == "temporaryLocationId": 
            return self.get_location_id(legacy_item, index_or_id, True)
        elif folio_prop_name == "callNumber":
            if legacy_value.startswith("["):
                self.add_stats("Bound-with items callnumber identified")
                self.add_to_migration_report(
                    "Bound-with mapping",
                    f"Number of bib-level callnumbers in record: {len(legacy_value.split(','))}",
                )
            return legacy_value
        elif folio_prop_name == "callNumberTypeId":
            return self.get_call_number_type_id(legacy_item)
        elif folio_prop_name == "statisticalCodeIds":
            return self.get_statistical_codes(legacy_values)
        elif folio_prop_name == "instanceId":
            return self.get_instance_ids(legacy_value, index_or_id)
        elif len(legacy_item_keys) == 1:
            logging.debug(
                f"One value from one property to return{folio_prop_name} "
            )
            value = self.mapped_from_values.get(folio_prop_name, "")
            if value in [None, ""]:
                return legacy_value
            return value
        elif any(legacy_item_keys):
            logging.debug(
                f"Multiple values from multiple mappings to return{folio_prop_name} "
            )
            return legacy_values
        else:
            self.report_folio_mapping(f"Edge case: {folio_prop_name}", False, False)
            return ""

    def get_location_id(self, legacy_item: dict, id_or_index, prevent_default=False):
        return self.get_mapped_value(
            self.location_mapping,
            legacy_item, prevent_default
        )

    def get_call_number_type_id(self, legacy_item):
        if self.call_number_mapping:
            return self.get_mapped_value(
                    self.call_number_mapping,
                    legacy_item
                )
        self.add_to_migration_report("Call number type mapping", "No mapping")
        return ""

    def get_instance_ids(self, legacy_value: str, index_or_id: str):
        # Returns a list of Id:s
        return_ids = []
        if legacy_value.startswith("["):
            try:
                new_legacy_values = ast.literal_eval(legacy_value)
                if len(new_legacy_values) > 1:
                    self.add_stats("Bound-with items identified by bib id")
                    for l in new_legacy_values:
                        self.add_stats("Bib ids referenced in bound-with items")
            except:
                logging.error(
                    f"{legacy_value} could not get parsed to array of strings"
                )
        else:
            new_legacy_values = [legacy_value]
        self.add_to_migration_report(
            "Bound-with mapping",
            f"Number of bib records referenced in item: {len(new_legacy_values)}",
        )
        for v in new_legacy_values:
            new_legacy_value = f".{v}" if v.startswith("b") else v
            if (
                new_legacy_value not in self.instance_id_map
                and v not in self.instance_id_map
            ):
                self.add_stats("Holdings IDs not mapped")
                s = f"Bib id '{new_legacy_value}' not in instance id map."
                logging.error(f"{s}\t{index_or_id}")
                # raise TransformationProcessError(s, index_or_id)
            else:
                self.add_stats("Holdings IDs mapped")
                entry = self.instance_id_map.get(
                    new_legacy_value, ""
                ) or self.instance_id_map.get(v)

                return_ids.append(entry["folio_id"])
        if any(return_ids):
            return return_ids
        else:
            raise TransformationProcessError(
                f"No instance id mapped from {legacy_value}"
            )
