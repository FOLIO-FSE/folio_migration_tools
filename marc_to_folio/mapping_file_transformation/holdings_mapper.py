import json
import logging
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
        error_file,
    ):
        holdings_schema = folio_client.get_holdings_schema()
        self.instance_id_map = instance_id_map
        super().__init__(folio_client, holdings_schema, holdings_map, error_file)
        self.holdings_map = holdings_map

        self.call_number_type_map = call_number_type_map
        self.call_number_type_keys = []
        self.default_call_number_type_id = ""
        self.setup_call_number_type_mappings()

        self.location_map = location_map
        self.location_keys = []
        self.default_location_id = ""
        self.setup_location_mappings(location_map)
    def setup_call_number_type_mappings(self):
        logging.info("Fetching Callnumber types...")
        self.folio_call_number_types = list(
            self.folio_client.folio_get_all("/call-number-types", "callNumberTypes")
        )
        for idx, call_number_type_mapping in enumerate(self.call_number_type_map):
            try:
                if idx == 1:
                    self.call_number_type_keys = list(
                        [
                            k
                            for k in call_number_type_mapping.keys()
                            if k not in ["folio_code", "folio_id", "folio_name"]
                        ]
                    )
                if any(m for m in call_number_type_mapping.values() if m == "*"):
                    t = self.get_ref_data_tuple_by_name(
                        self.folio_call_number_types,
                        "callnumbers",
                        call_number_type_mapping["folio_name"],
                    )
                    if t:
                        self.default_call_number_type_id = t[0]
                        logging.info(
                            f'Set {call_number_type_mapping["folio_name"]} as default call_numbertype mapping'
                        )
                    else:
                        raise TransformationProcessError(
                            "No Default call_number type set up in map."
                            "Add a row to mapping file with *:s and a valid call_number type"
                        )
                else:
                    call_number_type_mapping[
                        "folio_id"
                    ] = self.get_ref_data_tuple_by_name(
                        self.folio_call_number_types,
                        "callnumbers",
                        call_number_type_mapping["folio_name"],
                    )[
                        0
                    ]
            except TransformationProcessError as te:
                raise te
            except Exception:
                logging.info(json.dumps(self.call_number_type_map, indent=4))
                raise TransformationProcessError(
                    f"{call_number_type_mapping['folio_name']} could not be found in FOLIO"
                )
        if not self.default_call_number_type_id:
            raise TransformationProcessError(
                "No Default Callnumber type set up in map."
                "Add a row to mapping file with *:s and a valid callnumber type"
            )
        logging.info(
            f"loaded {idx} mappings for {len(self.folio_call_number_types)} loan types in FOLIO"
        )
        print(json.dumps(self.call_number_type_map, indent=4))

    def get_prop(self, legacy_item, folio_prop_name, index_or_id, i=0):
        arr_re = r"\[[0-9]\]"
        if self.use_map:
            legacy_item_keys = list(
                k["legacy_field"]
                for k in self.holdings_map["data"]
                if re.sub(arr_re, ".", k["folio_field"]).strip(".") == folio_prop_name
            )
            vals = list([v for k, v in legacy_item.items() if k in legacy_item_keys])
            legacy_value = " ".join(vals).strip()
            self.add_to_migration_report("Source fields with same target", len(vals))
            # legacy_value = legacy_item.get(legacy_item_key, "")
            if folio_prop_name in ["permanentLocationId", "temporaryLocationId"]:
                return self.get_location_id(legacy_item, index_or_id)
            elif folio_prop_name == "callNumber":
                if legacy_value.startswith("["):
                    new_legacy_values = ast.literal_eval(legacy_value)
                else:
                    new_legacy_values = [legacy_value]
                self.add_to_migration_report(
                    "Bound-with mapping",
                    f"Number of bib-level callnumbers referenced: {len(new_legacy_values)}",
                )
                return new_legacy_values[0]
            elif folio_prop_name == "callNumberTypeId":
                return self.get_call_number_type_id(legacy_item)
            elif folio_prop_name == "statisticalCodeIds":
                return self.get_statistical_codes(vals)
            elif folio_prop_name == "instanceId":
                return self.get_instance_id(legacy_value, index_or_id)
            elif len(legacy_item_keys) == 1:
                logging.debug(folio_prop_name)
                value = next(
                    (
                        k.get("value", "")
                        for k in self.holdings_map["data"]
                        if re.sub(arr_re, ".", k["folio_field"]).strip(".")
                        == folio_prop_name
                    ),
                    "",
                )
                if value not in [None, ""]:
                    return value
                else:

                    return legacy_value
            elif any(legacy_item_keys):
                return legacy_value
            else:
                # self.report_folio_mapping(f"{folio_prop_name}", False, False)
                return ""
        else:
            self.report_folio_mapping(f"{folio_prop_name}", True, False)
            return legacy_item[folio_prop_name]

    def get_location_id(self, legacy_item: dict, id_or_index):
        return self.get_mapped_value(
            "Location",
            legacy_item,
            self.location_keys,
            self.location_map,
            self.default_location_id,
            "folio_code",
        )

    def get_call_number_type_id(self, legacy_item):
        return self.default_call_number_type_id

    def get_instance_id(self, legacy_value: str, index_or_id: str):
        return_ids = []
        if legacy_value.startswith("["):
            try:
                new_legacy_values = ast.literal_eval(legacy_value)
            except:
                print(legacy_value)
        else:
            new_legacy_values = [legacy_value]
        self.add_to_migration_report(
            "Bound-with mapping",
            f"Number of bibs referenced: {len(new_legacy_values)}",
        )
        for v in new_legacy_values:
            if v.startswith("b"):
                new_legacy_value = f".{v}"
            else:
                new_legacy_value = v
            if new_legacy_value not in self.instance_id_map:
                self.add_to_migration_report("Holdings IDs mapped", f"Unmapped")
                s = f"Bib id '.{new_legacy_value}' not in instance id map."
                logging.error(f"{s}\t{index_or_id}")
                # raise TransformationProcessError(s, index_or_id)
            else:
                self.add_to_migration_report("Holdings IDs", f"Mapped")
                return_ids.append(self.instance_id_map[new_legacy_value]["folio_id"])
        if any(return_ids):
            return return_ids[0]
        else:
            raise TransformationProcessError(
                f"No instance id mapped from {legacy_value}"
            )
