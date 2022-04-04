import json
import logging
import sys
from datetime import datetime
from typing import Dict, List
from uuid import uuid4

from folioclient import FolioClient
from folio_uuid.folio_uuid import FOLIONamespaces
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.helper import Helper
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.report_blurbs import Blurbs


class ItemMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        items_map,
        material_type_map,
        loan_type_map,
        location_map,
        call_number_type_map,
        holdings_id_map,
        statistical_codes_map,
        item_statuses_map,
        temporary_loan_type_mapping,
        temporary_location_mapping,
        library_configuration: LibraryConfiguration,
    ):
        item_schema = folio_client.get_item_schema()
        super().__init__(
            folio_client,
            item_schema,
            items_map,
            statistical_codes_map,
            FOLIONamespaces.items,
            library_configuration,
        )
        self.item_schema = self.folio_client.get_item_schema()
        self.items_map = items_map
        self.holdings_id_map = holdings_id_map
        self.unique_barcodes = set()
        self.ids_dict: Dict[str, set] = {}
        self.use_map = True
        self.status_mapping = {}
        if temporary_loan_type_mapping:
            self.temp_loan_type_mapping = RefDataMapping(
                self.folio_client,
                "/loan-types",
                "loantypes",
                temporary_loan_type_mapping,
                "name",
                Blurbs.TemporaryLoanTypeMapping,
            )
        if temporary_location_mapping:
            self.temp_location_mapping = RefDataMapping(
                self.folio_client,
                "/locations",
                "locations",
                temporary_location_mapping,
                "code",
                Blurbs.TemporaryLocationMapping,
            )

        if item_statuses_map:
            self.setup_status_mapping(item_statuses_map)
        if call_number_type_map:
            self.call_number_mapping = RefDataMapping(
                self.folio_client,
                "/call-number-types",
                "callNumberTypes",
                call_number_type_map,
                "name",
                Blurbs.CallNumberTypeMapping,
            )
        self.loan_type_mapping = RefDataMapping(
            self.folio_client,
            "/loan-types",
            "loantypes",
            loan_type_map,
            "name",
            Blurbs.PermanentLoanTypeMapping,
        )

        self.material_type_mapping = RefDataMapping(
            self.folio_client,
            "/material-types",
            "mtypes",
            material_type_map,
            "name",
            Blurbs.MaterialTypeMapping,
        )

        self.location_mapping = RefDataMapping(
            self.folio_client,
            "/locations",
            "locations",
            location_map,
            "code",
            Blurbs.LocationMapping,
        )

    def perform_additional_mappings(self):
        raise NotImplementedError()

    def setup_status_mapping(self, item_statuses_map):
        statuses = self.item_schema["properties"]["status"]["properties"]["name"][
            "enum"
        ]
        for mapping in item_statuses_map:
            if "folio_name" not in mapping:
                logging.critical(
                    "folio_name is not a column in the status mapping file"
                )
                sys.exit()
            elif "legacy_code" not in mapping:
                logging.critical(
                    "legacy_code is not a column in the status mapping file"
                )
                sys.exit()
            elif mapping["folio_name"] not in statuses:
                logging.critical(
                    "%s in the mapping file is not a FOLIO item status",
                    mapping["folio_name"],
                )
                sys.exit()
            elif mapping["legacy_code"] == "*":
                logging.critical(
                    "* in status mapping not allowed. Available will be the default mapping. "
                    "Please remove the row with the *"
                )
                sys.exit()
            elif not all(mapping.values()):
                logging.critical(
                    "empty value in mapping %s. Check mapping file", mapping.values()
                )
                sys.exit()
            else:
                self.status_mapping = {
                    v["legacy_code"]: v["folio_name"] for v in item_statuses_map
                }
        logging.info(json.dumps(statuses, indent=True))

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        value_tuple = (legacy_item, folio_prop_name, index_or_id)
        if not self.use_map:
            return legacy_item[folio_prop_name]
        legacy_item_keys = self.mapped_from_legacy_data.get(folio_prop_name, [])
        # IF there is a value mapped, return that one
        if len(legacy_item_keys) == 1 and folio_prop_name in self.mapped_from_values:
            value = self.mapped_from_values.get(folio_prop_name, "")
            self.migration_report.add(
                Blurbs.DefaultValuesAdded, f"{value} added to {folio_prop_name}"
            )
            return value
        legacy_values = MappingFileMapperBase.get_legacy_vals(
            legacy_item, legacy_item_keys
        )
        legacy_value = " ".join(legacy_values).strip()
        if folio_prop_name == "permanentLocationId":
            return self.get_mapped_value(
                self.location_mapping,
                *value_tuple,
                False,
            )
        elif folio_prop_name == "temporaryLocationId":
            temp_loc = self.get_mapped_value(
                self.temp_location_mapping,
                *value_tuple,
                True,
            )
            self.migration_report.add(Blurbs.TemporaryLocationMapping, f"{temp_loc}")
            return temp_loc
        elif folio_prop_name == "materialTypeId":
            return self.get_mapped_value(
                self.material_type_mapping,
                *value_tuple,
            )
        elif folio_prop_name == "itemLevelCallNumberTypeId":
            return self.get_item_level_call_number_type_id(
                legacy_item, folio_prop_name, index_or_id
            )
        elif folio_prop_name == "status.name":
            return self.transform_status(legacy_value)
        elif folio_prop_name == "barcode":
            barcode = next((v for v in legacy_values if v), "")
            if barcode.strip() and barcode in self.unique_barcodes:
                Helper.log_data_issue(
                    index_or_id, "Duplicate barcode", "-".join(legacy_values)
                )
                self.migration_report.add_general_statistics("Duplicate barcodes")
                return f"{barcode}-{uuid4()}"
            else:
                if barcode.strip():
                    self.unique_barcodes.add(barcode)
                return barcode

        elif folio_prop_name == "status.date":
            return datetime.utcnow().isoformat()
        elif folio_prop_name == "temporaryLoanTypeId":
            ltid = self.get_mapped_value(
                self.temp_loan_type_mapping,
                *value_tuple,
                True,
            )
            self.migration_report.add(
                Blurbs.TemporaryLoanTypeMapping, f"{folio_prop_name} -> {ltid}"
            )
            return ltid
        elif folio_prop_name == "permanentLoanTypeId":
            return self.get_mapped_value(self.loan_type_mapping, *value_tuple)
        elif folio_prop_name.startswith("statisticalCodeIds"):
            statistical_code_id = self.get_statistical_codes(
                legacy_item, folio_prop_name, index_or_id
            )
            self.migration_report.add(
                Blurbs.StatisticalCodeMapping,
                f"{folio_prop_name} -> {statistical_code_id}",
            )
            return statistical_code_id
        elif folio_prop_name == "holdingsRecordId":
            if legacy_value in self.holdings_id_map:
                return self.holdings_id_map[legacy_value]["folio_id"]
            self.migration_report.add_general_statistics(
                "Records failed because of failed holdings",
            )
            s = (
                "Holdings id referenced in legacy item "
                "was not found amongst transformed Holdings records"
            )
            raise TransformationRecordFailedError(index_or_id, s, legacy_value)
        elif any(legacy_item_keys):
            return legacy_value
        else:
            self.migration_report.add(
                Blurbs.UnmappedProperties, f"{folio_prop_name} {legacy_item_keys}"
            )
            return ""

    def get_item_level_call_number_type_id(
        self, legacy_item, folio_prop_name: str, index_or_id
    ):
        if self.call_number_mapping:
            return self.get_mapped_value(
                self.call_number_mapping, legacy_item, index_or_id, folio_prop_name
            )
        self.migration_report.add(
            Blurbs.CallNumberTypeMapping,
            "Mapping not setup",
        )
        return ""

    def transform_status(self, legacy_value):
        status = self.status_mapping.get(legacy_value, "Available")
        self.migration_report.add(Blurbs.StatusMapping, f"'{legacy_value}' -> {status}")
        return status
