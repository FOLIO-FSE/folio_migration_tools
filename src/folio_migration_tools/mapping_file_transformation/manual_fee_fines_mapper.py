import json
import logging
import uuid
import i18n
from typing import Any
from typing import Dict
from zoneinfo import ZoneInfo

from dateutil import parser as dateutil_parser
from dateutil import tz
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)


class ManualFeeFinesMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        library_configuration: LibraryConfiguration,
        task_configuration,
        feefines_map,
        feefines_owner_map,
        feefines_type_map,
        service_point_map,
        ignore_legacy_identifier: bool = True,
    ):
        self.folio_client: FolioClient = folio_client
        self.composite_feefine_schema = self.get_composite_feefine_schema()
        self.task_configuration = task_configuration
        self.tenant_timezone = self.get_tenant_timezone()

        super().__init__(
            folio_client,
            self.composite_feefine_schema,
            feefines_map,
            None,
            FOLIONamespaces.fees_fines,
            library_configuration,
            ignore_legacy_identifier,
        )

        self.feefines_map = feefines_map
        self.user_cache: dict = {}
        self.item_cache: dict = {}

        if feefines_owner_map:
            self.feefines_owner_map = RefDataMapping(
                self.folio_client,
                "/owners",
                "owners",
                feefines_owner_map,
                "owner",
                "FeeFineOnwerMapping",
            )
        else:
            self.feefines_owner_map = None

        if feefines_type_map:
            self.feefines_type_map = RefDataMapping(
                self.folio_client,
                "/feefines",
                "feefines",
                feefines_type_map,
                "feeFineType",
                "FeeFineTypesMapping",
            )
        else:
            self.feefines_type_map = None

        if service_point_map:
            self.service_point_map = RefDataMapping(
                self.folio_client,
                "/service-points",
                "servicepoints",
                service_point_map,
                "name",
                "FeeFineServicePointTypesMapping",
            )
        else:
            self.service_point_map = None

    def store_objects(self, composite_feefine):
        try:
            self.extradata_writer.write("account", composite_feefine["account"])
            self.migration_report.add_general_statistics(i18n.t("TOTAL Accounts created"))
            self.extradata_writer.write("feefineaction", composite_feefine["feefineaction"])
            self.migration_report.add_general_statistics(i18n.t("TOTAL Feefineactions created"))

        except Exception as ee:
            raise TransformationRecordFailedError(
                composite_feefine, "Failed when storing", ee
            ) from ee

    def get_prop(self, legacy_object, folio_prop_name, index_or_id, schema_default_value):
        if folio_prop_name == "account.ownerId" and self.feefines_owner_map:
            return self.get_mapped_ref_data_value(
                self.feefines_owner_map, legacy_object, index_or_id, folio_prop_name, False
            )

        elif folio_prop_name == "account.feeFineId" and self.feefines_type_map:
            return self.get_mapped_ref_data_value(
                self.feefines_type_map, legacy_object, index_or_id, folio_prop_name, False
            )

        elif folio_prop_name == "feefineaction.createdAt" and self.service_point_map:
            return self.get_mapped_ref_data_value(
                self.service_point_map, legacy_object, index_or_id, folio_prop_name, False
            )

        elif folio_prop_name == "account.amount" or folio_prop_name == "account.remaining":
            return self.parse_sum_as_float(
                index_or_id,
                super().get_prop(
                    legacy_object, folio_prop_name, index_or_id, schema_default_value
                ),
                folio_prop_name,
            )

        elif folio_prop_name == "feefineaction.dateAction":
            return self.parse_date_with_tenant_timezone(
                "feefineaction.dateAction",
                index_or_id,
                super().get_prop(
                    legacy_object, folio_prop_name, index_or_id, schema_default_value
                ),
            )

        elif folio_prop_name == "feefineaction.id":
            return str(uuid.uuid4())

        elif mapped_value := super().get_prop(
            legacy_object, folio_prop_name, index_or_id, schema_default_value
        ):
            return mapped_value

        else:
            return ""

    def get_composite_feefine_schema(self) -> Dict[str, Any]:
        return {
            "properties": {
                "account": FolioClient.get_latest_from_github(
                    "folio-org", "mod-feesfines", "/ramls/accountdata.json"
                ),
                "feefineaction": FolioClient.get_latest_from_github(
                    "folio-org", "mod-feesfines", "/ramls/feefineactiondata.json"
                ),
            }
        }

    def get_tenant_timezone(self):
        config_path = (
            "/configurations/entries?query=(module==ORG%20and%20configName==localeSettings)"
        )
        try:
            tenant_timezone_str = json.loads(
                self.folio_client.folio_get_single_object(config_path)["configs"][0]["value"]
            )["timezone"]
            logging.info("Tenant timezone is: %s", tenant_timezone_str)
            return ZoneInfo(tenant_timezone_str)
        except TypeError as te:
            raise TransformationProcessError(
                "",
                "Failed to fetch Tenant Locale Settings. "
                "Is your library configuration correct?",
            ) from te
        except KeyError as ke:
            raise TransformationProcessError(
                "",
                "Failed to parse Tenant Locale Settings. "
                "Is the Tenant Locale config correctly formatted?",
            ) from ke

    def parse_date_with_tenant_timezone(self, folio_prop_name: str, index_or_id, mapped_value):
        try:
            format_date = dateutil_parser.parse(mapped_value, fuzzy=True)
            if format_date.tzinfo != tz.UTC:
                format_date = format_date.replace(tzinfo=self.tenant_timezone)
            return format_date.isoformat()
        except Exception:
            self.migration_report.add(
                "GeneralStatistics",
                i18n.t("DATA ISSUE Invalid dates"),
            )
            logging.log(
                26,
                "DATA ISSUE\t%s\t%s\t%s",
                index_or_id,
                f"{folio_prop_name} Not a valid date.",
                mapped_value,
            )

    def parse_sum_as_float(self, index_or_id, legacy_sum, folio_prop_name):
        try:
            return float(legacy_sum)
        except Exception as ee:
            self.migration_report.add(
                "GeneralStatistics",
                i18n.t("DATA ISSUE Invalid sum"),
            )
            raise TransformationRecordFailedError(
                index_or_id,
                f"{folio_prop_name} Value must only contain numbers/decimals.",
                legacy_sum,
            ) from ee

    def get_matching_record_from_folio(
        self,
        index_or_id,
        cache: dict,
        path: str,
        match_property: str,
        match_value: str,
        result_type: str,
    ):
        if match_value in cache:
            return cache[match_value]
        else:
            query = f'?query=({match_property}=="{match_value}")'
            if matching_record := next(
                self.folio_client.folio_get_all(path, result_type, query), None
            ):
                cache[match_value] = matching_record
                return matching_record

    def get_folio_user_uuid(self, index_or_id, user_barcode):
        if matching_user := self.get_matching_record_from_folio(
            index_or_id, self.user_cache, "/users", "barcode", user_barcode, "users"
        ):
            return matching_user["id"]
        else:
            self.migration_report.add(
                "GeneralStatistics",
                i18n.t("DATA ISSUE Users not in FOLIO"),
            )
            raise TransformationRecordFailedError(
                index_or_id,
                "No matching user in FOLIO for barcode",
                user_barcode,
            )

    def perform_additional_mapping(self, index_or_id, composite_feefine, legacy_object):
        # Generate account ID
        composite_feefine["account"]["id"] = composite_feefine["id"]
        composite_feefine["feefineaction"]["accountId"] = composite_feefine["id"]

        # Link to FOLIO user
        composite_feefine["account"]["userId"] = self.get_folio_user_uuid(
            index_or_id,
            composite_feefine["account"]["userId"],
        )
        composite_feefine["feefineaction"]["userId"] = composite_feefine["account"]["userId"]

        # Add item data from FOLIO if available
        if item_barcode := composite_feefine["account"].get("itemId"):
            self.enrich_with_folio_item_data(index_or_id, composite_feefine, item_barcode)

        self.add_additional_fields_and_values(composite_feefine, legacy_object)

        return composite_feefine

    def stringify_legacy_object(self, legacy_object):
        legacy_string = (
            "MIGRATION NOTE : This fee/fine was migrated to FOLIO from a previous "
            "library management system. The following is the original data: "
        )
        for key, value in legacy_object.items():
            legacy_string += f"{key.title()}: {value}; "
        return legacy_string.strip().strip(";")

    def enrich_with_folio_item_data(self, index_or_id, feefine, item_barcode):
        if folio_item := self.get_matching_record_from_folio(
            index_or_id,
            self.item_cache,
            "/inventory/items",
            "barcode",
            item_barcode,
            "items",
        ):
            feefine["account"]["itemId"] = folio_item.get("id", "")
            feefine["account"]["title"] = folio_item.get("title", "")
            feefine["account"]["barcode"] = folio_item.get("barcode", "")
            feefine["account"]["callNumber"] = folio_item.get("callNumber", "")
            feefine["account"]["materialType"] = folio_item.get("materialType", {}).get("name")
            feefine["account"]["materialTypeId"] = folio_item.get("materialType", {}).get("id")
            feefine["account"]["location"] = folio_item.get("effectiveLocation", {}).get("name")
        else:
            feefine["account"].pop("itemId")
            self.migration_report.add(
                "GeneralStatistics",
                i18n.t("DATA ISSUE Items not in FOLIO"),
            )
            logging.log(
                26,
                "DATA ISSUE\t%s\t%s\t%s",
                index_or_id,
                "No matching item in FOLIO for barcode",
                item_barcode,
            )

    def add_additional_fields_and_values(self, feefine, legacy_object):
        # Add standard values
        feefine["feefineaction"]["source"] = self.folio_client.username
        feefine["feefineaction"]["notify"] = False
        feefine["feefineaction"]["amountAction"] = feefine["account"]["amount"]
        feefine["feefineaction"]["balance"] = feefine["account"]["remaining"]

        # Set the account status to Open/Closed based on remainign amount
        if feefine["account"]["remaining"] > 0:
            feefine["account"]["status"] = {"name": "Open"}
        else:
            feefine["account"]["status"] = {"name": "Closed"}

        # Add the full legacy item dict to the comment field
        if feefine["feefineaction"].get("comments"):
            feefine["feefineaction"]["comments"] = (
                ("STAFF : " + feefine["feefineaction"]["comments"])
                + " "
                + self.stringify_legacy_object(legacy_object)
            )
        else:
            feefine["feefineaction"]["comments"] = self.stringify_legacy_object(legacy_object)

        # Add name values from reference data mapping
        if self.feefines_owner_map:
            feefine["account"]["feeFineOwner"] = [
                owner["owner"]
                for owner in self.feefines_owner_map.ref_data
                if owner["id"] == feefine["account"]["ownerId"]
            ][0]

        if self.feefines_type_map:
            type_name = [
                type["feeFineType"]
                for type in self.feefines_type_map.ref_data
                if type["id"] == feefine["account"]["feeFineId"]
            ][0]
            feefine["account"]["feeFineType"] = type_name
            feefine["feefineaction"]["typeAction"] = type_name

        return feefine
