import uuid
from typing import Any
from typing import Dict

from dateutil.parser import parse
from folio_uuid.folio_uuid import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.custom_exceptions import TransformationFieldMappingError
from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)
from folio_migration_tools.mapping_file_transformation.ref_data_mapping import (
    RefDataMapping,
)
from folio_migration_tools.report_blurbs import Blurbs


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
        self.user_cache: dict = {}
        self.item_cache: dict = {}
        self.composite_feefine_schema = self.get_composite_feefine_schema()

        self.task_configuration = task_configuration

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

        if feefines_owner_map:
            self.feefines_owner_map = RefDataMapping(
                self.folio_client,
                "/owners",
                "owners",
                feefines_owner_map,
                "owner",
                Blurbs.FeeFineOnwerMapping,
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
                Blurbs.FeeFineTypesMapping,
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
                Blurbs.FeeFineServicePointTypesMapping,
            )
        else:
            self.service_point_map = None

    def store_objects(self, composite_feefine):
        try:
            self.extradata_writer.write("account", composite_feefine["account"])
            self.migration_report.add_general_statistics("Stored account")
            self.extradata_writer.write("feefineaction", composite_feefine["feefineaction"])
            self.migration_report.add_general_statistics("Stored feefineactions")

        except Exception as ee:
            raise TransformationRecordFailedError(
                composite_feefine, "Failed when storing", ee
            ) from ee

    def get_prop(self, legacy_object, folio_prop_name, index_or_id, schema_default_value):
        if folio_prop_name == "account.id" or folio_prop_name == "feefineaction.accountId":
            return index_or_id

        elif folio_prop_name == "account.amount" or folio_prop_name == "account.remaining":
            return self.parse_sum_as_float(
                index_or_id,
                super().get_prop(
                    legacy_object, folio_prop_name, index_or_id, schema_default_value
                ),
                folio_prop_name,
            )

        elif folio_prop_name == "account.ownerId" and self.feefines_owner_map:
            return self.get_mapped_ref_data_value(
                self.feefines_owner_map, legacy_object, index_or_id, folio_prop_name, False
            )

        elif folio_prop_name == "account.feeFineId" and self.feefines_type_map:
            return self.get_mapped_ref_data_value(
                self.feefines_type_map, legacy_object, index_or_id, folio_prop_name, False
            )

        elif folio_prop_name == "account.userId" or folio_prop_name == "feefineaction.userId":
            return self.get_matching_record_from_folio(
                index_or_id,
                self.user_cache,
                "/users",
                "barcode",
                super().get_prop(
                    legacy_object, folio_prop_name, index_or_id, schema_default_value
                ),
                "users",
            )["id"]

        elif folio_prop_name == "feefineaction.id":
            return str(uuid.uuid4())

        elif folio_prop_name == "feefineaction.createdAt" and self.service_point_map:
            return self.get_mapped_ref_data_value(
                self.service_point_map, legacy_object, index_or_id, folio_prop_name, False
            )

        elif folio_prop_name == "feefineaction.dateAction":
            return self.parse_date(
                folio_prop_name,
                index_or_id,
                super().get_prop(
                    legacy_object, folio_prop_name, index_or_id, schema_default_value
                ),
                legacy_object,
            )

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

    def get_matching_record_from_folio(
        self,
        index_or_id,
        cache: dict,
        path: str,
        match_property: str,
        match_value: str,
        result_type: str,
    ):
        if match_value not in cache:
            query = f'?query=({match_property}=="{match_value}")'
            if matching_record := next(
                self.folio_client.folio_get_all(path, result_type, query), None
            ):
                cache[match_value] = matching_record
                return matching_record
            else:
                raise TransformationFieldMappingError(
                    index_or_id, f"No matching {result_type} for {match_property}", match_value
                )
        else:
            return cache[match_value]

    def parse_date(self, folio_prop_name: str, index_or_id, mapped_value, legacy_object):
        try:
            format_date = parse(mapped_value, fuzzy=True)
            return format_date.isoformat()
        except Exception as exception:
            raise TransformationFieldMappingError(
                index_or_id, f"Invalid {folio_prop_name} date for {legacy_object} ", mapped_value
            ) from exception

    def parse_sum_as_float(self, index_or_id, legacy_sum, folio_prop_name):
        try:
            return float(legacy_sum)
        except Exception as ee:
            raise TransformationRecordFailedError(
                index_or_id,
                f"Values mapped to '{folio_prop_name}' may only contain numbers/decimals.",
                legacy_sum,
            ) from ee

    def perform_additional_mapping(self, index_or_id, feefine, legacy_object):
        # Add some name values to ensure nice UI behaviour
        feefine["account"]["feeFineOwner"] = [
            owner["owner"]
            for owner in self.feefines_owner_map.ref_data
            if owner["id"] == feefine["account"]["ownerId"]
        ][0]

        type_name = [
            type["feeFineType"]
            for type in self.feefines_type_map.ref_data
            if type["id"] == feefine["account"]["feeFineId"]
        ][0]
        feefine["account"]["feeFineType"] = type_name
        feefine["feefineaction"]["typeAction"] = type_name

        feefine["feefineaction"]["source"] = self.folio_client.username
        feefine["feefineaction"]["notify"] = False
        feefine["feefineaction"]["amountAction"] = feefine["account"]["amount"]
        feefine["feefineaction"]["balance"] = feefine["account"]["remaining"]

        # Add item data from FOLIO if available
        if folio_item := self.get_matching_record_from_folio(
            index_or_id,
            self.item_cache,
            "/inventory/items",
            "barcode",
            super().get_prop(legacy_object, "account.itemId", "", ""),
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

        # Add the full legacy item dict to the comment field
        if feefine["feefineaction"].get("comments"):
            feefine["feefineaction"]["comments"] = (
                ("STAFF : " + feefine["feefineaction"]["comments"])
                + " "
                + self.stringify_legacy_object(legacy_object)
            )

        else:
            feefine["feefineaction"]["comments"] = self.stringify_legacy_object(legacy_object)

        return feefine

    def stringify_legacy_object(self, legacy_object):
        legacy_string = (
            "MIGRATION NOTE : This fee/fine was migrated to FOLIO from a previous "
            "library management system. The following is the original data: "
        )
        for key, value in legacy_object.items():
            legacy_string += f"{key.title()}: {value}; "
        return legacy_string.strip().strip(";")
