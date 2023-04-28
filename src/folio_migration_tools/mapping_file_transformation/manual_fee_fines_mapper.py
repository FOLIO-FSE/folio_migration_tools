import uuid
from typing import Any
from typing import Dict

from folio_uuid.folio_uuid import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient

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
            FOLIONamespaces.feefines,
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
        if folio_prop_name == "account.id":
            return index_or_id

        elif folio_prop_name == "account.ownerId":
            return self.get_mapped_ref_data_value(
                self.feefines_owner_map,
                legacy_object,
                index_or_id,
                folio_prop_name,
                False,
            )
        elif folio_prop_name == "account.feeFineId":
            return self.get_mapped_ref_data_value(
                self.feefines_type_map,
                legacy_object,
                index_or_id,
                folio_prop_name,
                False,
            )

        elif folio_prop_name == "feefineaction.accountId":
            return index_or_id

        elif folio_prop_name == "feefineaction.id":
            return str(uuid.uuid4())

        elif folio_prop_name == "account.userId" or folio_prop_name == "feefineaction.userId":
            return self.get_matching_uuid_from_folio(
                self.user_cache,
                "/users",
                "barcode",
                super().get_prop(
                    legacy_object, folio_prop_name, index_or_id, schema_default_value
                ),
                "users",
            )

        elif folio_prop_name == "account.itemId":
            return self.get_matching_uuid_from_folio(
                self.item_cache,
                "/item-storage/items",
                "barcode",
                super().get_prop(
                    legacy_object, folio_prop_name, index_or_id, schema_default_value
                ),
                "items",
            )

        elif mapped_value := super().get_prop(
            legacy_object, folio_prop_name, index_or_id, schema_default_value
        ):
            return mapped_value

        else:
            self.migration_report.add(Blurbs.UnmappedProperties, f"{folio_prop_name}")
            return ""

    def perform_additional_mapping(self, feefine):
        pass

        return feefine

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

    def get_matching_uuid_from_folio(
        self, cache: dict, path: str, match_property: str, match_value: str, result_type: str
    ):
        if match_value not in cache:
            query = f'?query=({match_property}=="{match_value}")'
            if matching_object := next(
                self.folio_client.folio_get_all(path, result_type, query), None
            ):
                cache[match_value] = matching_object["id"]
                return matching_object["id"]
            else:
                return None
        else:
            return cache[match_value]
