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


class ManualFeesFinesMapper(MappingFileMapperBase):
    def __init__(
        self,
        folio_client: FolioClient,
        feesfines_map,
        feesfines_owner_map,
        feesfines_type_map,
        library_configuration: LibraryConfiguration,
        task_configuration,
        ignore_legacy_identifier: bool = True
    ):
        self.folio_client: FolioClient = folio_client
        self.user_cache: dict = {}

        self.composite_feefine_schema = self.get_composite_feefine_schema()

        self.task_configuration = task_configuration

        super().__init__(
            folio_client,
            self.composite_feefine_schema,
            feesfines_map,
            None,
            FOLIONamespaces.account,
            library_configuration,
            ignore_legacy_identifier
        )

        self.feesfines_map = feesfines_map

        if feesfines_owner_map:
            self.feesfines_owner_map = RefDataMapping(
                self.folio_client,
                "/owners",
                "owners",
                feesfines_owner_map,
                "owner",
                Blurbs.FeeFineOnwerMapping,
            )
        else:
            self.feesfines_owner_map = None

        if feesfines_type_map:
            self.feesfines_type_map = RefDataMapping(
                self.folio_client,
                "/feefines",
                "feefines",
                feesfines_type_map,
                "feeFineType",
                Blurbs.FeeFineTypesMapping,
            )
        else:
            self.feesfines_type_map = None

    def store_objects(self, composite_feefine):
        try:
            self.extradata_writer.write("account", composite_feefine[0]["account"])
            self.migration_report.add_general_statistics("Stored account")
            self.extradata_writer.write("feefineaction", composite_feefine[0]["feefineaction"])
            self.migration_report.add_general_statistics("Stored feefineactions")

        except Exception as ee:
            raise TransformationRecordFailedError(
                composite_feefine[1], "Failed when storing", ee
            ) from ee

    def get_prop(self, legacy_item, folio_prop_name, index_or_id, schema_default_value):
        if folio_prop_name == "account.ownerId":
            return self.get_mapped_ref_data_value(
                self.feesfines_owner_map,
                legacy_item,
                folio_prop_name,
                index_or_id,
                False,
            )
        elif folio_prop_name == "account.feeFineId":
            return self.get_mapped_ref_data_value(
                self.feesfines_type_map,
                legacy_item,
                folio_prop_name,
                index_or_id,
                False,
            )
        
        elif mapped_value := super().get_prop(
            legacy_item, folio_prop_name, index_or_id, schema_default_value
        ):
            return mapped_value
        else:
            self.migration_report.add(Blurbs.UnmappedProperties, f"{folio_prop_name}")
            return ""
        
    def perform_additional_mapping(self, composite_feefine):
        composite_feefine["finefeefineaction"]["accountId"] = composite_feefine["account"]["id"]

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
