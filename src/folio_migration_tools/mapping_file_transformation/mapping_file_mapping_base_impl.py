from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)


class MappingFileMappingBaseImpl(MappingFileMapperBase):
    def __init__(
        self,
        library_configuration: LibraryConfiguration,
        folio_client: FolioClient,
        schema: dict,
        record_map: dict,
        object_type: FOLIONamespaces,
        ignore_legacy_identifier: bool = False,
    ):
        super().__init__(
            folio_client,
            schema,
            record_map,
            None,
            object_type,
            library_configuration,
            ignore_legacy_identifier,
        )

    def get_prop(self, legacy_item, folio_prop_name, index_or_id):
        return super().get_prop(legacy_item, folio_prop_name, index_or_id)
