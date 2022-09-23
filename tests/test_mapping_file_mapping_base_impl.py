from unittest.mock import Mock

from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.mapping_file_mapping_base_impl import (
    MappingFileMappingBaseImpl,
)
from folio_migration_tools.test_infrastructure import mocked_classes


def test_get_prop():
    record_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    record_schema = {}
    legacy_record = {"user_name": "user_name_1", "id": "1"}
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    mapper = MappingFileMappingBaseImpl(
        mock_library_conf, mock_folio, record_schema, record_map, FOLIONamespaces.other
    )
    prop = mapper.get_prop(legacy_record, "username", "1")
    assert prop == "user_name_1"
