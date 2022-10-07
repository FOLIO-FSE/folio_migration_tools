from folio_migration_tools.mapping_file_transformation.organization_mapper import (
    OrganizationMapper,
)
from folio_migration_tools.mapping_file_transformation.mapping_file_mapper_base import (
    MappingFileMapperBase,
)


def test_subclass_inheritance():
    assert issubclass(OrganizationMapper, MappingFileMapperBase)


def test_fetch_acq_schemas_from_github_happy_path():
    organization_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-organizations-storage", "mod-orgs", "organization"
    )

    assert organization_schema["$schema"]

def test_add_one_cateorgy_from_ref_data():
    mappings = [
        {"categories": "claim"},
        {"categories": ""}
    ]

    legacy_object = {"location": "l_1 ", "loan_type": "lt1", "material_type": "mt2 "}

    mock = Mock(spec=RefDataMapping)
    legacy_gategory = "clm"

    folio_category = OrganizationMapper.method()

    assert folio_category = "claim"

    
def test_normal_refdata_mapping_strip():
    mappings = [
        {"location": "l_2", "loan_type": "lt2", "material_type": "mt_1"},
        {"location": "L_1", "loan_type": "Lt1", "material_type": "Mt_1"},
        {"location": "l_1", "loan_type": "lt1", "material_type": "mt2"},
    ]
    legacy_object = {"location": "l_1 ", "loan_type": "lt1", "material_type": "mt2 "}
    mock = Mock(spec=RefDataMapping)
    mock.regular_mappings = mappings
    mock.cache = {}
    mock.mapped_legacy_keys = ["location", "loan_type", "material_type"]
    res = RefDataMapping.get_ref_data_mapping(mock, legacy_object)
    assert res == mappings[2]
