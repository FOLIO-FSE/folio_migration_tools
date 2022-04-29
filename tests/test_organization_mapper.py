from migration_tools.mapping_file_transformation.organization_mapper import OrganizationMapper


def test_get_latest_acq_schema_from_github():
    schema = OrganizationMapper.get_latest_acq_schema_from_github("mod-orgs", "organizations")

    assert schema