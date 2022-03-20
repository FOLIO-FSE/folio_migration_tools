from migration_tools.helper import Helper

def test_orgs_schema():
    organization_schema = Helper.get_latest_from_github(
        "folio-org", "mod-organizations", "schemas/organization.json"
        )
    assert organization_schema is not None, "Schema must not be None."