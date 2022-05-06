import uuid
import pytest
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.mapping_file_transformation.organization_mapper import OrganizationMapper


def test_fetch_acq_schemas_from_github_happy_path():
    organization_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-organizations-storage", "mod-orgs", "organization")

    assert organization_schema["$schema"]