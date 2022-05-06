import uuid
import pytest
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.mapping_file_transformation.organization_mapper import OrganizationMapper


def test_fetch_schema_from_github():
    organization_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-organizations-storage", "mod-orgs", "organization")

assert organization_schema
#     folio_rec = {"circulationNotes": [{"id": "someId", "noteType": "Check in", "note": ""}]}
#     ItemsTransformer.handle_circiulation_notes(folio_rec, str(uuid.uuid4()))
#     assert "circulationNotes" not in folio_rec