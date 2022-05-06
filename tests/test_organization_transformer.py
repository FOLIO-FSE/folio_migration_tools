import uuid
import pytest
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.migration_tasks.organization_transformer import OrganizationTransformer


def test_transform_organization():
    legacy_org = []
#     folio_rec = {"circulationNotes": [{"id": "someId", "noteType": "Check in", "note": ""}]}
#     ItemsTransformer.handle_circiulation_notes(folio_rec, str(uuid.uuid4()))
#     assert "circulationNotes" not in folio_rec