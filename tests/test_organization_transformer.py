import uuid
import pytest
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.migration_tasks.organization_transformer import OrganizationsTransformer


# def test_fetch_schema_from_github():
#     folio_rec = {"circulationNotes": [{"id": "someId", "noteType": "Check in", "note": ""}]}
#     ItemsTransformer.handle_circiulation_notes(folio_rec, str(uuid.uuid4()))
#     assert "circulationNotes" not in folio_rec