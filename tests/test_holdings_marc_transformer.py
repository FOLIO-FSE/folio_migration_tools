from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)


def test_get_object_type():
    assert HoldingsMarcTransformer.get_object_type() == FOLIONamespaces.holdings
