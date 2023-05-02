from folio_migration_tools.migration_tasks.manual_fee_fines_transformer import (
    ManualFeeFinesTransformer,
)


def test_get_object_type():
    res = ManualFeeFinesTransformer.get_object_type()
    assert res.name == "feefines"
