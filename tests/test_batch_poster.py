from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks import batch_poster
from folio_migration_tools.migration_tasks.batch_poster import BatchPoster


def test_get_object_type():
    assert BatchPoster.get_object_type() == FOLIONamespaces.other


def test_get_unsafe_and_safe_endpoints():

    assert (
        batch_poster.get_api_info("Instances", False)["api_endpoint"]
        == "/instance-storage/batch/synchronous-unsafe"
    )
    assert (
        batch_poster.get_api_info("Instances")["api_endpoint"]
        == "/instance-storage/batch/synchronous"
    )
    assert (
        batch_poster.get_api_info("Holdings", False)["api_endpoint"]
        == "/holdings-storage/batch/synchronous-unsafe"
    )
    assert (
        batch_poster.get_api_info("Holdings")["api_endpoint"]
        == "/holdings-storage/batch/synchronous"
    )
    assert (
        batch_poster.get_api_info("Items", False)["api_endpoint"]
        == "/item-storage/batch/synchronous-unsafe"
    )
    assert batch_poster.get_api_info("Items")["api_endpoint"] == "/item-storage/batch/synchronous"
