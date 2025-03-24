from unittest.mock import Mock

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
    assert(
        batch_poster.get_api_info("ShadowInstances", False)["api_endpoint"]
        == "/instance-storage/batch/synchronous-unsafe"
    )
    assert (
        batch_poster.get_api_info("ShadowInstances")["api_endpoint"]
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


def test_get_extradata_endpoint_interface_credential():
    extradata = 'interfaceCredential\t{"interfaceId": "7e131c38-5384-44ed-9f4a-da6ca2f36498"}'
    (object_name, data) = extradata.split("\t")

    batch_poster_task = Mock(spec=BatchPoster)
    batch_poster_task.task_configuration = Mock()
    batch_poster_task.task_configuration.extradata_endpoints = {}

    endpoint = BatchPoster.get_extradata_endpoint(
        batch_poster_task.task_configuration, object_name, data
    )

    assert (
        endpoint
        == "organizations-storage/interfaces/7e131c38-5384-44ed-9f4a-da6ca2f36498/credentials"
    )


def test_get_extradata_endpoint_from_task_configuration():
    extradata = 'otherData\t{"otherDataId": "7e131c38-5384-44ed-9f4a-da6ca2f36498"}'
    (object_name, data) = extradata.split("\t")

    batch_poster_task = Mock(spec=BatchPoster)
    batch_poster_task.task_configuration = Mock()
    batch_poster_task.task_configuration.extradata_endpoints = {
        "otherData": "otherdata-endpoint/endpoint"
    }

    endpoint = BatchPoster.get_extradata_endpoint(
        batch_poster_task.task_configuration, object_name, data
    )

    assert endpoint == "otherdata-endpoint/endpoint"
