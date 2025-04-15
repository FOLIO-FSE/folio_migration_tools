from unittest import mock
from unittest.mock import Mock, AsyncMock, patch, create_autospec

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.migration_tasks import batch_poster
from folio_migration_tools.migration_tasks.batch_poster import BatchPoster
import pytest
import httpx


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
    assert (
        batch_poster.get_api_info("Items")["api_endpoint"]
        == "/item-storage/batch/synchronous"
    )


def test_get_extradata_endpoint_interface_credential():
    extradata = (
        'interfaceCredential\t{"interfaceId": "7e131c38-5384-44ed-9f4a-da6ca2f36498"}'
    )
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


def test_set_consortium_source():
    json_rec_marc = {"source": "MARC"}
    json_rec_folio = {"source": "FOLIO"}
    BatchPoster.set_consortium_source(json_rec_marc)
    BatchPoster.set_consortium_source(json_rec_folio)
    assert json_rec_marc["source"] == "CONSORTIUM-MARC"
    assert json_rec_folio["source"] == "CONSORTIUM-FOLIO"

@pytest.mark.asyncio
async def test_get_with_retry_successful():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "instances": [
            {"id": "record1", "_version": 1, "status": {"name": "Available"}},
            {"id": "record2", "_version": 2, "status": {"name": "Checked out"}},
        ]
    }
    mock_response.raise_for_status = Mock(return_value=None)
    mock_response.raise_for_status.return_value = None
    mock_response.headers = {"x-okapi-tenant": "test_tenant"}
    mock_response.text = "OK"

    # Mock the httpx.AsyncClient.get method
    with patch("httpx.AsyncClient.get", AsyncMock(return_value=mock_response)) as mock_get:
        # Create an instance of the BatchPoster class
        batch_poster = create_autospec(spec=BatchPoster)
        batch_poster.folio_client = Mock(spec=FolioClient)
        batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
        batch_poster.folio_client.okapi_url = "http://folio-snapshot-okapi.dev.folio.org"
        batch_poster.get_with_retry = Mock(wraps=BatchPoster.get_with_retry)

        # Define test inputs
        query_api = "/instance-storage/instances"
        params = {"query": "id==(record1 OR record2)", "limit": 90}

        # Act
        async with httpx.AsyncClient(base_url=batch_poster.folio_client.okapi_url) as client:
            response = await batch_poster.get_with_retry(batch_poster, client, query_api, params)

        # Assert
        assert response.status_code == 200
        assert response.json() == {
            "instances": [
                {"id": "record1", "_version": 1, "status": {"name": "Available"}},
                {"id": "record2", "_version": 2, "status": {"name": "Checked out"}},
            ]
        }
        mock_get.assert_called_once_with(
            query_api,
            params=params,
            headers=batch_poster.folio_client.okapi_headers,
        )


@pytest.mark.asyncio
async def test_set_version_async():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "instances": [
            {"id": "record1", "_version": 1, "status": {"name": "Available"}},
            {"id": "record2", "_version": 2, "status": {"name": "Checked out"}},
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.okapi_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.update_record_versions = BatchPoster.update_record_versions

    # Define test inputs
    batch = [{"id": "record1"}, {"id": "record2"}]
    query_api = "/instance-storage/instances"
    object_type = "instances"

    # Act
    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert batch[0]["status"]["name"] == "Available"
    assert batch[1]["status"]["name"] == "Checked out"


@pytest.mark.asyncio
async def test_set_version_async_one_existing_items():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {"id": "record1", "_version": 1, "status": {"name": "Available"}},
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.okapi_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.update_record_versions = BatchPoster.update_record_versions

    # Define test inputs
    batch = [{"id": "record1"}, {"id": "record2"}]
    query_api = "/item-storage/items"
    object_type = "items"

    # Act
    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert "_verstion" not in batch[1]
    assert batch[0]["status"]["name"] == "Available"
    assert "status" not in batch[1]


def test_set_version():
    # Mock the asynchronous function
    with patch("folio_migration_tools.migration_tasks.batch_poster.BatchPoster.set_version_async") as mock_async:
        mock_async.return_value = None

        batch_poster = create_autospec(spec=BatchPoster)
        batch_poster.set_version_async = mock_async
        batch_poster.set_version = Mock(wraps=BatchPoster.set_version)

        batch = [{"id": "record1"}, {"id": "record2"}]
        query_api = "/instance-storage/instances"
        object_type = "instances"

        batch_poster.set_version(batch_poster, batch, query_api, object_type)
        mock_async.assert_called_once_with(batch, query_api, object_type)

        batch_poster.set_version(batch_poster, batch, query_api, object_type)
        mock_async.assert_called_with(batch, query_api, object_type)


