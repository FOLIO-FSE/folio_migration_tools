from types import MethodType
from unittest.mock import Mock, AsyncMock, patch, create_autospec

from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from src.folio_migration_tools.migration_tasks import batch_poster
from src.folio_migration_tools.migration_tasks.batch_poster import BatchPoster
import pytest
import httpx


def test_get_object_type():
    assert BatchPoster.get_object_type() == FOLIONamespaces.other


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
    with patch(
        "httpx.AsyncClient.get", AsyncMock(return_value=mock_response)
    ) as mock_get:
        # Create an instance of the BatchPoster class
        batch_poster = create_autospec(spec=BatchPoster)
        batch_poster.folio_client = Mock(spec=FolioClient)
        batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
        batch_poster.folio_client.gateway_url = (
            "http://folio-snapshot-okapi.dev.folio.org"
        )
        batch_poster.get_with_retry = Mock(wraps=BatchPoster.get_with_retry)

        # Define test inputs
        query_api = "/instance-storage/instances"
        params = {"query": "id==(record1 OR record2)", "limit": 90}

        # Act
        async with httpx.AsyncClient(
            base_url=batch_poster.folio_client.gateway_url
        ) as client:
            response = await batch_poster.get_with_retry(
                batch_poster, client, query_api, params
            )

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
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=False,
        preserve_administrative_notes=False,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.handle_source_marc = MethodType(
        BatchPoster.patch_record, batch_poster
    )
    batch_poster.keep_existing_fields = MethodType(
        BatchPoster.keep_existing_fields, batch_poster
    )

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
async def test_set_version_async_preserve_status_false():
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
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=False,
        preserve_administrative_notes=False,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
        preserve_item_status=False
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.handle_source_marc = MethodType(
        BatchPoster.patch_record, batch_poster
    )
    batch_poster.keep_existing_fields = MethodType(
        BatchPoster.keep_existing_fields, batch_poster
    )
    
    # Define test inputs
    batch = [{"id": "record1", "status": {"name": "Declared lost"}}, {"id": "record2", "status": {"name": "Unavailable"}}]
    query_api = "/instance-storage/instances"
    object_type = "instances"

    # Act
    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert batch[0]["status"]["name"] == "Declared lost"
    assert batch[1]["status"]["name"] == "Unavailable"


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
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=False,
        preserve_administrative_notes=False,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.handle_source_marc = MethodType(
        BatchPoster.patch_record, batch_poster
    )
    batch_poster.keep_existing_fields = MethodType(
        BatchPoster.keep_existing_fields, batch_poster
    )

    # Define test inputs
    batch = [{"id": "record1"}, {"id": "record2"}]
    query_api = "/item-storage/items"
    object_type = "items"

    # Act
    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert "_version" not in batch[1]
    assert batch[0]["status"]["name"] == "Available"
    assert "status" not in batch[1]


@pytest.mark.asyncio
async def test_set_version_async_preserve_temporary_locations():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1", "_version": 1,
                "status": {"name": "Available"},
                "temporaryLocationId": "tempLocation2"
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=True,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.handle_source_marc = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [{"id": "record1", "temporaryLocationId": "tempLocation1"}, {"id": "record2"}]
    query_api = "/item-storage/items"
    object_type = "items"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[0]["temporaryLocationId"] == "tempLocation2"
    assert "_version" not in batch[1]


@pytest.mark.asyncio
async def test_set_version_async_preserve_temporary_loan_types():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1", "_version": 1,
                "status": {"name": "Available"},
                "temporaryLoanTypeId": "LoanType2"
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=True,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.handle_source_marc = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [{"id": "record1", "temporaryLoanTypeId": "LoanType1"}, {"id": "record2"}]
    query_api = "/item-storage/items"
    object_type = "items"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[0]["temporaryLoanTypeId"] == "LoanType2"
    assert "_version" not in batch[1]


@pytest.mark.asyncio
async def test_set_version_async_preserve_administrative_notes_and_statistical_codes():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1", "_version": 1,
                "status": {"name": "Available"},
                "administrativeNotes": ["note1"],
                "statisticalCodeIds": ["code1", "code2"]
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.handle_source_marc = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [
        {
            "id": "record1", "administrativeNotes": ["note2"], 
            "statisticalCodeIds": ["code3", "code4"]
        },
        {"id": "record2"}
    ]
    query_api = "/item-storage/items"
    object_type = "items"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[0]["administrativeNotes"] == ["note1", "note2"]
    assert batch[0]["statisticalCodeIds"] == ["code1", "code2", "code3", "code4"]
    assert "_version" not in batch[1]


@pytest.mark.asyncio
async def test_set_version_async_preserve_administrative_notes_and_statistical_codes_no_existing_codes_or_notes():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1", "_version": 1,
                "status": {"name": "Available"},
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.handle_source_marc = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [
        {
            "id": "record1", "administrativeNotes": ["note2"], 
            "statisticalCodeIds": ["code3", "code4"]
        },
        {"id": "record2"}
    ]
    query_api = "/item-storage/items"
    object_type = "items"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[0]["administrativeNotes"] == ["note2"]
    assert batch[0]["statisticalCodeIds"] == ["code3", "code4"]
    assert "_version" not in batch[1]


@pytest.mark.asyncio
async def test_set_version_async_source_marc_instance():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "instances": [
            {
                "id": "record1",
                "_version": 1,
                "source": "MARC",
                "title": "Test Title 1",
                "statisticalCodeIds": ["code1", "code2"],
            },
            {
                "id": "record2",
                "_version": 2,
                "source": "MARC",
                "title": "Test Title 2",
                "administrativeNotes": ["test note 1", "test note 2"],
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.patch_record = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [
        {
            "id": "record1", "source": "FOLIO", "statisticalCodeIds": ["code3"],
            "title": "Test Title 3"
        },
        {
            "id": "record2", "source": "FOLIO", "administrativeNotes": ["test note 3"],
            "title": "Test Title 4"
        },
    ]
    query_api = "/instance-storage/instances"
    object_type = "instances"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert batch[0]["source"] == "MARC"
    assert batch[1]["source"] == "MARC"
    assert "administrativeNotes" in batch[0]
    assert batch[0]["administrativeNotes"] == []
    assert batch[1]["administrativeNotes"] == [
        "test note 3",
        "test note 1",
        "test note 2",
    ]
    assert batch[0]["statisticalCodeIds"] == ["code3", "code1", "code2"]
    assert "statisticalCodeIds" in batch[1]
    assert batch[1]["statisticalCodeIds"] == []
    assert batch[0]["title"] == "Test Title 1"
    assert batch[1]["title"] == "Test Title 2"


@pytest.mark.asyncio
async def test_set_version_async_source_marc_instance_do_not_preserve_statistical_codes():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "instances": [
            {
                "id": "record1",
                "_version": 1,
                "source": "MARC",
                "title": "Test Title 1",
                "statisticalCodeIds": ["code1", "code2"],
            },
            {
                "id": "record2",
                "_version": 2,
                "source": "MARC",
                "title": "Test Title 2",
                "administrativeNotes": ["test note 1", "test note 2"],
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=False,
        preserve_administrative_notes=True,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.patch_record = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [
        {
            "id": "record1", "source": "FOLIO", "statisticalCodeIds": ["code3"],
            "title": "Test Title 3"
        },
        {
            "id": "record2", "source": "FOLIO", "administrativeNotes": ["test note 3"],
            "title": "Test Title 4"
        },
    ]
    query_api = "/instance-storage/instances"
    object_type = "instances"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert batch[0]["source"] == "MARC"
    assert batch[1]["source"] == "MARC"
    assert "administrativeNotes" in batch[0]
    assert batch[0]["administrativeNotes"] == []
    assert batch[1]["administrativeNotes"] == [
        "test note 3",
        "test note 1",
        "test note 2",
    ]
    assert batch[0]["statisticalCodeIds"] == ["code3"]
    assert "statisticalCodeIds" in batch[1]
    assert batch[1]["statisticalCodeIds"] == []
    assert batch[0]["title"] == "Test Title 1"
    assert batch[1]["title"] == "Test Title 2"


@pytest.mark.asyncio
async def test_set_version_async_source_marc_instance_do_not_preserve_administrative_notes():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "instances": [
            {
                "id": "record1",
                "_version": 1,
                "source": "MARC",
                "title": "Test Title 1",
                "statisticalCodeIds": ["code1", "code2"],
            },
            {
                "id": "record2",
                "_version": 2,
                "source": "MARC",
                "title": "Test Title 2",
                "administrativeNotes": ["test note 1", "test note 2"],
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=False,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.patch_record = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [
        {
            "id": "record1",
            "source": "FOLIO",
            "statisticalCodeIds": ["code3"],
            "title": "Test Title 3"
        },
        {
            "id": "record2",
            "source": "FOLIO",
            "administrativeNotes": ["test note 3"],
            "title": "Test Title 4"
        },
    ]
    query_api = "/instance-storage/instances"
    object_type = "instances"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert batch[0]["source"] == "MARC"
    assert batch[1]["source"] == "MARC"
    assert "administrativeNotes" in batch[0]
    assert batch[0]["administrativeNotes"] == []
    assert batch[1]["administrativeNotes"] == [
        "test note 3"
    ]
    assert batch[0]["statisticalCodeIds"] == [
        "code3",
        "code1",
        "code2", 
    ]
    assert "statisticalCodeIds" in batch[1]
    assert batch[1]["statisticalCodeIds"] == []
    assert batch[0]["title"] == "Test Title 1"
    assert batch[1]["title"] == "Test Title 2"

@pytest.mark.asyncio
async def test_set_version_async_patch_object_with_patch_paths_no_preserve():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1",
                "holdingsId": "holdings1",
                "_version": 1,
                "barcode": "123456",
                "statisticalCodeIds": ["code1", "code2"],
            },
            {
                "id": "record2",
                "holdingsId": "holdings2",
                "_version": 2,
                "barcode": "789012",
                "administrativeNotes": ["test note 1", "test note 2"],
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=False,
        preserve_administrative_notes=False,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
        patch_existing_records=True,
        patch_paths=["statisticalCodeIds[1]", "subObject.subObjectField"]
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.patch_record = MethodType(
        BatchPoster.patch_record, batch_poster
    )

    # Define test inputs
    batch = [
        {
            "id": "record1",
            "source": "FOLIO",
            "statisticalCodeIds": ["code3", "code4"],
            "title": "Test Title 3",
            "subObject": {
                "subObjectField": "subObjectValue"
            }
        },
        {
            "id": "record2",
            "source": "FOLIO",
            "administrativeNotes": ["test note 3"],
            "title": "Test Title 4"
        },
    ]
    query_api = "/item-storage/items"
    object_type = "items"

    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)

    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert "administrativeNotes" in batch[0]
    assert batch[0]["administrativeNotes"] == []
    assert batch[0]["statisticalCodeIds"] == ["code4"]
    assert "statisticalCodeIds" in batch[1]
    assert batch[1]["statisticalCodeIds"] == []
    assert "subObject" in batch[0]
    assert "subObjectField" in batch[0]["subObject"]
    assert "barcode" in batch[0]
    assert batch[0]["barcode"] == "123456"
    assert "holdingsId" in batch[0]
    assert batch[0]["holdingsId"] == "holdings1"
    assert "barcode" in batch[1]
    assert batch[1]["barcode"] == "789012"
    assert "holdingsId" in batch[1]
    assert batch[1]["holdingsId"] == "holdings2"
    assert batch[0]["subObject"]["subObjectField"] == "subObjectValue"


@pytest.mark.asyncio
async def test_set_version_async_patch_object_with_patch_paths_preserve_statistical_codes_and_administrative_notes():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1",
                "holdingsId": "holdings1",
                "_version": 1,
                "barcode": "123456",
                "statisticalCodeIds": ["code1", "code2"],
            },
            {
                "id": "record2",
                "holdingsId": "holdings2",
                "_version": 2,
                "barcode": "789012",
                "administrativeNotes": ["test note 1", "test note 2"],
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
        patch_existing_records=True,
        patch_paths=["statisticalCodeIds[1]", "subObject.subObjectField"]
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.patch_record = MethodType(
        BatchPoster.patch_record, batch_poster
    )
    # Define test inputs
    batch = [
        {
            "id": "record1",
            "source": "FOLIO",
            "statisticalCodeIds": ["code3", "code4"],
            "title": "Test Title 3",
            "subObject": {
                "subObjectField": "subObjectValue"
            }
        },
        {
            "id": "record2",
            "source": "FOLIO",
            "administrativeNotes": ["test note 1", "test note 2"],
            "title": "Test Title 4",
            "subObject": {
                "subObjectField": "subObjectValue"
            }
        }
    ]
    query_api = "/item-storage/items"
    object_type = "items"
    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)
    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert "administrativeNotes" in batch[0]
    assert batch[0]["administrativeNotes"] == []
    assert "administrativeNotes" in batch[1]
    assert batch[1]["administrativeNotes"] == ["test note 1", "test note 2"]
    assert batch[0]["statisticalCodeIds"] == ["code4", "code1", "code2"]
    assert "statisticalCodeIds" in batch[1]
    assert batch[1]["statisticalCodeIds"] == []
    assert "subObject" in batch[0]
    assert "subObjectField" in batch[0]["subObject"]
    assert "barcode" in batch[0]
    assert batch[0]["barcode"] == "123456"
    assert "holdingsId" in batch[0]
    assert batch[0]["holdingsId"] == "holdings1"
    assert "barcode" in batch[1]
    assert batch[1]["barcode"] == "789012"
    assert "holdingsId" in batch[1]
    assert batch[1]["holdingsId"] == "holdings2"
    assert batch[0]["subObject"]["subObjectField"] == "subObjectValue"


@pytest.mark.asyncio
async def test_set_version_async_patch_object_with_no_patch_paths_preserve_statistical_codes_and_administrative_notes():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1",
                "holdingsId": "holdings1",
                "_version": 1,
                "source": "FOLIO",
                "barcode": "123456",
                "statisticalCodeIds": ["code1", "code2"],
            },
            {
                "id": "record2",
                "holdingsId": "holdings2",
                "_version": 2,
                "source": "FOLIO",
                "barcode": "789012",
                "administrativeNotes": ["test note 1", "test note 2"],
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=True,
        preserve_administrative_notes=True,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
        patch_existing_records=True,
        patch_paths=[]
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.patch_record = MethodType(
        BatchPoster.patch_record, batch_poster
    )
    # Define test inputs
    batch = [
        {
            "id": "record1",
            "statisticalCodeIds": [None, "code4"],
            "subObject": {
                "subObjectField": "subObjectValue"
            }
        },
        {
            "id": "record2",
            "administrativeNotes": ["test note 1", "test note 2"],
            "subObject": {
                "subObjectField": "subObjectValue"
            }
        }
    ]
    query_api = "/item-storage/items"
    object_type = "items"
    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)
    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert "source" in batch[0]
    assert "source" in batch[1]
    assert batch[0]["source"] == "FOLIO"
    assert batch[1]["source"] == "FOLIO"
    assert "administrativeNotes" in batch[0]
    assert batch[0]["administrativeNotes"] == []
    assert "administrativeNotes" in batch[1]
    assert batch[1]["administrativeNotes"] == ["test note 1", "test note 2"]
    assert batch[0]["statisticalCodeIds"] == ["code4", "code1", "code2"]
    assert "statisticalCodeIds" in batch[1]
    assert batch[1]["statisticalCodeIds"] == []
    assert "subObject" in batch[0]
    assert "subObjectField" in batch[0]["subObject"]
    assert "barcode" in batch[0]
    assert batch[0]["barcode"] == "123456"
    assert "holdingsId" in batch[0]
    assert batch[0]["holdingsId"] == "holdings1"
    assert "barcode" in batch[1]
    assert batch[1]["barcode"] == "789012"
    assert "holdingsId" in batch[1]
    assert batch[1]["holdingsId"] == "holdings2"
    assert batch[0]["subObject"]["subObjectField"] == "subObjectValue"


@pytest.mark.asyncio
async def test_set_version_async_patch_object_with_no_patch_paths_no_preserve_statistical_codes_and_administrative_notes():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "items": [
            {
                "id": "record1",
                "holdingsId": "holdings1",
                "_version": 1,
                "source": "FOLIO",
                "barcode": "123456",
                "statisticalCodeIds": ["code1", "code2"],
            },
            {
                "id": "record2",
                "holdingsId": "holdings2",
                "_version": 2,
                "source": "FOLIO",
                "barcode": "789012",
                "administrativeNotes": ["test note 1", "test note 2", "test note 3"],
            },
        ]
    }

    # Create an instance of the BatchPoster class
    batch_poster = create_autospec(spec=BatchPoster)
    batch_poster.task_configuration = Mock()
    batch_poster.task_configuration = BatchPoster.TaskConfiguration(
        name="Test Task",
        migration_task_type="Test Type",
        object_type="Test Object",
        files=[],
        batch_size=100,
        rerun_failed_records=True,
        use_safe_inventory_endpoints=True,
        extradata_endpoints={},
        upsert=False,
        preserve_statistical_codes=False,
        preserve_administrative_notes=False,
        preserve_temporary_locations=False,
        preserve_temporary_loan_types=False,
        patch_existing_records=True,
        patch_paths=[]
    )
    batch_poster.folio_client = Mock(spec=FolioClient)
    batch_poster.folio_client.okapi_headers = {"x-okapi-token": "token"}
    batch_poster.folio_client.gateway_url = "http://folio-snapshot-okapi.dev.folio.org"
    batch_poster.set_version_async = Mock(wraps=BatchPoster.set_version_async)
    batch_poster.get_with_retry = AsyncMock(return_value=mock_response)
    batch_poster.prepare_record_for_upsert = MethodType(
        BatchPoster.prepare_record_for_upsert, batch_poster
    )
    batch_poster.collect_existing_records_for_upsert = (
        BatchPoster.collect_existing_records_for_upsert
    )
    batch_poster.handle_upsert_for_statistical_codes = MethodType(
        BatchPoster.handle_upsert_for_statistical_codes, batch_poster
    )
    batch_poster.handle_upsert_for_administrative_notes = MethodType(
        BatchPoster.handle_upsert_for_administrative_notes, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_locations = MethodType(
        BatchPoster.handle_upsert_for_temporary_locations, batch_poster
    )
    batch_poster.handle_upsert_for_temporary_loan_types = MethodType(
        BatchPoster.handle_upsert_for_temporary_loan_types, batch_poster
    )
    batch_poster.patch_record = MethodType(
        BatchPoster.patch_record, batch_poster
    )
    # Define test inputs
    batch = [
        {
            "id": "record1",
            "statisticalCodeIds": [None,"code4"],
            "subObject": {
                "subObjectField": "subObjectValue"
            }
        },
        {
            "id": "record2",
            "administrativeNotes": ["test note 1", "test note 2"],
            "subObject": {
                "subObjectField": "subObjectValue"
            }
        }
    ]
    query_api = "/item-storage/items"
    object_type = "items"
    await batch_poster.set_version_async(batch_poster, batch, query_api, object_type)
    # Assert
    assert batch[0]["_version"] == 1
    assert batch[1]["_version"] == 2
    assert "source" in batch[0]
    assert "source" in batch[1]
    assert batch[0]["source"] == "FOLIO"
    assert batch[1]["source"] == "FOLIO"
    assert "administrativeNotes" in batch[0]
    assert batch[0]["administrativeNotes"] == []
    assert "administrativeNotes" in batch[1]
    assert batch[1]["administrativeNotes"] == ["test note 1", "test note 2"]
    assert batch[0]["statisticalCodeIds"] == ["code4"]
    assert "statisticalCodeIds" in batch[1]
    assert batch[1]["statisticalCodeIds"] == []
    assert "subObject" in batch[0]
    assert "subObjectField" in batch[0]["subObject"]
    assert "barcode" in batch[0]
    assert batch[0]["barcode"] == "123456"
    assert "holdingsId" in batch[0]
    assert batch[0]["holdingsId"] == "holdings1"
    assert "barcode" in batch[1]
    assert batch[1]["barcode"] == "789012"
    assert "holdingsId" in batch[1]
    assert batch[1]["holdingsId"] == "holdings2"
    assert batch[0]["subObject"]["subObjectField"] == "subObjectValue"


def test_set_version():
    # Mock the asynchronous function
    with patch(
        "folio_migration_tools.migration_tasks.batch_poster.BatchPoster.set_version_async"
    ) as mock_async:
        mock_async.return_value = None

        batch_poster_task = create_autospec(spec=BatchPoster)
        batch_poster_task.set_version_async = mock_async
        batch_poster_task.set_version = Mock(wraps=BatchPoster.set_version)

        batch = [{"id": "record1"}, {"id": "record2"}]
        query_api = "/instance-storage/instances"
        object_type = "instances"

        batch_poster_task.set_version(batch_poster_task, batch, query_api, object_type)
        mock_async.assert_called_once_with(batch, query_api, object_type)

        batch_poster_task.set_version(batch_poster_task, batch, query_api, object_type)
        mock_async.assert_called_with(batch, query_api, object_type)
