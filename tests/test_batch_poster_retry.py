"""
Tests for batch_poster.py retry logic in get_with_retry method.
These tests focus on exception handling and retry behavior.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from types import MethodType
import asyncio
from folioclient import FolioClient, FolioConnectionError, FolioHTTPError
import httpx

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster


def create_mock_request():
    """Create a mock httpx.Request for exception testing."""
    mock_request = Mock(spec=httpx.Request)
    mock_request.method = "GET"
    mock_request.url = "https://test.folio.org/test"
    return mock_request


def create_mock_http_error(status_code: int, message: str):
    """Create a FolioHTTPError with proper request and response mocks."""
    mock_request = create_mock_request()
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = status_code
    mock_response.text = message
    
    error = FolioHTTPError(message, request=mock_request, response=mock_response)
    # Ensure the response attribute is set (FolioHTTPError should do this)
    error.response = mock_response
    return error


def create_batch_poster():
    """Helper to create a BatchPoster mock with get_with_retry method for testing."""
    batch_poster = Mock(spec=BatchPoster)
    batch_poster.folio_client = Mock(spec=FolioClient)
    
    # Bind the actual get_with_retry method to the mock
    batch_poster.get_with_retry = MethodType(BatchPoster.get_with_retry, batch_poster)
    
    return batch_poster


@pytest.mark.asyncio
async def test_get_with_retry_connection_error_then_success():
    """Test that FolioConnectionError triggers retry and succeeds on second attempt."""
    batch_poster = create_batch_poster()
    
    # Mock: fail once with connection error, then succeed
    mock_response = {"instances": [{"id": "1", "_version": 1}]}
    mock_request = create_mock_request()
    connection_error = FolioConnectionError("Connection failed", request=mock_request)
    
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=[connection_error, mock_response]
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        result = await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should succeed on second attempt
    assert result == mock_response
    assert batch_poster.folio_client.folio_get_async.call_count == 2
    # Should wait 2^0 = 1 second before retry
    mock_sleep.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_get_with_retry_connection_error_exhausted():
    """Test that FolioConnectionError raises after 3 failed attempts."""
    batch_poster = create_batch_poster()
    
    # Mock: fail all 3 attempts
    mock_request = create_mock_request()
    connection_error = FolioConnectionError("Connection failed", request=mock_request)
    batch_poster.folio_client.folio_get_async = AsyncMock(side_effect=connection_error)
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(FolioConnectionError, match="Connection failed"):
            await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should try 3 times total
    assert batch_poster.folio_client.folio_get_async.call_count == 3
    # Should sleep twice: 1s (2^0) and 2s (2^1)
    assert mock_sleep.call_count == 2
    assert mock_sleep.call_args_list[0][0][0] == 1  # First retry wait
    assert mock_sleep.call_args_list[1][0][0] == 2  # Second retry wait


@pytest.mark.asyncio
async def test_get_with_retry_http_429_rate_limit():
    """Test that HTTP 429 (rate limit) triggers retry with 5-second wait."""
    batch_poster = create_batch_poster()
    
    http_error_429 = create_mock_http_error(429, "Rate limit exceeded")
    mock_success = {"instances": [{"id": "1", "_version": 1}]}
    
    # Mock: 429 error first, then success
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=[http_error_429, mock_success]
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        result = await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should succeed on second attempt
    assert result == mock_success
    assert batch_poster.folio_client.folio_get_async.call_count == 2
    # Should wait 5 seconds for rate limiting
    mock_sleep.assert_called_once_with(5)


@pytest.mark.asyncio
async def test_get_with_retry_http_500_server_error():
    """Test that HTTP 500 (server error) triggers retry with exponential backoff."""
    batch_poster = create_batch_poster()
    
    http_error_500 = create_mock_http_error(500, "Internal server error")
    mock_success = {"instances": [{"id": "1", "_version": 1}]}
    
    # Mock: 500 error twice, then success
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=[http_error_500, http_error_500, mock_success]
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        result = await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should succeed on third attempt
    assert result == mock_success
    assert batch_poster.folio_client.folio_get_async.call_count == 3
    # Should use exponential backoff: 1s (2^0), 2s (2^1)
    assert mock_sleep.call_count == 2
    assert mock_sleep.call_args_list[0][0][0] == 1
    assert mock_sleep.call_args_list[1][0][0] == 2


@pytest.mark.asyncio
async def test_get_with_retry_http_503_exhausted():
    """Test that HTTP 503 raises after 3 failed attempts."""
    batch_poster = create_batch_poster()
    
    http_error_503 = create_mock_http_error(503, "Service unavailable")
    
    # Mock: fail all 3 attempts with 503
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=http_error_503
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(FolioHTTPError):
            await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should try 3 times total
    assert batch_poster.folio_client.folio_get_async.call_count == 3
    # Should sleep twice with exponential backoff
    assert mock_sleep.call_count == 2


@pytest.mark.asyncio
async def test_get_with_retry_http_404_no_retry():
    """Test that HTTP 404 (client error) does NOT retry."""
    batch_poster = create_batch_poster()
    
    http_error_404 = create_mock_http_error(404, "Not found")
    
    # Mock: 404 error (should not retry)
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=http_error_404
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(FolioHTTPError):
            await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should only try once (no retries for 4xx except 429)
    assert batch_poster.folio_client.folio_get_async.call_count == 1
    # Should not sleep at all
    mock_sleep.assert_not_called()


@pytest.mark.asyncio
async def test_get_with_retry_http_422_no_retry():
    """Test that HTTP 422 (unprocessable entity) does NOT retry."""
    batch_poster = create_batch_poster()
    
    http_error_422 = create_mock_http_error(422, "Validation error")
    
    # Mock: 422 error (should not retry)
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=http_error_422
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(FolioHTTPError):
            await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should only try once
    assert batch_poster.folio_client.folio_get_async.call_count == 1
    # Should not sleep
    mock_sleep.assert_not_called()


@pytest.mark.asyncio
async def test_get_with_retry_http_400_no_retry():
    """Test that HTTP 400 (bad request) does NOT retry."""
    batch_poster = create_batch_poster()
    
    http_error_400 = create_mock_http_error(400, "Bad request")
    
    # Mock: 400 error (should not retry)
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=http_error_400
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(FolioHTTPError):
            await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should only try once
    assert batch_poster.folio_client.folio_get_async.call_count == 1
    # Should not sleep
    mock_sleep.assert_not_called()


@pytest.mark.asyncio
async def test_get_with_retry_http_502_bad_gateway():
    """Test that HTTP 502 (bad gateway) triggers retry."""
    batch_poster = create_batch_poster()
    
    http_error_502 = create_mock_http_error(502, "Bad gateway")
    mock_success = {"instances": [{"id": "1", "_version": 1}]}
    
    # Mock: 502 error first, then success
    batch_poster.folio_client.folio_get_async = AsyncMock(
        side_effect=[http_error_502, mock_success]
    )
    
    with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        result = await batch_poster.get_with_retry("/test", {"limit": 10})
    
    # Should succeed on second attempt
    assert result == mock_success
    assert batch_poster.folio_client.folio_get_async.call_count == 2
    # Should use exponential backoff (2^0 = 1s)
    mock_sleep.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_get_with_retry_params_default_to_empty_dict():
    """Test that params defaults to empty dict when None."""
    batch_poster = create_batch_poster()
    
    mock_response = {"instances": []}
    batch_poster.folio_client.folio_get_async = AsyncMock(return_value=mock_response)
    
    result = await batch_poster.get_with_retry("/test")  # No params provided
    
    assert result == mock_response
    # Should be called with empty dict for params
    batch_poster.folio_client.folio_get_async.assert_called_once_with(
        "/test", query_params={}
    )
