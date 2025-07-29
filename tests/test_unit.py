import pytest
import httpx
import json
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock, mock_open
from httpx import AsyncClient
from threading import Lock
from app.main import fetch_with_retry_and_limit, process_file
from aiolimiter import AsyncLimiter
from app.utils.CircuitBreaker import CircuitBreaker


# Unit tests for important functions in the data processing pipeline
# Example usage: pytest tests/test_unit.py

@pytest.mark.asyncio
async def test_fetch_with_retry_and_limit_success():
    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json = AsyncMock(
        return_value={'products': [], 'total': 194, 'skip': 200, 'limit': 0})

    with patch("httpx.AsyncClient.get", return_value=mock_response):
        limiter = AsyncLimiter(5, 1)  # Allow 5 requests per second
        circuit_breaker = CircuitBreaker(
            failure_threshold=2, recovery_timeout=5)

        # Call the function
        response = await fetch_with_retry_and_limit(
            client=AsyncClient(),  # Use AsyncClient to match async behavior
            url="https://dummyjson.com/products?limit=20&skip=200",
            headers={},
            limiter=limiter,
            retries=3,
            circuit_breaker=circuit_breaker
        )

        # Assertions
        assert response.status_code == 200
        assert await response.json() == {'products': [], 'total': 194, 'skip': 200, 'limit': 0}


@pytest.mark.asyncio
async def test_circuit_breaker_usage():
    circuit_breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=5)
    assert circuit_breaker.state == CircuitBreaker.CLOSE_STATE

    circuit_breaker._on_failure()
    circuit_breaker._on_failure()

    # The state should now be OPEN after 2 failures
    assert circuit_breaker.state == CircuitBreaker.OPEN_STATE

    circuit_breaker._on_success()

    # The state should be CLOSED after success
    assert circuit_breaker.state == CircuitBreaker.CLOSE_STATE

    circuit_breaker._on_failure()
    circuit_breaker._on_failure()

    # The state should be OPEN after 2 additional failures
    assert circuit_breaker.state == CircuitBreaker.OPEN_STATE


@pytest.mark.asyncio
async def test_async_limiter():
    limiter = AsyncLimiter(1, 1)  # Limit to 1 request per second

    # A simple async function that will use the limiter
    async def fetch_data():
        async with limiter:
            await asyncio.sleep(0.1)

    # Call the fetch_data function concurrently 3 times
    start_time = asyncio.get_event_loop().time()
    tasks = [fetch_data(), fetch_data(), fetch_data()]
    await asyncio.gather(*tasks)
    end_time = asyncio.get_event_loop().time()

    # Check the elapsed time, it should be at least 2 seconds because we only allow 1 request per second
    elapsed_time = end_time - start_time
    assert elapsed_time >= 2, f"Expected at least 2 seconds, but took {elapsed_time} seconds"


@pytest.fixture
def mock_product_list():
    return []


@pytest.fixture
def mock_lock():
    return Lock()


@pytest.fixture
def mock_filepath():
    return "source1_12345.json"


@pytest.fixture
def mock_raw_data():
    return [
        {"id": 1, "title": "Product 1", "price": 100, "category": "Category 1"},
        {"id": 2, "name": "Product 2", "price": 200, "category": "Category 2"},
        {"id": 3, "first_name": "John", "last_name": "Doe", "price": 300, "category": "Category 3"}
    ]


@patch("builtins.open", new_callable=mock_open)
@patch("os.remove")
@patch("logging.debug")
@patch("json.load")
def test_process_file(mock_json_load, mock_logging, mock_remove, mock_open_func, mock_product_list, mock_lock, mock_filepath, mock_raw_data):
    # mock JSON loading
    mock_json_load.return_value = mock_raw_data

    # mock progress bar
    mock_pbar = MagicMock()
    file_index = 0
    total_files = 2

    process_file(mock_filepath, mock_product_list, mock_lock, file_index, total_files, mock_pbar)

    # Assert products were added correctly
    assert len(mock_product_list) == 3
    assert mock_product_list[0].id == "1"
    assert mock_product_list[1].id == "2"
    assert mock_product_list[2].id == "3"
    assert mock_product_list[0].title == "Product 1"
    assert mock_product_list[1].title == "Product 2"
    assert mock_product_list[2].title == "John Doe"
    assert all(p.source == "source1" for p in mock_product_list)

    # Assert file was removed
    mock_remove.assert_called_once_with(mock_filepath)

    # Assert progress bar updated
    mock_pbar.update.assert_called_once_with(1)
    mock_pbar.set_postfix.assert_called_once()

    # Ensure logging was used
    assert mock_logging.call_count >= 3