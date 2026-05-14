"""
Pytest configuration and fixtures for text completion tests
"""

import pytest
from unittest.mock import MagicMock, AsyncMock
from trustgraph.base import LlmResult


# === Common Fixtures for All Text Completion Models ===


@pytest.fixture
def base_processor_config():
    """Base configuration required by all processors"""
    return {"concurrency": 1, "taskgroup": AsyncMock(), "id": "test-processor"}


@pytest.fixture
def sample_llm_result():
    """Sample LlmResult for testing"""
    return LlmResult(text="Test response", in_token=10, out_token=5)


@pytest.fixture
def mock_async_processor_init():
    """Mock AsyncProcessor.__init__ to avoid infrastructure requirements"""
    mock = MagicMock()
    mock.return_value = None
    return mock


@pytest.fixture
def mock_llm_service_init():
    """Mock LlmService.__init__ to avoid infrastructure requirements"""
    mock = MagicMock()
    mock.return_value = None
    return mock


@pytest.fixture
def mock_prometheus_metrics():
    """Mock Prometheus metrics"""
    mock_metric = MagicMock()
    mock_metric.labels.return_value.time.return_value = MagicMock()
    return mock_metric


@pytest.fixture
def mock_pulsar_consumer():
    """Mock Pulsar consumer for integration testing"""
    return AsyncMock()


@pytest.fixture
def mock_pulsar_producer():
    """Mock Pulsar producer for integration testing"""
    return AsyncMock()


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing"""
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-openrouter-key")


@pytest.fixture
def mock_async_context_manager():
    """Mock async context manager for testing"""

    class MockAsyncContextManager:
        def __init__(self, return_value):
            self.return_value = return_value

        async def __aenter__(self):
            return self.return_value

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    return MockAsyncContextManager


# === OpenRouter Specific Fixtures ===


@pytest.fixture
def openrouter_processor_config(base_processor_config):
    """Default configuration for OpenRouter processor"""
    config = base_processor_config.copy()
    config.update(
        {
            "model": "openai/gpt-4o",
            "api_key": "test-openrouter-key",
            "url": "https://openrouter.ai/api/v1",
            "temperature": 0.0,
            "max_output": 4096,
        }
    )
    return config


@pytest.fixture
def mock_openrouter_client():
    """Mock OpenAI client for OpenRouter"""
    mock_client = MagicMock()

    mock_response = MagicMock()
    mock_response.choices = [MagicMock()]
    mock_response.choices[0].message.content = "Test response from OpenRouter"
    mock_response.usage.prompt_tokens = 15
    mock_response.usage.completion_tokens = 8

    mock_client.chat.completions.create.return_value = mock_response
    return mock_client


@pytest.fixture
def mock_openrouter_rate_limit_error():
    """Mock OpenRouter rate limit error"""
    from openai import RateLimitError

    return RateLimitError("Rate limit exceeded", response=MagicMock(), body=None)
