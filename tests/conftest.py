from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.es import get_es
from app.http_pool import get_http_client


@pytest.fixture
def mock_es():
    mock = AsyncMock()
    # 기본 cluster.health 응답
    mock.cluster.health.return_value = {
        "cluster_name": "test-cluster",
        "status": "green",
    }
    # 기본 indices.exists 응답
    mock.indices.exists.return_value = True
    return mock


@pytest.fixture
def client(mock_es):
    mock_http_client = AsyncMock()
    mock_http_response = AsyncMock()
    mock_http_response.status_code = 200
    mock_http_response.json = lambda: {"cluster_name": "test-cluster"}
    mock_http_client.get.return_value = mock_http_response

    app.dependency_overrides[get_es] = lambda: mock_es
    app.dependency_overrides[get_http_client] = lambda: mock_http_client

    with (
        patch("app.main.init_es", new_callable=AsyncMock),
        patch("app.main.close_es", new_callable=AsyncMock),
        patch("app.main.init_http_pool", new_callable=AsyncMock),
        patch("app.main.close_http_pool", new_callable=AsyncMock),
    ):
        with TestClient(app) as test_client:
            yield test_client
    app.dependency_overrides.clear()
