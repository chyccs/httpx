from unittest.mock import MagicMock
from elasticsearch import NotFoundError


def test_health_check(client, mock_es):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["elasticsearch_cluster"] == "test-cluster"


def test_create_document(client, mock_es):
    mock_es.index.return_value = {
        "_id": "test-doc-id-123",
        "_index": "documents",
        "result": "created",
    }

    payload = {
        "title": "테스트 제목",
        "content": "테스트 본문 내용입니다.",
        "author": "테스터",
        "tags": ["test", "elasticsearch"],
    }
    response = client.post("/documents", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["id"] == "test-doc-id-123"
    assert data["result"] == "created"


def test_get_document_success(client, mock_es):
    mock_es.get.return_value = {
        "_id": "test-doc-id-123",
        "_index": "documents",
        "_source": {
            "title": "테스트 제목",
            "content": "테스트 본문 내용입니다.",
            "author": "테스터",
            "tags": ["test"],
            "created_at": "2026-11-10T00:00:00Z",
        },
    }

    response = client.get("/documents/test-doc-id-123")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "test-doc-id-123"
    assert data["source"]["title"] == "테스트 제목"


def test_get_document_not_found(client, mock_es):
    mock_es.get.side_effect = NotFoundError(
        message="Document not found", meta=None, body={}
    )

    response = client.get("/documents/non-existent-id")
    assert response.status_code == 404
    assert "찾을 수 없습니다" in response.json()["detail"]


def test_update_document(client, mock_es):
    mock_es.update.return_value = {
        "_id": "test-doc-id-123",
        "_index": "documents",
        "result": "updated",
    }

    update_payload = {"title": "수정된 제목"}
    response = client.put("/documents/test-doc-id-123", json=update_payload)
    assert response.status_code == 200
    data = response.json()
    assert data["result"] == "updated"


def test_search_documents(client, mock_es):
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "hits": [
                {
                    "_id": "test-doc-id-123",
                    "_index": "documents",
                    "_source": {
                        "title": "테스트 제목",
                        "content": "테스트 내용",
                        "author": "테스터",
                    },
                }
            ],
        }
    }

    response = client.get("/documents?q=테스트")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["hits"]) == 1
    assert data["hits"][0]["id"] == "test-doc-id-123"


def test_delete_document(client, mock_es):
    mock_es.delete.return_value = {
        "_id": "test-doc-id-123",
        "_index": "documents",
        "result": "deleted",
    }

    response = client.delete("/documents/test-doc-id-123")
    assert response.status_code == 200
    data = response.json()
    assert data["result"] == "deleted"



def test_concurrent_search_offset_mode(client, mock_es):
    # 5번의 검색에 대해 각각 문서 1건씩 반환하도록 Mock 설정
    def side_effect(*args, **kwargs):
        offset = kwargs.get("from_", 0)
        return {
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [
                    {
                        "_id": f"doc-offset-{offset}",
                        "_index": "documents",
                        "_score": 1.0,
                        "_source": {
                            "title": f"문서 offset {offset}",
                            "content": "내용",
                            "created_at": "2026-11-10T10:00:00Z",
                        },
                    }
                ],
            }
        }

    mock_es.search.side_effect = side_effect

    response = client.get("/documents/concurrent-search?q=test&mode=offset&size_per_query=5")
    assert response.status_code == 200
    data = response.json()
    assert data["total_queries"] == 5
    assert data["total_hits_before_dedup"] == 5
    assert data["total_unique_hits"] == 5
    assert len(data["query_stats"]) == 5
    assert len(data["hits"]) == 5


def test_concurrent_search_deduplication(client, mock_es):
    # 동일한 문서가 5번 모두에서 발견되는 경우 (중복 제거 검증)
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "hits": [
                {
                    "_id": "duplicate-doc-id",
                    "_index": "documents",
                    "_score": 2.5,
                    "_source": {
                        "title": "공통 문서",
                        "content": "공통 내용",
                        "created_at": "2026-11-10T10:00:00Z",
                    },
                }
            ],
        }
    }

    response = client.get("/documents/concurrent-search?q=common&mode=strategy")
    assert response.status_code == 200
    data = response.json()
    assert data["total_queries"] == 5
    assert data["total_hits_before_dedup"] == 5
    # 중복 제거되어 최종 유니크 문서는 1개여야 함
    assert data["total_unique_hits"] == 1
    assert data["hits"][0]["id"] == "duplicate-doc-id"
    # 5개 쿼리 모두에서 발견되었는지 확인
    assert data["hits"][0]["found_in_queries"] == [1, 2, 3, 4, 5]



def test_custom_concurrent_search(client, mock_es):
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "hits": [
                {
                    "_id": "custom-doc-1",
                    "_index": "documents",
                    "_score": 3.0,
                    "_source": {
                        "title": "커스텀 문서",
                        "content": "내용",
                        "created_at": "2026-11-10T10:00:00Z",
                    },
                }
            ],
        }
    }

    payload = {
        "queries": [
            {"q": "쿼리1", "label": "첫번째 쿼리"},
            {"q": "쿼리2", "label": "두번째 쿼리"},
            {"tag": "태그3", "label": "세번째 쿼리"},
            {"author": "홍길동", "label": "네번째 쿼리"},
            {"q": "쿼리5", "label": "다섯번째 쿼리"},
        ]
    }
    response = client.post("/documents/concurrent-search", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["total_queries"] == 5
    assert len(data["query_stats"]) == 5
    assert data["total_unique_hits"] == 1
    assert data["hits"][0]["found_in_queries"] == [1, 2, 3, 4, 5]



def test_shared_pool_endpoint(client, mock_es):
    mock_es.cluster.health.return_value = {"cluster_name": "test-cluster"}
    mock_es.ping.return_value = True

    # mock_es._transport 구조 모킹 (node_pool.all()은 동기 메서드이므로 MagicMock 사용)
    mock_node = MagicMock()
    mock_node.client._transport = None
    mock_es._transport = MagicMock()
    mock_es._transport.node_pool.all.return_value = [mock_node]

    response = client.get("/test-shared-pool")
    assert response.status_code == 200
    data = response.json()
    assert "configured_backend" in data
    assert "is_same_connection_pool" in data



def test_pool_limit_endpoint(client, mock_es):
    mock_es.search.return_value = {"hits": {"total": {"value": 1}, "hits": []}}

    response = client.get("/test-pool-limit?concurrency=5")
    assert response.status_code == 200
    data = response.json()
    assert data["concurrency_tested"] == 5
    assert "peak_active_connections" in data



def test_metrics_endpoint(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    content = response.text
    assert "http_pool_active_connections" in content
    assert "http_pool_max_connections" in content



def test_tcp_connections_endpoint(client):
    response = client.get("/tcp-connections")
    assert response.status_code == 200
    data = response.json()
    assert "summary" in data
    assert "outbound_total" in data["summary"]
    assert "inbound_total" in data["summary"]



def test_pydantic_settings():
    from app.config import Settings
    # 기본값 및 오버라이드 검증
    s = Settings(
        ELASTICSEARCH_URL="http://test-host:9200",
        ES_MAX_CONNECTIONS="30",
    )
    assert s.ELASTICSEARCH_URL == "http://test-host:9200"
    assert s.ES_MAX_CONNECTIONS == 30
    assert s.DEFAULT_INDEX == "documents"
    assert s.HTTP_CLIENT_BACKEND == "httpx"
