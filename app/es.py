import logging
from typing import Any, Dict, Optional
import httpx
from elasticsearch import AsyncElasticsearch, exceptions
from elastic_transport import HttpxAsyncHttpNode

from app.config import settings
from app.http_pool import get_shared_transport

logger = logging.getLogger("es_service")

es_client: Optional[AsyncElasticsearch] = None

# 기본 인덱스 매핑
DEFAULT_MAPPINGS: Dict[str, Any] = {
    "properties": {
        "title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "content": {"type": "text"},
        "author": {"type": "keyword"},
        "tags": {"type": "keyword"},
        "created_at": {"type": "date"},
        "extra": {"type": "object", "dynamic": True},
    }
}


class SharedPoolHttpxNode(HttpxAsyncHttpNode):
    """
    http_pool.py에서 관리하는 shared_transport 커넥션 풀을 그대로 공유하는 Elasticsearch HTTP 노드
    """
    def __init__(self, config):
        super().__init__(config)
        transport = get_shared_transport()
        if transport is not None:
            self.client = httpx.AsyncClient(
                base_url=f"{config.scheme}://{config.host}:{config.port}",
                transport=transport,  # http_pool의 공통 커넥션 풀 공유
                timeout=config.request_timeout,
            )

    async def close(self):
        # 공통 커넥션 풀(transport)은 http_pool.close_http_pool()에서 종료하므로 여기서는 유지
        pass


async def get_es() -> AsyncElasticsearch:
    """FastAPI Depends용 Elasticsearch 클라이언트 주입 함수"""
    if es_client is None:
        raise RuntimeError("Elasticsearch 클라이언트가 초기화되지 않았습니다.")
    return es_client


def _create_client() -> AsyncElasticsearch:
    """백엔드 설정(HTTP_CLIENT_BACKEND)에 따라 Elasticsearch 클라이언트 생성"""
    options: Dict[str, Any] = {
        "hosts": [settings.ELASTICSEARCH_URL],
        "request_timeout": settings.ES_REQUEST_TIMEOUT,
        "max_retries": 3,
        "retry_on_timeout": True,
    }
    if settings.HTTP_CLIENT_BACKEND == "httpx":
        logger.info("Elasticsearch 백엔드로 'httpx' 공유 커넥션 풀(SharedPoolHttpxNode)을 사용합니다.")
        options["node_class"] = SharedPoolHttpxNode
    else:
        logger.info("Elasticsearch 백엔드로 기본 'aiohttp'를 사용합니다.")
    return AsyncElasticsearch(**options)


async def init_es() -> AsyncElasticsearch:
    """앱 시작 시 Elasticsearch 클라이언트 초기화 및 기본 인덱스 확인"""
    global es_client

    es_client = _create_client()

    try:
        if await es_client.ping():
            logger.info("Elasticsearch에 성공적으로 연결되었습니다: %s", settings.ELASTICSEARCH_URL)
            await ensure_index_exists(settings.DEFAULT_INDEX)
        else:
            logger.warning("Elasticsearch ping 응답이 없습니다. (%s)", settings.ELASTICSEARCH_URL)
    except exceptions.ConnectionError as e:
        logger.warning("Elasticsearch 연결 실패: %s. 나중에 재시도됩니다.", e)

    return es_client


async def close_es():
    """앱 종료 시 Elasticsearch 클라이언트 세션 종료"""
    global es_client

    if es_client is not None:
        await es_client.close()
        es_client = None
        logger.info("Elasticsearch 클라이언트가 종료되었습니다.")


async def ensure_index_exists(index_name: Optional[str] = None):
    """기본 매핑과 함께 인덱스 생성 (없는 경우)"""
    if es_client is None:
        return

    target_index = index_name or settings.DEFAULT_INDEX
    try:
        if not await es_client.indices.exists(index=target_index):
            await es_client.indices.create(index=target_index, mappings=DEFAULT_MAPPINGS)
            logger.info("인덱스 '%s'가 생성되었습니다.", target_index)
    except Exception as e:
        logger.error("인덱스 확인/생성 중 오류 발생: %s", e)
