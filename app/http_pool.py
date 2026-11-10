import logging
from typing import Optional
import httpx
from app.config import settings

logger = logging.getLogger("http_pool")

shared_transport: Optional[httpx.AsyncHTTPTransport] = None
shared_http_client: Optional[httpx.AsyncClient] = None


async def init_http_pool() -> httpx.AsyncClient:
    """서버 전역에서 공유하는 httpx 커넥션 풀 및 클라이언트 초기화"""
    global shared_transport, shared_http_client

    if shared_transport is None:
        shared_transport = httpx.AsyncHTTPTransport(
            limits=httpx.Limits(
                max_connections=settings.ES_MAX_CONNECTIONS,
                max_keepalive_connections=settings.ES_MAX_KEEPALIVE_CONNECTIONS,
            ),
            retries=1,
        )
        shared_http_client = httpx.AsyncClient(
            transport=shared_transport,
            timeout=settings.ES_REQUEST_TIMEOUT,
        )
        logger.info(
            "공유 HTTP 커넥션 풀이 초기화되었습니다. (max_connections=%d, keepalive=%d)",
            settings.ES_MAX_CONNECTIONS,
            settings.ES_MAX_KEEPALIVE_CONNECTIONS,
        )

    return shared_http_client


async def close_http_pool():
    """앱 종료 시 공유 httpx 클라이언트 및 커넥션 풀(Transport) 종료"""
    global shared_transport, shared_http_client

    if shared_http_client is not None:
        await shared_http_client.aclose()
        shared_http_client = None
        logger.info("공유 httpx 클라이언트가 종료되었습니다.")

    if shared_transport is not None:
        await shared_transport.aclose()
        shared_transport = None
        logger.info("공유 HTTP 커넥션 풀(Transport)이 종료되었습니다.")


async def get_http_client() -> httpx.AsyncClient:
    """FastAPI Depends 주입용 공통 httpx 클라이언트 반환 함수"""
    if shared_http_client is None:
        raise RuntimeError("공유 httpx 클라이언트가 초기화되지 않았습니다.")
    return shared_http_client


def get_shared_transport() -> Optional[httpx.AsyncHTTPTransport]:
    """공통 커넥션 풀(Transport) 객체 반환 함수"""
    return shared_transport


def get_pool_status() -> dict:
    """현재 커넥션 풀의 내부 활성 소켓 상태 실시간 조회"""
    if shared_transport is None:
        return {"status": "uninitialized"}

    pool = getattr(shared_transport, "_pool", None)
    if pool is None:
        return {"status": "pool_not_ready"}

    connections = getattr(pool, "_connections", [])
    return {
        "max_connections": getattr(pool, "_max_connections", None),
        "max_keepalive_connections": getattr(pool, "_max_keepalive_connections", None),
        "current_active_connections": len(connections),
    }

