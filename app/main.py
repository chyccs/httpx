import asyncio
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from functools import lru_cache
import logging
from pathlib import Path
import time
from typing import Iterator, Optional

import httpx
from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from elasticsearch import AsyncElasticsearch, NotFoundError

from app.config import settings
from app.es import init_es, close_es, get_es, ensure_index_exists
from app.http_pool import (
    init_http_pool,
    close_http_pool,
    get_http_client,
    get_shared_transport,
    get_pool_status,
)
from app.metrics import init_metrics, get_tcp_connection_details
from app.models import (
    DocumentCreate,
    DocumentUpdate,
    DocumentResponse,
    DocumentCreateResponse,
    SearchResponse,
    ConcurrentSearchResponse,
    CustomConcurrentSearchRequest,
    AggregationResponse,
    AggregationItem,
)
from app.search import (
    CONCURRENT_QUERY_COUNT,
    SORT_BY_CREATED_DESC,
    SearchSpec,
    elapsed_ms,
    empty_concurrent_response,
    filtered_query,
    multi_strategy_specs,
    offset_chunk_specs,
    run_concurrent_search,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("es_api")

DASHBOARD_HTML_PATH = Path(__file__).parent / "templates" / "dashboard.html"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. 서버 전역 HTTP 커넥션 풀 초기화
    await init_http_pool()
    # 2. Elasticsearch 클라이언트 초기화 (http_pool의 shared_transport 주입)
    await init_es()
    yield
    # 3. 종료 시 역순으로 안전하게 연결 종료 (Graceful Shutdown)
    await close_es()
    await close_http_pool()


app = FastAPI(
    title="Elasticsearch FastAPI Service",
    description="Elasticsearch 문서 추가 및 조회 API 서비스",
    version="1.0.0",
    lifespan=lifespan,
)

# Prometheus 메트릭 Exporter 등록 (/metrics)
init_metrics(app)


def _index_query(description: str = "대상 인덱스명"):
    """엔드포인트 공통 index 쿼리 파라미터"""
    return Query(settings.DEFAULT_INDEX, description=description)


@contextmanager
def es_errors(action: str, not_found_detail: Optional[str] = None) -> Iterator[None]:
    """
    Elasticsearch 호출 예외를 HTTP 응답으로 변환한다.
    - NotFoundError -> 404 (not_found_detail 지정 시)
    - 그 외 예외   -> 500
    """
    try:
        yield
    except HTTPException:
        raise
    except NotFoundError:
        if not_found_detail is None:
            raise
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=not_found_detail)
    except Exception as e:
        logger.error("%s 실패: %s", action, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"{action} 중 오류가 발생했습니다: {e}",
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@lru_cache
def _load_dashboard_html() -> str:
    return DASHBOARD_HTML_PATH.read_text(encoding="utf-8")


@app.get("/", response_class=HTMLResponse, tags=["Web UI"], summary="동시 검색 시각화 웹 대시보드")
async def web_dashboard():
    """브라우저에서 동시 5회 검색 및 결과 병합을 시각적으로 테스트할 수 있는 대시보드"""
    return HTMLResponse(content=_load_dashboard_html())


@app.get("/health", tags=["Health"])
async def health_check(es: AsyncElasticsearch = Depends(get_es)):
    """Elasticsearch 클러스터 연결 및 헬스 상태 확인"""
    try:
        health = await es.cluster.health()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Elasticsearch 연결 실패: {e}",
        )
    return {
        "status": "healthy",
        "elasticsearch_cluster": health.get("cluster_name"),
        "cluster_status": health.get("status"),
    }


def _es_node_transport(es: AsyncElasticsearch) -> Optional[httpx.AsyncHTTPTransport]:
    """AsyncElasticsearch 내부 첫 번째 노드가 사용하는 httpx transport 추출"""
    try:
        nodes = es._transport.node_pool.all()
        node = nodes[0] if isinstance(nodes, (list, tuple)) and nodes else None
        return getattr(getattr(node, "client", None), "_transport", None)
    except Exception:
        return None


@app.get("/test-shared-pool", tags=["Health"], summary="httpx 공유 커넥션 풀 검증 테스트")
async def test_shared_pool(
    es: AsyncElasticsearch = Depends(get_es),
    http_client: httpx.AsyncClient = Depends(get_http_client),
):
    """
    방법 2 검증: 서버의 httpx 클라이언트와 Elasticsearch가
    동일한 소켓 커넥션 풀(AsyncHTTPTransport)을 공유하고 있는지 검증합니다.
    """
    # 1. 서버의 공유 httpx 클라이언트로 ES 루트 직접 호출
    http_res = await http_client.get(settings.ELASTICSEARCH_URL.rstrip("/") + "/")
    http_data = http_res.json()

    # 2. AsyncElasticsearch 클라이언트로 호출
    es_ping = await es.ping()
    es_health = await es.cluster.health()

    # 3. 내부 커넥션 풀(AsyncHTTPTransport) 인스턴스 메모리 주소 비교
    shared_transport = get_shared_transport()
    es_transport = _es_node_transport(es)
    is_identical_pool = shared_transport is not None and es_transport is shared_transport

    return {
        "configured_backend": settings.HTTP_CLIENT_BACKEND,
        "is_same_connection_pool": is_identical_pool,
        "shared_transport_id": hex(id(shared_transport)) if shared_transport else None,
        "es_internal_transport_id": hex(id(es_transport)) if es_transport else None,
        "httpx_client_test": {
            "status_code": http_res.status_code,
            "cluster_name": http_data.get("cluster_name"),
            "message": "서버의 shared httpx 클라이언트로 직접 호출 성공",
        },
        "elasticsearch_client_test": {
            "ping": es_ping,
            "cluster_name": es_health.get("cluster_name"),
            "message": "AsyncElasticsearch 클라이언트로 호출 성공",
        },
        "verification_result": (
            "🎉 검증 성공: 서버의 httpx 클라이언트와 Elasticsearch가 100% 동일한 커넥션 풀(소켓 세션)을 공유하고 있습니다!"
            if is_identical_pool
            else "공유 풀이 아닙니다."
        ),
    }


@app.get("/test-pool-limit", tags=["Health"], summary="실시간 커넥션 풀 동시성 및 소켓 수 제한 검증")
async def test_pool_limit(
    concurrency: int = Query(10, ge=1, le=50, description="동시에 발생시킬 요청 수"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    지정된 개수(예: 10개)의 Elasticsearch 쿼리를 동시에 실행하면서
    실제 풀 내부의 활성 소켓 수가 max_connections 이하로 엄격하게 유지되는지 실시간 측정합니다.
    """
    initial_status = get_pool_status()
    observed_connections = []
    start = time.perf_counter()

    async def single_task():
        await es.search(index=settings.DEFAULT_INDEX, size=1)
        observed_connections.append(get_pool_status().get("current_active_connections", 0))

    await asyncio.gather(*(single_task() for _ in range(concurrency)))

    peak_connections = max(observed_connections, default=0)
    max_limit = initial_status.get("max_connections")
    is_constrained = max_limit is None or peak_connections <= max_limit

    return {
        "concurrency_tested": concurrency,
        "elapsed_time_ms": elapsed_ms(start),
        "max_connections_limit": max_limit,
        "peak_active_connections": peak_connections,
        "initial_pool_status": initial_status,
        "final_pool_status": get_pool_status(),
        "is_strictly_constrained": is_constrained,
        "message": (
            f"✔ 검증 성공: {concurrency}개의 동시 요청이 몰려도 활성 소켓이 최대 {peak_connections}개로 제한되었습니다."
            if is_constrained
            else f"❌ 한도 초과: {peak_connections}개가 열렸습니다."
        ),
    }


@app.get("/tcp-connections", tags=["Monitoring"], summary="현재 프로세스의 상태별 TCP 커넥션 상세 조회")
async def get_tcp_connections():
    """
    현재 FastAPI 프로세스가 맺고 있는 모든 TCP 소켓의 상태(ESTABLISHED, LISTEN, TIME_WAIT 등)와
    로컬/원격 주소 상세 정보를 반환합니다.
    """
    return get_tcp_connection_details()


@app.post(
    "/documents",
    response_model=DocumentCreateResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Documents"],
    summary="문서 추가 (색인)",
)
async def create_document(
    doc: DocumentCreate,
    doc_id: Optional[str] = Query(None, description="지정할 문서 ID (생략 시 자동 생성)"),
    index: str = _index_query("색인할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    Elasticsearch에 새로운 문서를 추가(인덱싱)합니다.
    """
    await ensure_index_exists(index)

    body = {**doc.model_dump(), "created_at": _now_iso()}

    with es_errors("문서 추가"):
        # id=None 이면 elasticsearch 클라이언트가 파라미터를 생략하여 ID가 자동 생성된다.
        res = await es.index(index=index, id=doc_id, document=body, refresh=True)

    return DocumentCreateResponse(
        id=res["_id"],
        index=res["_index"],
        result=res.get("result", "created"),
        message="문서가 성공적으로 색인되었습니다.",
    )


@app.get(
    "/documents",
    response_model=SearchResponse,
    tags=["Documents"],
    summary="문서 검색 및 목록 조회",
)
async def search_documents(
    q: Optional[str] = Query(None, description="검색 키워드 (제목 및 내용 대상 검색, 생략 시 전체 조회)"),
    tag: Optional[str] = Query(None, description="태그 필터"),
    author: Optional[str] = Query(None, description="작성자 필터"),
    page: int = Query(1, ge=1, description="페이지 번호 (1부터 시작)"),
    size: int = Query(10, ge=1, le=100, description="페이지 당 결과 수"),
    index: str = _index_query("조회할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    키워드 검색, 태그/작성자 필터 및 페이지네이션을 지원하는 문서 목록 조회 API입니다.
    """
    search_kwargs = {
        "index": index,
        "query": filtered_query(q, tag, author),
        "from_": (page - 1) * size,
        "size": size,
        "sort": [SORT_BY_CREATED_DESC],
    }
    if q:
        mark = {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]}
        search_kwargs["highlight"] = {"fields": {"title": mark, "content": mark}}

    with es_errors("문서 검색"):
        if not await es.indices.exists(index=index):
            return SearchResponse(total=0, hits=[])
        res = await es.search(**search_kwargs)

    hits = [
        DocumentResponse(
            id=hit["_id"],
            index=hit["_index"],
            source=hit["_source"],
            highlight=hit.get("highlight"),
        )
        for hit in res["hits"]["hits"]
    ]
    return SearchResponse(total=res["hits"]["total"]["value"], hits=hits)


@app.get(
    "/documents/aggregations",
    response_model=AggregationResponse,
    tags=["Documents"],
    summary="태그 및 작성자 통계 집계",
)
async def get_document_aggregations(
    index: str = _index_query(),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    Elasticsearch의 terms 집계 기능을 사용하여
    인기 태그 Top 10과 작성자별 문서 수를 실시간 집계합니다.
    """
    if not await es.indices.exists(index=index):
        return AggregationResponse(total_documents=0, top_tags=[], top_authors=[])

    with es_errors("집계 조회"):
        res = await es.search(
            index=index,
            size=0,
            aggregations={
                "top_tags": {"terms": {"field": "tags", "size": 10}},
                "top_authors": {"terms": {"field": "author", "size": 10}},
            },
        )

    aggs = res.get("aggregations", {})

    def buckets(name: str):
        return [
            AggregationItem(key=b["key"], doc_count=b["doc_count"])
            for b in aggs.get(name, {}).get("buckets", [])
        ]

    return AggregationResponse(
        total_documents=res["hits"]["total"]["value"],
        top_tags=buckets("top_tags"),
        top_authors=buckets("top_authors"),
    )


@app.get(
    "/documents/concurrent-search",
    response_model=ConcurrentSearchResponse,
    tags=["Documents"],
    summary="5회 동시(병렬) 검색 및 결과 병합",
)
async def concurrent_search_documents(
    q: Optional[str] = Query(None, description="검색 키워드 (생략 시 전체 문서 대상)"),
    mode: str = Query(
        "offset",
        pattern="^(offset|strategy)$",
        description="동시 조회 모드: 'offset'(오프셋을 나눠 5회 병렬 수집) 또는 'strategy'(5가지 검색 전략 동시 수행)",
    ),
    size_per_query: int = Query(5, ge=1, le=50, description="각 쿼리당 조회할 문서 건수"),
    index: str = _index_query(),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    Elasticsearch에 비동기로 동시에 5번의 조회를 요청(asyncio.gather)하고,
    그 결과를 중복 제거 및 점수 순으로 병합하여 반환하는 API입니다.
    """
    if not await es.indices.exists(index=index):
        return empty_concurrent_response(CONCURRENT_QUERY_COUNT)

    build_specs = offset_chunk_specs if mode == "offset" else multi_strategy_specs
    return await run_concurrent_search(es, index, build_specs(q, size_per_query))


@app.post(
    "/documents/concurrent-search",
    response_model=ConcurrentSearchResponse,
    tags=["Documents"],
    summary="사용자 지정 다중 쿼리 동시(병렬) 검색 및 결과 병합",
)
async def custom_concurrent_search_documents(
    payload: CustomConcurrentSearchRequest,
    index: str = _index_query(),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    사용자가 직접 지정한 N개(예: 5개)의 서로 다른 검색 조건을 동시에 병렬로 조회하고
    결과를 중복 제거 및 점수 순으로 병합하여 반환합니다.
    """
    if not await es.indices.exists(index=index):
        return empty_concurrent_response(len(payload.queries))

    specs = [
        SearchSpec(
            strategy=item.label or f"custom_query_{idx}",
            query=filtered_query(item.q, item.tag, item.author),
            size=item.size,
            sort=["_score", SORT_BY_CREATED_DESC],
        )
        for idx, item in enumerate(payload.queries, start=1)
    ]
    return await run_concurrent_search(es, index, specs)


@app.get(
    "/documents/{doc_id}",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="문서 단건 조회 (ID)",
)
async def get_document_by_id(
    doc_id: str,
    index: str = _index_query("조회할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    문서 ID를 통해 특정 문서를 단건 조회합니다.
    """
    not_found = f"ID '{doc_id}'에 해당하는 문서를 인덱스 '{index}'에서 찾을 수 없습니다."
    with es_errors("문서 조회", not_found_detail=not_found):
        res = await es.get(index=index, id=doc_id)

    return DocumentResponse(id=res["_id"], index=res["_index"], source=res["_source"])


@app.put(
    "/documents/{doc_id}",
    response_model=DocumentCreateResponse,
    tags=["Documents"],
    summary="문서 수정 (부분 업데이트)",
)
async def update_document(
    doc_id: str,
    doc_update: DocumentUpdate,
    index: str = _index_query("수정할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """문서 ID에 해당하는 문서의 필드를 부분 업데이트합니다."""
    update_data = doc_update.model_dump(exclude_none=True)
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="수정할 필드가 최소 1개 이상 필요합니다.",
        )
    update_data["updated_at"] = _now_iso()

    with es_errors("문서 수정", not_found_detail=f"ID '{doc_id}' 문서를 찾을 수 없습니다."):
        res = await es.update(index=index, id=doc_id, doc=update_data, refresh=True)

    return DocumentCreateResponse(
        id=res["_id"],
        index=res["_index"],
        result=res.get("result", "updated"),
        message="문서가 성공적으로 수정되었습니다.",
    )


@app.delete(
    "/documents/{doc_id}",
    response_model=DocumentCreateResponse,
    tags=["Documents"],
    summary="문서 삭제",
)
async def delete_document(
    doc_id: str,
    index: str = _index_query("삭제할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """문서 ID로 문서를 삭제합니다."""
    with es_errors("문서 삭제", not_found_detail=f"ID '{doc_id}' 문서를 찾을 수 없습니다."):
        res = await es.delete(index=index, id=doc_id, refresh=True)

    return DocumentCreateResponse(
        id=res["_id"],
        index=res["_index"],
        result=res.get("result", "deleted"),
        message="문서가 삭제되었습니다.",
    )
