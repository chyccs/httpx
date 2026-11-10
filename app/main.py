import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import time
from typing import Any, Dict, List, Optional
import httpx
from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from elasticsearch import AsyncElasticsearch, exceptions, NotFoundError

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
    ConcurrentSearchHit,
    QueryStat,
    CustomConcurrentSearchRequest,
    CustomSearchQuery,
    AggregationResponse,
    AggregationItem,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("es_api")


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


@app.get("/", response_class=HTMLResponse, tags=["Web UI"], summary="동시 검색 시각화 웹 대시보드")
async def web_dashboard():
    """브라우저에서 동시 5회 검색 및 결과 병합을 시각적으로 테스트할 수 있는 대시보드"""
    html_content = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Elasticsearch 동시 5회 검색 대시보드</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    </head>
    <body class="bg-slate-900 text-slate-100 min-h-screen font-sans">
        <div class="max-w-5xl mx-auto px-4 py-8">
            <!-- Header -->
            <header class="flex justify-between items-center mb-8 border-b border-slate-800 pb-4">
                <div>
                    <h1 class="text-2xl font-bold bg-gradient-to-r from-blue-400 via-indigo-400 to-purple-400 bg-clip-text text-transparent">
                        <i class="fa-solid fa-bolt mr-2 text-yellow-400"></i>Elasticsearch 동시 5회 검색 & 병합
                    </h1>
                    <p class="text-sm text-slate-400 mt-1">Python asyncio.gather 병렬 검색 & 결과 중복 제거 실시간 시각화</p>
                </div>
                <div class="flex items-center space-x-3">
                    <span id="health-badge" class="px-3 py-1 bg-green-900/50 text-green-300 border border-green-700/50 rounded-full text-xs font-semibold flex items-center">
                        <span class="w-2 h-2 bg-green-400 rounded-full animate-ping mr-2"></span>ES Connected
                    </span>
                    <a href="/docs" target="_blank" class="text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 px-3 py-1.5 rounded-lg border border-slate-700 transition">
                        <i class="fa-solid fa-book mr-1"></i>Swagger Docs
                    </a>
                </div>
            </header>

            <!-- Search Panel -->
            <div class="bg-slate-800/80 rounded-2xl p-6 border border-slate-700 shadow-xl mb-8 backdrop-blur-sm">
                <form id="search-form" onsubmit="event.preventDefault(); runSearch();" class="space-y-4">
                    <div class="flex flex-col sm:flex-row gap-3">
                        <div class="relative flex-1">
                            <i class="fa-solid fa-magnifying-glass absolute left-4 top-1/2 -translate-y-1/2 text-slate-400"></i>
                            <input type="text" id="query-input" placeholder="검색어를 입력하세요 (예: FastAPI, Elasticsearch, 비동기...)"
                                class="w-full pl-11 pr-4 py-3 bg-slate-950 border border-slate-700 rounded-xl focus:outline-none focus:ring-2 focus:ring-blue-500 text-slate-200 placeholder-slate-500 text-sm">
                        </div>
                        <button type="submit" class="px-6 py-3 bg-blue-600 hover:bg-blue-500 text-white font-semibold rounded-xl text-sm transition flex items-center justify-center shadow-lg shadow-blue-600/30">
                            <i class="fa-solid fa-play mr-2"></i>동시 5회 검색 실행
                        </button>
                    </div>

                    <!-- Mode Selection -->
                    <div class="flex flex-wrap items-center justify-between gap-4 pt-2 text-xs text-slate-300">
                        <div class="flex items-center space-x-4">
                            <span class="font-medium text-slate-400">검색 모드:</span>
                            <label class="flex items-center space-x-1.5 cursor-pointer">
                                <input type="radio" name="search-mode" value="offset" checked class="text-blue-500 focus:ring-blue-500">
                                <span>오프셋 분할 병렬 수집 (Offset Chunks)</span>
                            </label>
                            <label class="flex items-center space-x-1.5 cursor-pointer">
                                <input type="radio" name="search-mode" value="strategy" class="text-blue-500 focus:ring-blue-500">
                                <span>5가지 다각도 검색 전략 (Multi Strategy)</span>
                            </label>
                        </div>
                        <div class="flex items-center space-x-2">
                            <span class="text-slate-400">쿼리당 개수:</span>
                            <select id="size-input" class="bg-slate-950 border border-slate-700 rounded-lg px-2.5 py-1 text-slate-200 focus:outline-none">
                                <option value="3">3건</option>
                                <option value="5" selected>5건</option>
                                <option value="10">10건</option>
                            </select>
                        </div>
                    </div>
                </form>
            </div>

            <!-- Stats Bar (Hidden initially) -->
            <div id="stats-section" class="hidden mb-6">
                <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
                    <div class="bg-slate-800/60 border border-slate-700/60 rounded-xl p-4 text-center">
                        <div class="text-xs text-slate-400">동시 실행 쿼리</div>
                        <div class="text-2xl font-bold text-blue-400 mt-1" id="stat-queries">5</div>
                    </div>
                    <div class="bg-slate-800/60 border border-slate-700/60 rounded-xl p-4 text-center">
                        <div class="text-xs text-slate-400">전체 소요 시간</div>
                        <div class="text-2xl font-bold text-emerald-400 mt-1" id="stat-time">0 ms</div>
                    </div>
                    <div class="bg-slate-800/60 border border-slate-700/60 rounded-xl p-4 text-center">
                        <div class="text-xs text-slate-400">수집 총 건수 (중복 포함)</div>
                        <div class="text-2xl font-bold text-amber-400 mt-1" id="stat-before">0</div>
                    </div>
                    <div class="bg-slate-800/60 border border-slate-700/60 rounded-xl p-4 text-center">
                        <div class="text-xs text-slate-400">최종 결과 건수 (중복 제거)</div>
                        <div class="text-2xl font-bold text-purple-400 mt-1" id="stat-after">0</div>
                    </div>
                </div>

                <!-- 5 Query Progress Cards -->
                <div class="mt-4">
                    <div class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-2">5개 병렬 쿼리 실행 결과</div>
                    <div class="grid grid-cols-1 sm:grid-cols-5 gap-2" id="query-chips"></div>
                </div>
            </div>

            <!-- Results Section -->
            <div id="results-container" class="space-y-3">
                <div class="text-center py-16 text-slate-500 border border-dashed border-slate-800 rounded-2xl">
                    <i class="fa-solid fa-layer-group text-4xl mb-3 text-slate-600"></i>
                    <p class="text-sm">검색어를 입력하고 '동시 5회 검색'을 클릭해 보세요.</p>
                </div>
            </div>
        </div>

        <script>
            async function runSearch() {
                const query = document.getElementById('query-input').value.trim();
                const mode = document.querySelector('input[name="search-mode"]:checked').value;
                const size = document.getElementById('size-input').value;
                const resultsContainer = document.getElementById('results-container');
                const statsSection = document.getElementById('stats-section');

                resultsContainer.innerHTML = `
                    <div class="text-center py-12 text-slate-400">
                        <i class="fa-solid fa-circle-notch fa-spin text-3xl text-blue-500 mb-3"></i>
                        <p class="text-sm">Elasticsearch에 5개의 비동기 쿼리를 동시에 전송 중...</p>
                    </div>
                `;

                try {
                    const url = `/documents/concurrent-search?q=${encodeURIComponent(query)}&mode=${mode}&size_per_query=${size}`;
                    const res = await fetch(url);
                    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
                    const data = await res.json();

                    // Update Stats
                    statsSection.classList.remove('hidden');
                    document.getElementById('stat-queries').textContent = data.total_queries;
                    document.getElementById('stat-time').textContent = data.elapsed_time_ms + ' ms';
                    document.getElementById('stat-before').textContent = data.total_hits_before_dedup;
                    document.getElementById('stat-after').textContent = data.total_unique_hits;

                    // Update 5 Query Chips
                    const queryChips = document.getElementById('query-chips');
                    queryChips.innerHTML = data.query_stats.map(q => `
                        <div class="bg-slate-950 border border-slate-800 rounded-lg p-2.5 text-xs">
                            <div class="flex justify-between text-slate-400">
                                <span>#${q.query_index} ${q.strategy}</span>
                                <span class="text-emerald-400">${q.took_ms}ms</span>
                            </div>
                            <div class="mt-1 text-slate-200 font-semibold">${q.hits_count}건 발견</div>
                        </div>
                    `).join('');

                    // Render Final Hits
                    if (data.hits.length === 0) {
                        resultsContainer.innerHTML = `
                            <div class="text-center py-12 text-slate-500 border border-dashed border-slate-800 rounded-2xl">
                                <p class="text-sm">일치하는 문서가 없습니다.</p>
                            </div>
                        `;
                        return;
                    }

                    resultsContainer.innerHTML = data.hits.map(hit => `
                        <div class="bg-slate-800/80 border border-slate-700 rounded-xl p-5 hover:border-slate-600 transition shadow-md">
                            <div class="flex justify-between items-start">
                                <h3 class="text-base font-semibold text-slate-100">${hit.source.title || '(제목 없음)'}</h3>
                                <div class="flex items-center space-x-2">
                                    ${hit.score ? `<span class="bg-blue-900/60 text-blue-300 border border-blue-700/60 text-[11px] font-bold px-2 py-0.5 rounded">Score ${hit.score.toFixed(2)}</span>` : ''}
                                    <span class="bg-purple-900/60 text-purple-300 border border-purple-700/60 text-[11px] px-2 py-0.5 rounded">
                                        발견: ${hit.found_in_queries.map(q => '#' + q).join(', ')} 쿼리
                                    </span>
                                </div>
                            </div>
                            <p class="text-sm text-slate-300 mt-2 leading-relaxed">${hit.source.content || ''}</p>
                            <div class="mt-4 flex flex-wrap items-center justify-between gap-2 pt-3 border-t border-slate-700/50 text-xs text-slate-400">
                                <div class="flex items-center space-x-2">
                                    <span><i class="fa-solid fa-user text-slate-500 mr-1"></i>${hit.source.author || '익명'}</span>
                                    <span>•</span>
                                    <span class="font-mono text-slate-500">ID: ${hit.id}</span>
                                </div>
                                <div class="flex items-center space-x-1.5">
                                    ${(hit.source.tags || []).map(tag => `<span class="bg-slate-900 text-slate-300 border border-slate-700/80 px-2 py-0.5 rounded text-[11px]">#${tag}</span>`).join('')}
                                </div>
                            </div>
                        </div>
                    `).join('');
                } catch (e) {
                    resultsContainer.innerHTML = `
                        <div class="p-4 bg-red-950/60 border border-red-800 rounded-xl text-red-300 text-sm">
                            <i class="fa-solid fa-triangle-exclamation mr-2"></i>검색 중 오류가 발생했습니다: ${e.message}
                        </div>
                    `;
                }
            }

            // Auto-load on initial view
            window.addEventListener('DOMContentLoaded', () => {
                runSearch();
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/health", tags=["Health"])
async def health_check(es: AsyncElasticsearch = Depends(get_es)):
    """Elasticsearch 클러스터 연결 및 헬스 상태 확인"""
    try:
        health = await es.cluster.health()
        return {
            "status": "healthy",
            "elasticsearch_cluster": health.get("cluster_name"),
            "cluster_status": health.get("status"),
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Elasticsearch 연결 실패: {str(e)}",
        )


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
    http_res = await http_client.get("http://localhost:9200/")
    http_data = http_res.json()

    # 2. AsyncElasticsearch 클라이언트로 호출
    es_ping = await es.ping()
    es_health = await es.cluster.health()

    # 3. 내부 커넥션 풀(AsyncHTTPTransport) 인스턴스 메모리 주소 비교
    shared_transport = get_shared_transport()

    # ES 내부의 node transport 인스턴스 추출
    try:
        nodes = es._transport.node_pool.all()
        node = nodes[0] if isinstance(nodes, (list, tuple)) else None
        es_transport = getattr(getattr(node, "client", None), "_transport", None)
    except Exception:
        es_transport = None

    is_identical_pool = (shared_transport is not None) and (es_transport is shared_transport)

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
    start_t = time.perf_counter()

    async def single_task(i: int):
        res = await es.search(index=settings.DEFAULT_INDEX, size=1)
        current = get_pool_status().get("current_active_connections", 0)
        observed_connections.append(current)
        return res

    tasks = [single_task(i) for i in range(concurrency)]
    await asyncio.gather(*tasks)
    elapsed_ms = round((time.perf_counter() - start_t) * 1000, 2)

    peak_connections = max(observed_connections) if observed_connections else 0
    final_status = get_pool_status()
    max_limit = initial_status.get("max_connections")

    is_constrained = (max_limit is None) or (peak_connections <= max_limit)

    return {
        "concurrency_tested": concurrency,
        "elapsed_time_ms": elapsed_ms,
        "max_connections_limit": max_limit,
        "peak_active_connections": peak_connections,
        "initial_pool_status": initial_status,
        "final_pool_status": final_status,
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
    index: str = Query(settings.DEFAULT_INDEX, description="색인할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    Elasticsearch에 새로운 문서를 추가(인덱싱)합니다.
    """
    await ensure_index_exists(index)

    body = doc.model_dump()
    body["created_at"] = datetime.now(timezone.utc).isoformat()

    try:
        if doc_id:
            res = await es.index(index=index, id=doc_id, document=body, refresh=True)
        else:
            res = await es.index(index=index, document=body, refresh=True)

        return DocumentCreateResponse(
            id=res["_id"],
            index=res["_index"],
            result=res.get("result", "created"),
            message="문서가 성공적으로 색인되었습니다.",
        )
    except Exception as e:
        logger.error("문서 추가 실패: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 추가 중 오류가 발생했습니다: {str(e)}",
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
    index: str = Query(settings.DEFAULT_INDEX, description="조회할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    키워드 검색, 태그/작성자 필터 및 페이지네이션을 지원하는 문서 목록 조회 API입니다.
    """
    from_offset = (page - 1) * size

    must_clauses = []
    filter_clauses = []

    if q:
        must_clauses.append(
            {
                "multi_match": {
                    "query": q,
                    "fields": ["title^2", "content"],
                    "fuzziness": "AUTO",
                }
            }
        )
    else:
        must_clauses.append({"match_all": {}})

    if tag:
        filter_clauses.append({"term": {"tags": tag}})

    if author:
        filter_clauses.append({"term": {"author": author}})

    query_body = {
        "bool": {
            "must": must_clauses,
            "filter": filter_clauses,
        }
    }

    try:
        exists = await es.indices.exists(index=index)
        if not exists:
            return SearchResponse(total=0, hits=[])

        search_kwargs = {
            "index": index,
            "query": query_body,
            "from_": from_offset,
            "size": size,
            "sort": [{"created_at": {"order": "desc", "unmapped_type": "date"}}],
        }
        if q:
            search_kwargs["highlight"] = {
                "fields": {
                    "title": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                    "content": {"pre_tags": ["<mark>"], "post_tags": ["</mark>"]},
                }
            }

        res = await es.search(**search_kwargs)

        total = res["hits"]["total"]["value"]
        hits = [
            DocumentResponse(
                id=hit["_id"],
                index=hit["_index"],
                source=hit["_source"],
                highlight=hit.get("highlight"),
            )
            for hit in res["hits"]["hits"]
        ]

        return SearchResponse(total=total, hits=hits)
    except Exception as e:
        logger.error("문서 검색 실패: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 검색 중 오류가 발생했습니다: {str(e)}",
        )


@app.get(
    "/documents/aggregations",
    response_model=AggregationResponse,
    tags=["Documents"],
    summary="태그 및 작성자 통계 집계",
)
async def get_document_aggregations(
    index: str = Query(settings.DEFAULT_INDEX, description="대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    Elasticsearch의 terms 집계 기능을 사용하여
    인기 태그 Top 10과 작성자별 문서 수를 실시간 집계합니다.
    """
    exists = await es.indices.exists(index=index)
    if not exists:
        return AggregationResponse(total_documents=0, top_tags=[], top_authors=[])

    try:
        res = await es.search(
            index=index,
            size=0,
            aggregations={
                "top_tags": {"terms": {"field": "tags", "size": 10}},
                "top_authors": {"terms": {"field": "author", "size": 10}},
            },
        )
        total_docs = res["hits"]["total"]["value"]
        tag_buckets = res.get("aggregations", {}).get("top_tags", {}).get("buckets", [])
        author_buckets = res.get("aggregations", {}).get("top_authors", {}).get("buckets", [])

        return AggregationResponse(
            total_documents=total_docs,
            top_tags=[AggregationItem(key=b["key"], doc_count=b["doc_count"]) for b in tag_buckets],
            top_authors=[AggregationItem(key=b["key"], doc_count=b["doc_count"]) for b in author_buckets],
        )
    except Exception as e:
        logger.error("집계 실패: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"집계 조회 중 오류 발생: {str(e)}",
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
    index: str = Query(settings.DEFAULT_INDEX, description="대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    Elasticsearch에 비동기로 동시에 5번의 조회를 요청(asyncio.gather)하고,
    그 결과를 중복 제거 및 점수 순으로 병합하여 반환하는 API입니다.
    """
    exists = await es.indices.exists(index=index)
    if not exists:
        return ConcurrentSearchResponse(
            total_queries=5,
            total_hits_before_dedup=0,
            total_unique_hits=0,
            elapsed_time_ms=0.0,
            query_stats=[],
            hits=[],
        )

    query_specs: List[Dict[str, Any]] = []

    if mode == "offset":
        if q:
            base_query = {
                "multi_match": {
                    "query": q,
                    "fields": ["title^2", "content"],
                    "fuzziness": "AUTO",
                }
            }
        else:
            base_query = {"match_all": {}}

        for i in range(5):
            query_specs.append(
                {
                    "strategy": f"offset_chunk_{i+1}",
                    "from_offset": i * size_per_query,
                    "size": size_per_query,
                    "query": base_query,
                    "sort": [{"created_at": {"order": "desc", "unmapped_type": "date"}}],
                }
            )
    else:  # mode == "strategy"
        search_term = q or "*"
        strategies = [
            ("exact_phrase", {"match_phrase": {"title": search_term}} if q else {"match_all": {}}),
            ("title_boosted", {"match": {"title": {"query": search_term, "boost": 3.0}}} if q else {"match_all": {}}),
            ("content_match", {"match": {"content": search_term}} if q else {"match_all": {}}),
            ("fuzzy_tolerant", {"multi_match": {"query": search_term, "fields": ["title", "content"], "fuzziness": "AUTO"}} if q else {"match_all": {}}),
            ("tag_or_wildcard", {"wildcard": {"tags": f"*{search_term}*"}} if q else {"match_all": {}}),
        ]
        for name, query_body in strategies:
            query_specs.append(
                {
                    "strategy": name,
                    "from_offset": 0,
                    "size": size_per_query,
                    "query": query_body,
                    "sort": ["_score"],
                }
            )

    async def execute_single_search(query_idx: int, spec: Dict[str, Any]):
        start_t = time.perf_counter()
        try:
            res = await es.search(
                index=index,
                query=spec["query"],
                from_=spec["from_offset"],
                size=spec["size"],
                sort=spec.get("sort"),
            )
            took_ms = round((time.perf_counter() - start_t) * 1000, 2)
            hits = res.get("hits", {}).get("hits", [])
            return {
                "query_index": query_idx,
                "strategy": spec["strategy"],
                "offset": spec["from_offset"],
                "took_ms": took_ms,
                "hits": hits,
            }
        except Exception as err:
            took_ms = round((time.perf_counter() - start_t) * 1000, 2)
            logger.warning("개별 쿼리(%d) 실패: %s", query_idx, err)
            return {
                "query_index": query_idx,
                "strategy": spec["strategy"],
                "offset": spec["from_offset"],
                "took_ms": took_ms,
                "hits": [],
            }

    total_start_t = time.perf_counter()
    tasks = [
        execute_single_search(idx + 1, spec)
        for idx, spec in enumerate(query_specs)
    ]
    raw_results = await asyncio.gather(*tasks)
    total_elapsed_ms = round((time.perf_counter() - total_start_t) * 1000, 2)

    merged_hits: Dict[str, ConcurrentSearchHit] = {}
    query_stats: List[QueryStat] = []
    total_hits_before_dedup = 0

    for res in raw_results:
        q_idx = res["query_index"]
        hits = res["hits"]
        total_hits_before_dedup += len(hits)

        query_stats.append(
            QueryStat(
                query_index=q_idx,
                offset=res["offset"],
                strategy=res["strategy"],
                hits_count=len(hits),
                took_ms=res["took_ms"],
            )
        )

        for hit in hits:
            doc_id = hit["_id"]
            score = hit.get("_score")
            source = hit.get("_source", {})

            if doc_id in merged_hits:
                merged_hits[doc_id].found_in_queries.append(q_idx)
                if score is not None:
                    existing_score = merged_hits[doc_id].score or 0.0
                    merged_hits[doc_id].score = max(existing_score, score)
            else:
                merged_hits[doc_id] = ConcurrentSearchHit(
                    id=doc_id,
                    index=hit.get("_index", index),
                    score=score,
                    source=source,
                    found_in_queries=[q_idx],
                )

    final_hits = list(merged_hits.values())
    final_hits.sort(
        key=lambda x: (
            x.score if x.score is not None else 0.0,
            x.source.get("created_at", ""),
        ),
        reverse=True,
    )

    return ConcurrentSearchResponse(
        total_queries=5,
        total_hits_before_dedup=total_hits_before_dedup,
        total_unique_hits=len(final_hits),
        elapsed_time_ms=total_elapsed_ms,
        query_stats=query_stats,
        hits=final_hits,
    )


@app.post(
    "/documents/concurrent-search",
    response_model=ConcurrentSearchResponse,
    tags=["Documents"],
    summary="사용자 지정 다중 쿼리 동시(병렬) 검색 및 결과 병합",
)
async def custom_concurrent_search_documents(
    payload: CustomConcurrentSearchRequest,
    index: str = Query(settings.DEFAULT_INDEX, description="대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    사용자가 직접 지정한 N개(예: 5개)의 서로 다른 검색 조건을 동시에 병렬로 조회하고
    결과를 중복 제거 및 점수 순으로 병합하여 반환합니다.
    """
    exists = await es.indices.exists(index=index)
    if not exists:
        return ConcurrentSearchResponse(
            total_queries=len(payload.queries),
            total_hits_before_dedup=0,
            total_unique_hits=0,
            elapsed_time_ms=0.0,
            query_stats=[],
            hits=[],
        )

    async def execute_query(idx: int, item: CustomSearchQuery):
        must = []
        filter_ = []

        if item.q:
            must.append(
                {
                    "multi_match": {
                        "query": item.q,
                        "fields": ["title^2", "content"],
                        "fuzziness": "AUTO",
                    }
                }
            )
        else:
            must.append({"match_all": {}})

        if item.tag:
            filter_.append({"term": {"tags": item.tag}})
        if item.author:
            filter_.append({"term": {"author": item.author}})

        query_body = {"bool": {"must": must, "filter": filter_}}

        start_t = time.perf_counter()
        try:
            res = await es.search(
                index=index,
                query=query_body,
                size=item.size,
                sort=["_score", {"created_at": {"order": "desc", "unmapped_type": "date"}}],
            )
            took_ms = round((time.perf_counter() - start_t) * 1000, 2)
            hits = res.get("hits", {}).get("hits", [])
            return {
                "query_index": idx,
                "strategy": item.label or f"custom_query_{idx}",
                "offset": 0,
                "took_ms": took_ms,
                "hits": hits,
            }
        except Exception as err:
            took_ms = round((time.perf_counter() - start_t) * 1000, 2)
            logger.warning("커스텀 쿼리(%d) 실패: %s", idx, err)
            return {
                "query_index": idx,
                "strategy": item.label or f"custom_query_{idx}",
                "offset": 0,
                "took_ms": took_ms,
                "hits": [],
            }

    total_start_t = time.perf_counter()
    tasks = [execute_query(idx + 1, q_item) for idx, q_item in enumerate(payload.queries)]
    raw_results = await asyncio.gather(*tasks)
    total_elapsed_ms = round((time.perf_counter() - total_start_t) * 1000, 2)

    merged_hits: Dict[str, ConcurrentSearchHit] = {}
    query_stats: List[QueryStat] = []
    total_hits_before_dedup = 0

    for res in raw_results:
        q_idx = res["query_index"]
        hits = res["hits"]
        total_hits_before_dedup += len(hits)

        query_stats.append(
            QueryStat(
                query_index=q_idx,
                offset=res["offset"],
                strategy=res["strategy"],
                hits_count=len(hits),
                took_ms=res["took_ms"],
            )
        )

        for hit in hits:
            doc_id = hit["_id"]
            score = hit.get("_score")
            source = hit.get("_source", {})

            if doc_id in merged_hits:
                merged_hits[doc_id].found_in_queries.append(q_idx)
                if score is not None:
                    existing_score = merged_hits[doc_id].score or 0.0
                    merged_hits[doc_id].score = max(existing_score, score)
            else:
                merged_hits[doc_id] = ConcurrentSearchHit(
                    id=doc_id,
                    index=hit.get("_index", index),
                    score=score,
                    source=source,
                    found_in_queries=[q_idx],
                )

    final_hits = list(merged_hits.values())
    final_hits.sort(
        key=lambda x: (
            x.score if x.score is not None else 0.0,
            x.source.get("created_at", ""),
        ),
        reverse=True,
    )

    return ConcurrentSearchResponse(
        total_queries=len(payload.queries),
        total_hits_before_dedup=total_hits_before_dedup,
        total_unique_hits=len(final_hits),
        elapsed_time_ms=total_elapsed_ms,
        query_stats=query_stats,
        hits=final_hits,
    )


@app.get(
    "/documents/{doc_id}",
    response_model=DocumentResponse,
    tags=["Documents"],
    summary="문서 단건 조회 (ID)",
)
async def get_document_by_id(
    doc_id: str,
    index: str = Query(settings.DEFAULT_INDEX, description="조회할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """
    문서 ID를 통해 특정 문서를 단건 조회합니다.
    """
    try:
        res = await es.get(index=index, id=doc_id)
        return DocumentResponse(
            id=res["_id"],
            index=res["_index"],
            source=res["_source"],
        )
    except NotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ID '{doc_id}'에 해당하는 문서를 인덱스 '{index}'에서 찾을 수 없습니다.",
        )
    except Exception as e:
        logger.error("문서 조회 실패: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 조회 중 오류가 발생했습니다: {str(e)}",
        )


@app.put(
    "/documents/{doc_id}",
    tags=["Documents"],
    summary="문서 수정 (부분 업데이트)",
)
async def update_document(
    doc_id: str,
    doc_update: DocumentUpdate,
    index: str = Query(settings.DEFAULT_INDEX, description="수정할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """문서 ID에 해당하는 문서의 필드를 부분 업데이트합니다."""
    update_data = {k: v for k, v in doc_update.model_dump().items() if v is not None}
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="수정할 필드가 최소 1개 이상 필요합니다.",
        )

    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()

    try:
        res = await es.update(
            index=index,
            id=doc_id,
            doc=update_data,
            refresh=True,
        )
        return {
            "id": res["_id"],
            "index": res["_index"],
            "result": res.get("result", "updated"),
            "message": "문서가 성공적으로 수정되었습니다.",
        }
    except NotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ID '{doc_id}' 문서를 찾을 수 없습니다.",
        )
    except Exception as e:
        logger.error("문서 수정 실패: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 수정 중 오류 발생: {str(e)}",
        )


@app.delete(
    "/documents/{doc_id}",
    tags=["Documents"],
    summary="문서 삭제",
)
async def delete_document(
    doc_id: str,
    index: str = Query(settings.DEFAULT_INDEX, description="삭제할 대상 인덱스명"),
    es: AsyncElasticsearch = Depends(get_es),
):
    """문서 ID로 문서를 삭제합니다."""
    try:
        res = await es.delete(index=index, id=doc_id, refresh=True)
        return {
            "id": res["_id"],
            "index": res["_index"],
            "result": res.get("result", "deleted"),
            "message": "문서가 삭제되었습니다.",
        }
    except NotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"ID '{doc_id}' 문서를 찾을 수 없습니다.",
        )
    except Exception as e:
        logger.error("문서 삭제 실패: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"문서 삭제 중 오류 발생: {str(e)}",
        )
