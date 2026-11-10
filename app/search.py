"""Elasticsearch 쿼리 생성 및 동시(병렬) 검색 실행/병합 로직"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from elasticsearch import AsyncElasticsearch

from app.models import ConcurrentSearchHit, ConcurrentSearchResponse, QueryStat

logger = logging.getLogger("es_search")

# 동시 검색 시 기본으로 실행하는 쿼리 수
CONCURRENT_QUERY_COUNT = 5

# 최신 문서 우선 정렬 (created_at 매핑이 없는 인덱스에서도 오류가 나지 않도록 unmapped_type 지정)
SORT_BY_CREATED_DESC: Dict[str, Any] = {"created_at": {"order": "desc", "unmapped_type": "date"}}

MATCH_ALL: Dict[str, Any] = {"match_all": {}}


def elapsed_ms(start: float) -> float:
    """perf_counter 기준 시작 시각으로부터 경과 시간(ms)"""
    return round((time.perf_counter() - start) * 1000, 2)


def keyword_query(q: Optional[str]) -> Dict[str, Any]:
    """제목(가중치 2) + 본문 대상 퍼지 multi_match 쿼리. 키워드가 없으면 match_all"""
    if not q:
        return MATCH_ALL
    return {
        "multi_match": {
            "query": q,
            "fields": ["title^2", "content"],
            "fuzziness": "AUTO",
        }
    }


def filtered_query(
    q: Optional[str] = None,
    tag: Optional[str] = None,
    author: Optional[str] = None,
) -> Dict[str, Any]:
    """키워드 검색 + 태그/작성자 term 필터를 결합한 bool 쿼리"""
    filters: List[Dict[str, Any]] = []
    if tag:
        filters.append({"term": {"tags": tag}})
    if author:
        filters.append({"term": {"author": author}})
    return {"bool": {"must": [keyword_query(q)], "filter": filters}}


@dataclass
class SearchSpec:
    """동시 검색에서 실행할 개별 쿼리 명세"""
    strategy: str
    query: Dict[str, Any]
    size: int
    offset: int = 0
    sort: Optional[List[Any]] = None


@dataclass
class SearchResult:
    query_index: int
    spec: SearchSpec
    took_ms: float
    hits: List[Dict[str, Any]] = field(default_factory=list)


def offset_chunk_specs(q: Optional[str], size_per_query: int) -> List[SearchSpec]:
    """동일 쿼리를 오프셋 구간별로 나누어 병렬 수집하는 명세 목록"""
    base_query = keyword_query(q)
    return [
        SearchSpec(
            strategy=f"offset_chunk_{i + 1}",
            query=base_query,
            size=size_per_query,
            offset=i * size_per_query,
            sort=[SORT_BY_CREATED_DESC],
        )
        for i in range(CONCURRENT_QUERY_COUNT)
    ]


def multi_strategy_specs(q: Optional[str], size_per_query: int) -> List[SearchSpec]:
    """서로 다른 5가지 검색 전략을 동시에 수행하는 명세 목록"""
    strategies = [
        ("exact_phrase", lambda t: {"match_phrase": {"title": t}}),
        ("title_boosted", lambda t: {"match": {"title": {"query": t, "boost": 3.0}}}),
        ("content_match", lambda t: {"match": {"content": t}}),
        ("fuzzy_tolerant", lambda t: {"multi_match": {"query": t, "fields": ["title", "content"], "fuzziness": "AUTO"}}),
        ("tag_or_wildcard", lambda t: {"wildcard": {"tags": f"*{t}*"}}),
    ]
    return [
        SearchSpec(
            strategy=name,
            query=build(q) if q else MATCH_ALL,
            size=size_per_query,
            sort=["_score"],
        )
        for name, build in strategies
    ]


async def _execute(
    es: AsyncElasticsearch, index: str, query_index: int, spec: SearchSpec
) -> SearchResult:
    """단일 검색 실행. 개별 쿼리 실패는 전체를 실패시키지 않고 빈 결과로 처리한다."""
    start = time.perf_counter()
    try:
        res = await es.search(
            index=index,
            query=spec.query,
            from_=spec.offset,
            size=spec.size,
            sort=spec.sort,
        )
        hits = res.get("hits", {}).get("hits", [])
    except Exception as err:
        logger.warning("개별 쿼리(%d, %s) 실패: %s", query_index, spec.strategy, err)
        hits = []
    return SearchResult(query_index=query_index, spec=spec, took_ms=elapsed_ms(start), hits=hits)


def _merge_hits(results: List[SearchResult], index: str) -> List[ConcurrentSearchHit]:
    """문서 ID 기준 중복 제거 후 (최고 점수, 최신순)으로 정렬"""
    merged: Dict[str, ConcurrentSearchHit] = {}

    for result in results:
        for hit in result.hits:
            doc_id = hit["_id"]
            score = hit.get("_score")
            existing = merged.get(doc_id)

            if existing is None:
                merged[doc_id] = ConcurrentSearchHit(
                    id=doc_id,
                    index=hit.get("_index", index),
                    score=score,
                    source=hit.get("_source", {}),
                    found_in_queries=[result.query_index],
                )
                continue

            existing.found_in_queries.append(result.query_index)
            if score is not None:
                existing.score = max(existing.score or 0.0, score)

    return sorted(
        merged.values(),
        key=lambda h: (h.score or 0.0, h.source.get("created_at", "")),
        reverse=True,
    )


def empty_concurrent_response(total_queries: int) -> ConcurrentSearchResponse:
    return ConcurrentSearchResponse(
        total_queries=total_queries,
        total_hits_before_dedup=0,
        total_unique_hits=0,
        elapsed_time_ms=0.0,
        query_stats=[],
        hits=[],
    )


async def run_concurrent_search(
    es: AsyncElasticsearch, index: str, specs: List[SearchSpec]
) -> ConcurrentSearchResponse:
    """명세 목록을 asyncio.gather로 동시에 실행하고 결과를 병합한다."""
    start = time.perf_counter()
    results = await asyncio.gather(
        *(_execute(es, index, idx, spec) for idx, spec in enumerate(specs, start=1))
    )
    total_elapsed = elapsed_ms(start)

    query_stats = [
        QueryStat(
            query_index=r.query_index,
            offset=r.spec.offset,
            strategy=r.spec.strategy,
            hits_count=len(r.hits),
            took_ms=r.took_ms,
        )
        for r in results
    ]
    final_hits = _merge_hits(results, index)

    return ConcurrentSearchResponse(
        total_queries=len(specs),
        total_hits_before_dedup=sum(len(r.hits) for r in results),
        total_unique_hits=len(final_hits),
        elapsed_time_ms=total_elapsed,
        query_stats=query_stats,
        hits=final_hits,
    )

