"""리팩토링으로 분리된 검색 로직(app/search.py) 및 공통 에러 처리 테스트"""
from elasticsearch import NotFoundError

from app.search import (
    MATCH_ALL,
    SearchResult,
    SearchSpec,
    _merge_hits,
    filtered_query,
    keyword_query,
    multi_strategy_specs,
    offset_chunk_specs,
)


def _hit(doc_id, score=None, created_at=""):
    return {"_id": doc_id, "_index": "documents", "_score": score, "_source": {"created_at": created_at}}


def _result(idx, hits):
    return SearchResult(query_index=idx, spec=SearchSpec(strategy="s", query=MATCH_ALL, size=5), took_ms=1.0, hits=hits)


def test_keyword_query():
    assert keyword_query(None) == MATCH_ALL
    assert keyword_query("abc")["multi_match"]["query"] == "abc"


def test_filtered_query_builds_filters():
    q = filtered_query("x", tag="t", author="a")
    assert q["bool"]["filter"] == [{"term": {"tags": "t"}}, {"term": {"author": "a"}}]
    assert filtered_query()["bool"] == {"must": [MATCH_ALL], "filter": []}


def test_offset_chunk_specs():
    specs = offset_chunk_specs("x", 3)
    assert [s.offset for s in specs] == [0, 3, 6, 9, 12]
    assert all(s.size == 3 for s in specs)


def test_multi_strategy_specs_without_keyword_uses_match_all():
    specs = multi_strategy_specs(None, 5)
    assert len(specs) == 5
    assert all(s.query == MATCH_ALL for s in specs)


def test_merge_hits_dedup_keeps_max_score_and_sorts():
    merged = _merge_hits(
        [
            _result(1, [_hit("a", 1.0), _hit("b", 5.0)]),
            _result(2, [_hit("a", 3.0)]),
        ],
        "documents",
    )
    assert [h.id for h in merged] == ["b", "a"]
    assert merged[1].score == 3.0
    assert merged[1].found_in_queries == [1, 2]


def test_merge_hits_ties_sorted_by_created_at_desc():
    merged = _merge_hits(
        [_result(1, [_hit("old", None, "2026-01-01"), _hit("new", None, "2026-06-01")])],
        "documents",
    )
    assert [h.id for h in merged] == ["new", "old"]


def test_dashboard_served(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "<!DOCTYPE html>" in response.text


def test_concurrent_search_index_missing(client, mock_es):
    mock_es.indices.exists.return_value = False
    data = client.get("/documents/concurrent-search?q=x").json()
    assert data["total_queries"] == 5
    assert data["hits"] == []
    mock_es.search.assert_not_called()


def test_partial_query_failure_does_not_fail_request(client, mock_es):
    calls = {"n": 0}

    def side_effect(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"hits": {"hits": [_hit(f"d{calls['n']}", 1.0)]}}

    mock_es.search.side_effect = side_effect
    data = client.get("/documents/concurrent-search?mode=strategy").json()
    assert data["total_hits_before_dedup"] == 4
    assert sorted(s["hits_count"] for s in data["query_stats"]) == [0, 1, 1, 1, 1]


def test_update_requires_fields(client):
    response = client.put("/documents/x", json={})
    assert response.status_code == 400


def test_update_and_delete_not_found(client, mock_es):
    err = NotFoundError(message="nf", meta=None, body={})
    mock_es.update.side_effect = err
    mock_es.delete.side_effect = err
    assert client.put("/documents/x", json={"title": "t"}).status_code == 404
    assert client.delete("/documents/x").status_code == 404


def test_es_error_returns_500(client, mock_es):
    mock_es.get.side_effect = RuntimeError("down")
    response = client.get("/documents/x")
    assert response.status_code == 500
    assert "문서 조회" in response.json()["detail"]
