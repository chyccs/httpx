from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class DocumentCreate(BaseModel):
    title: str = Field(..., description="문서 제목", json_schema_extra={"example": "FastAPI와 Elasticsearch 연동 가이드"})
    content: str = Field(..., description="문서 본문", json_schema_extra={"example": "FastAPI와 AsyncElasticsearch를 사용하여 검색 서비스를 구축합니다."})
    author: Optional[str] = Field(None, description="작성자", json_schema_extra={"example": "홍길동"})
    tags: List[str] = Field(default_factory=list, description="태그 목록", json_schema_extra={"example": ["fastapi", "elasticsearch", "python"]})
    extra: Dict[str, Any] = Field(default_factory=dict, description="추가 메타데이터")


class DocumentUpdate(BaseModel):
    title: Optional[str] = Field(None, description="수정할 제목")
    content: Optional[str] = Field(None, description="수정할 본문")
    author: Optional[str] = Field(None, description="수정할 작성자")
    tags: Optional[List[str]] = Field(None, description="수정할 태그 목록")
    extra: Optional[Dict[str, Any]] = Field(None, description="수정할 추가 메타데이터")


class DocumentResponse(BaseModel):
    id: str
    index: str
    source: Dict[str, Any]
    highlight: Optional[Dict[str, List[str]]] = Field(None, description="검색어 하이라이트 결과")


class DocumentCreateResponse(BaseModel):
    id: str
    index: str
    result: str
    message: str


class SearchResponse(BaseModel):
    total: int
    hits: List[DocumentResponse]



class ConcurrentSearchHit(BaseModel):
    id: str
    index: str
    score: Optional[float] = None
    source: Dict[str, Any]
    highlight: Optional[Dict[str, List[str]]] = Field(None, description="검색어 하이라이트 결과")
    found_in_queries: List[int] = Field(default_factory=list, description="해당 문서가 발견된 조회 차수 (1~5)")


class AggregationItem(BaseModel):
    key: str
    doc_count: int


class AggregationResponse(BaseModel):
    total_documents: int
    top_tags: List[AggregationItem]
    top_authors: List[AggregationItem]


class QueryStat(BaseModel):
    query_index: int
    offset: Optional[int] = None
    strategy: Optional[str] = None
    hits_count: int
    took_ms: float


class CustomSearchQuery(BaseModel):
    q: Optional[str] = Field(None, description="검색 키워드")
    tag: Optional[str] = Field(None, description="태그 필터")
    author: Optional[str] = Field(None, description="작성자 필터")
    size: int = Field(5, ge=1, le=50, description="조회 건수")
    label: Optional[str] = Field(None, description="쿼리 식별 레이블")


class CustomConcurrentSearchRequest(BaseModel):
    queries: List[CustomSearchQuery] = Field(
        ...,
        min_length=1,
        max_length=10,
        description="동시에 실행할 검색 조건 목록 (기본 5개 권장)",
        json_schema_extra={
            "example": [
                {"q": "FastAPI", "label": "FastAPI 검색"},
                {"q": "Elasticsearch", "label": "Elasticsearch 검색"},
                {"tag": "python", "label": "Python 태그"},
                {"author": "홍길동", "label": "홍길동 작성글"},
                {"q": "비동기", "label": "비동기 검색"},
            ]
        },
    )


class ConcurrentSearchResponse(BaseModel):
    total_queries: int = 5
    total_hits_before_dedup: int
    total_unique_hits: int
    elapsed_time_ms: float
    query_stats: List[QueryStat]
    hits: List[ConcurrentSearchHit]
