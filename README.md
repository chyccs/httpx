# Elasticsearch + FastAPI 서비스 예제

FastAPI와 비동기 Elasticsearch 클라이언트(`elasticsearch[async]`)를 이용한 문서 추가 및 검색 서비스입니다.  
`asyncio.gather`를 활용해 **동시에 5번 조회 후 결과를 중복 제거 및 스코어 순으로 병합하는 병렬 검색 API**와 **웹 시각화 대시보드**를 기본 제공합니다.

---

## 📁 프로젝트 구조

```text
es-example/
├── app/
│   ├── __init__.py
│   ├── config.py         # 환경 설정 (ES URL, 기본 인덱스, 풀 크기)
│   ├── http_pool.py      # 전역 httpx 커넥션 풀 및 클라이언트 관리 (공통 풀)
│   ├── es.py             # AsyncElasticsearch 연결 및 인덱스 관리
│   ├── models.py         # Pydantic 요청/응답 모델
│   ├── search.py         # 쿼리 빌더 및 동시(병렬) 검색 실행·중복 제거·병합 로직
│   ├── metrics.py        # Prometheus 메트릭 및 인/아웃바운드 TCP 소켓 수집기
│   ├── templates/
│   │   └── dashboard.html # 웹 시각화 대시보드
│   └── main.py           # FastAPI 앱 및 API 엔드포인트
├── tests/
│   ├── conftest.py       # 공용 fixture (Mock ES / TestClient)
│   ├── test_api.py       # API 엔드포인트 테스트
│   └── test_search.py    # 검색 로직·에러 처리 단위 테스트
├── .vscode/
│   └── settings.json     # VS Code / Antigravity 가상환경 자동 인식 설정
├── docker-compose.yml     # Elasticsearch 8.x 컨테이너 설정
├── Dockerfile             # FastAPI 애플리케이션 컨테이너 빌드 파일
├── Makefile               # 간편 실행 단축 명령어 (start, test, docker-up 등)
├── run.sh                 # 원클릭 실행 스크립트 (ES 체크 + FastAPI 실행)
├── requirements.txt       # 의존성 패키지 목록
├── .env                   # 환경변수 파일
└── README.md
```

---

## 🚀 빠른 시작 (3초 실행)

터미널에서 아래 한 줄로 즉시 실행할 수 있습니다:

```bash
make start
# 또는 ./run.sh
```

- **웹 시각화 대시보드**: [http://localhost:8000](http://localhost:8000)
- **Swagger UI (대화형 API 문서)**: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 🧪 단위 테스트 실행

```bash
make test
# 또는 ./.venv/bin/python -m pytest -v tests/
```

- **Swagger UI (대화형 API 문서)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc 문서**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## 📌 API 명세 및 cURL 테스트 예시

### 1) 서비스 및 Elasticsearch 헬스체크
```bash
curl -X GET "http://localhost:8000/health"
```

### 2) 📈 Prometheus 메트릭 수집 (/metrics)
FastAPI HTTP 요청 통계 및 **실시간 커넥션 풀 상태**, **OS 레벨 상태별 TCP 커넥션(ESTABLISHED, TIME_WAIT, CLOSE_WAIT 등)**을 Prometheus 표준 규격으로 노출합니다.
```bash
# 커넥션 풀 상태 메트릭
curl -X GET "http://localhost:8000/metrics" | grep http_pool

# 인바운드(클라이언트 접속) 및 아웃바운드(외부 서버 호출) 구분 TCP 소켓 메트릭
curl -X GET "http://localhost:8000/metrics" | grep process_tcp
```

### 3) 🌐 인/아웃바운드 구분 TCP 커넥션 상세 조회 (/tcp-connections)
현재 서버 프로세스의 소켓을 `direction: outbound`(외부 서버 호출)와 `direction: inbound`(클라이언트 접속 및 LISTEN)로 분류하여 상태별 요약과 상세 목록을 JSON으로 반환합니다.
```bash
curl -X GET "http://localhost:8000/tcp-connections"
```

### 4) 🔗 httpx 공유 커넥션 풀(동일 소켓 세션) 검증 테스트
FastAPI의 `httpx.AsyncClient`와 `AsyncElasticsearch`가 **100% 동일한 커넥션 풀(메모리 인스턴스)**을 공유하고 있는지 검증합니다.
```bash
curl -X GET "http://localhost:8000/test-shared-pool"
```

---

### 3) 문서 추가 (색인)
- ID를 지정하지 않으면 Elasticsearch가 고유 ID를 자동 발급합니다.
- `?doc_id=my-doc-1` 파라미터로 특정 ID를 부여할 수도 있습니다.

```bash
curl -X POST "http://localhost:8000/documents" \
     -H "Content-Type: application/json" \
     -d '{
       "title": "FastAPI와 Elasticsearch 연동 가이드",
       "content": "FastAPI의 비동기 핸들러와 AsyncElasticsearch를 결합하면 대용량 검색 트래픽도 안정적으로 처리할 수 있습니다.",
       "author": "홍길동",
       "tags": ["fastapi", "elasticsearch", "python"],
       "extra": {"category": "backend", "views": 120}
     }'
```

**응답 예시:**
```json
{
  "id": "U3J4xZIB52J8vL_4dE8x",
  "index": "documents",
  "result": "created",
  "message": "문서가 성공적으로 색인되었습니다."
}
```

---

### 3) 문서 단건 조회 (ID 기준)
```bash
curl -X GET "http://localhost:8000/documents/U3J4xZIB52J8vL_4dE8x"
```

---

### 4) 문서 수정 (부분 업데이트)
```bash
curl -X PUT "http://localhost:8000/documents/U3J4xZIB52J8vL_4dE8x" \
     -H "Content-Type: application/json" \
     -d '{
       "title": "수정된 제목: FastAPI & Elasticsearch 연동",
       "tags": ["fastapi", "elasticsearch", "python", "search"]
     }'
```

---

### 5) 문서 검색 및 목록 조회
다양한 쿼리 파라미터를 조합하여 검색할 수 있습니다 (검색 시 일치 키워드 자동 `<mark>` 하이라이팅 지원):
- `q`: 제목(가중치 적용) 및 내용 대상 전문(full-text) 검색
- `tag`: 태그 필터
- `author`: 작성자 필터
- `page`: 페이지 번호 (기본값: 1)
- `size`: 조회 개수 (기본값: 10)

```bash
# 전체 목록 조회 (최신순)
curl -X GET "http://localhost:8000/documents"

# 키워드 검색 (자동 하이라이팅 포함)
curl -X GET "http://localhost:8000/documents?q=FastAPI"

# 태그 필터링 검색
curl -X GET "http://localhost:8000/documents?tag=python"
```

---

### 6) 📊 태그 및 작성자 통계 집계 (Terms Aggregation)
Elasticsearch의 집계 엔진을 활용해 전체 문서의 태그 분포 및 작성자별 문서 수를 즉시 산출합니다.
```bash
curl -X GET "http://localhost:8000/documents/aggregations"
```

---

### 7) ⚡ 동시에 5번 조회 후 결과 합치기 (Concurrent Search)

Python의 비동기 `asyncio.gather`를 활용해 Elasticsearch에 **5번의 검색 요청을 동시에(병렬) 전송**하고, 결과를 **중복 제거 및 스코어 순으로 병합**합니다.

- **옵션 1: `mode=offset` (기본값 - 오프셋 분할 병렬 수집)**
  - 오프셋을 `[0, size, size*2, size*3, size*4]`로 나누어 5개 청크를 동시에 인출해 대량 문서를 빠르게 합칩니다.
- **옵션 2: `mode=strategy` (5가지 검색 전략 동시 수행)**
  - 동일 검색어에 대해 1) 정확 구문 일치(`phrase`), 2) 제목 가중치 검색, 3) 내용 검색, 4) 오타 허용 퍼지(`fuzzy`), 5) 태그/와일드카드 검색 등 5개 쿼리를 동시에 실행해 가장 적합도 높은 결과를 취합합니다.

```bash
# 기본 오프셋 분할 동시 5회 조회 (각 쿼리당 5건씩 총 25건 병렬 취합)
curl -X GET "http://localhost:8000/documents/concurrent-search?q=FastAPI&mode=offset&size_per_query=5"

# 5가지 검색 전략 동시 실행 및 결과 병합
curl -X GET "http://localhost:8000/documents/concurrent-search?q=FastAPI&mode=strategy"

# 사용자 정의 5개 쿼리 직접 전송 (POST)
curl -X POST "http://localhost:8000/documents/concurrent-search" \
     -H "Content-Type: application/json" \
     -d '{
       "queries": [
         {"q": "FastAPI", "label": "키워드1: FastAPI"},
         {"q": "Elasticsearch", "label": "키워드2: Elasticsearch"},
         {"tag": "python", "label": "태그: python"},
         {"author": "홍길동", "label": "작성자: 홍길동"},
         {"q": "비동기", "label": "키워드3: 비동기"}
       ]
     }'
```

**응답 예시:**
```json
{
  "total_queries": 5,
  "total_hits_before_dedup": 25,
  "total_unique_hits": 20,
  "elapsed_time_ms": 32.14,
  "query_stats": [
    {"query_index": 1, "offset": 0, "strategy": "offset_chunk_1", "hits_count": 5, "took_ms": 12.3},
    {"query_index": 2, "offset": 5, "strategy": "offset_chunk_2", "hits_count": 5, "took_ms": 11.8},
    ...
  ],
  "hits": [
    {
      "id": "doc-1",
      "index": "documents",
      "score": 4.52,
      "found_in_queries": [1, 2],
      "source": { "title": "...", "content": "..." }
    }
  ]
}
```

---

### 5) 문서 삭제
```bash
curl -X DELETE "http://localhost:8000/documents/U3J4xZIB52J8vL_4dE8x"
```

