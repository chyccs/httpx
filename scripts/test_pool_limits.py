import asyncio
import time
import httpx
from elasticsearch import AsyncElasticsearch
from app.es import SharedPoolHttpxNode


async def test_pool_limit(max_conn: int, num_requests: int = 10):
    print(f"\n=======================================================")
    print(f" 🧪 [테스트] max_connections = {max_conn} (동시 요청: {num_requests}개)")
    print(f"=======================================================")

    # 1. 제한된 커넥션 풀 생성
    transport = httpx.AsyncHTTPTransport(
        limits=httpx.Limits(max_connections=max_conn, max_keepalive_connections=max_conn),
        retries=1,
    )

    # 2. Elasticsearch 노드에 해당 풀 주입
    class TestNode(SharedPoolHttpxNode):
        def __init__(self, config):
            super().__init__(config)
            self.client = httpx.AsyncClient(
                base_url=f"{config.scheme}://{config.host}:{config.port}",
                transport=transport,
                timeout=5.0,
            )

    es = AsyncElasticsearch("http://localhost:9200", node_class=TestNode)

    # 3. 요청 시작 전 풀 상태
    initial_conn_count = len(transport._pool._connections)
    print(f"  • 요청 시작 전 풀 내 활성 소켓 수: {initial_conn_count}")

    # 4. num_requests 개의 Elasticsearch 검색을 동시에 병렬 요청
    observed_conn_counts = []
    start_time = time.perf_counter()

    async def single_search(idx: int):
        # 쿼리 실행
        res = await es.search(index="documents", size=1)
        # 쿼리 직후 풀의 소켓 수 기록
        current_active = len(transport._pool._connections)
        observed_conn_counts.append(current_active)
        return res

    tasks = [single_search(i) for i in range(num_requests)]
    results = await asyncio.gather(*tasks)
    elapsed_time = (time.perf_counter() - start_time) * 1000

    peak_connections = max(observed_conn_counts) if observed_conn_counts else 0
    final_conn_count = len(transport._pool._connections)

    print(f"  • {num_requests}개 동시 쿼리 완료 시간: {elapsed_time:.2f} ms")
    print(f"  • 측정된 최대 활성 소켓 수 (Peak Connections): {peak_connections}")
    print(f"  • 요청 완료 후 풀에 유지된 Keep-Alive 소켓 수: {final_conn_count}")

    # 검증 판정
    if peak_connections <= max_conn:
        print(f"  ✔ [성공] 동시 요청이 {num_requests}개 몰려도 소켓 수가 {max_conn}개를 초과하지 않았습니다!")
    else:
        print(f"  ❌ [실패] 설정된 {max_conn}개를 초과하여 {peak_connections}개의 소켓이 열렸습니다.")

    await es.close()
    await transport.aclose()


async def main():
    print("🚀 [커넥션 풀 제한 실시간 스트레스 검증]")
    # Case 1: 커넥션 풀을 극단적으로 1개로 제한
    await test_pool_limit(max_conn=1, num_requests=10)

    # Case 2: 커넥션 풀을 2개로 제한
    await test_pool_limit(max_conn=2, num_requests=10)

    # Case 3: 커넥션 풀을 5개로 설정
    await test_pool_limit(max_conn=5, num_requests=10)


if __name__ == "__main__":
    asyncio.run(main())
