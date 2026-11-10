.PHONY: help start test test-coverage docker-up docker-down clean

help:
	@echo "사용 가능한 명령어:"
	@echo "  make start        - Elasticsearch 컨테이너 확인 후 FastAPI 로컬 서버 실행"
	@echo "  make test         - pytest 단위 테스트 실행"
	@echo "  make test-pool    - 커넥션 풀 크기 제한(1개, 2개, 5개) 실시간 스트레스 검증 스크립트 실행"
	@echo "  make docker-up    - Docker Compose로 Elasticsearch + API 전체 컨테이너 구동"
	@echo "  make docker-down  - Docker Compose 컨테이너 종료"
	@echo "  make clean        - 파이썬 캐시 파일 정리"

start:
	./run.sh

test:
	./.venv/bin/python -m pytest -v tests/

test-pool:
	PYTHONPATH=. ./.venv/bin/python scripts/test_pool_limits.py

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

