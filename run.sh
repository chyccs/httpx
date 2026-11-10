#!/bin/bash
set -e

# 색상 정의
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}=======================================${NC}"
echo -e "${BLUE}  FastAPI + Elasticsearch 서비스 시작  ${NC}"
echo -e "${BLUE}=======================================${NC}"

# 1. Elasticsearch 컨테이너 확인 및 실행
echo -e "\n${YELLOW}[1/3] Elasticsearch 컨테이너 구동 확인...${NC}"
docker compose up -d elasticsearch

# 2. Elasticsearch 헬스체크 대기
echo -e "${YELLOW}[2/3] Elasticsearch 준비 대기 중 (최대 30초)...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:9200 > /dev/null; then
        echo -e "${GREEN}✔ Elasticsearch가 준비되었습니다!${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

# 3. FastAPI 서버 구동
echo -e "\n${YELLOW}[3/3] FastAPI 서버 실행 (http://localhost:8000)...${NC}"
echo -e "${GREEN}웹 대시보드 : http://localhost:8000${NC}"
echo -e "${GREEN}Swagger 문서 : http://localhost:8000/docs${NC}"
echo -e "${BLUE}종료하려면 Ctrl+C를 누르세요.${NC}\n"

./.venv/bin/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

