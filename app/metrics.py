from collections import Counter
import logging
import time
import psutil
from prometheus_client import Gauge
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi import FastAPI

from app.http_pool import get_pool_status
from app.config import settings

logger = logging.getLogger("metrics")

# 1. 커넥션 풀 내부 상태 추적 Gauge 메트릭
HTTP_POOL_ACTIVE_CONNECTIONS = Gauge(
    "http_pool_active_connections",
    "Current number of active TCP socket connections in the shared HTTP pool",
)

HTTP_POOL_MAX_CONNECTIONS = Gauge(
    "http_pool_max_connections",
    "Configured maximum number of connections allowed in the shared HTTP pool",
)

HTTP_POOL_KEEPALIVE_CONNECTIONS = Gauge(
    "http_pool_keepalive_connections",
    "Configured maximum number of idle keepalive connections in the pool",
)


# TCP 소켓 시스템 콜 캐싱 변수
_tcp_cache_time: float = 0.0
_tcp_cached_result = None


def _classify_tcp_connections(force_refresh: bool = False):
    """
    현재 프로세스가 열고 있는 모든 TCP 소켓을 '인바운드(Inbound)'와 '아웃바운드(Outbound)'로 분류합니다.
    - Inbound: 서버 리스닝 포트(예: 8000) 및 클라이언트가 해당 포트로 접속한 소켓
    - Outbound: 서버가 외부 서버(Elasticsearch 등)로 요청하기 위해 연결한 소켓
    
    * 성능 최적화: 고빈도 스크랩(/metrics) 시 OS 시스템 콜 부하를 방지하기 위해 TTL 캐싱 적용
    """
    global _tcp_cache_time, _tcp_cached_result
    now = time.time()

    # TTL 이내의 연속 호출이면 캐시된 결과 즉시 반환 (OS 시스템 콜 0회)
    if not force_refresh and _tcp_cached_result and (now - _tcp_cache_time < settings.TCP_CACHE_TTL_SECONDS):
        return _tcp_cached_result

    try:
        proc = psutil.Process()
        all_conns = proc.net_connections(kind="tcp")
        pid = proc.pid
    except Exception as e:
        logger.debug("net_connections 조회 실패: %s", e)
        return [], [], set(), None

    # 서버가 클라이언트 요청 수신을 위해 열어둔 LISTEN 포트 식별 (예: 8000)
    listen_ports = {c.laddr.port for c in all_conns if c.status == psutil.CONN_LISTEN}

    outbound_conns = []
    inbound_conns = []

    for c in all_conns:
        if c.status == psutil.CONN_LISTEN:
            inbound_conns.append(c)
        elif c.laddr and c.laddr.port in listen_ports:
            # 로컬 포트가 LISTEN 포트인 소켓 = 외부 클라이언트가 서버로 들어온 인바운드 소켓
            inbound_conns.append(c)
        elif c.raddr:
            # 원격지가 있고 로컬 포트가 LISTEN 포트가 아닌 소켓 = 외부 서버로 나간 아웃바운드 소켓
            outbound_conns.append(c)

    _tcp_cached_result = (outbound_conns, inbound_conns, listen_ports, pid)
    _tcp_cache_time = now
    return _tcp_cached_result


# 2. 프로세스의 인바운드 / 아웃바운드 구분 TCP 상태별 실시간 Prometheus 수집기
class DirectionalTCPConnectionCollector:
    """
    현재 FastAPI 프로세스의 TCP 소켓을 방향(direction: outbound, inbound) 및
    상태(state: ESTABLISHED, LISTEN, TIME_WAIT, CLOSE_WAIT 등)별로 수집하는 Collector
    """
    TRACKED_STATES = [
        "ESTABLISHED",
        "LISTEN",
        "TIME_WAIT",
        "CLOSE_WAIT",
        "SYN_SENT",
        "SYN_RECV",
        "FIN_WAIT1",
        "FIN_WAIT2",
        "LAST_ACK",
        "CLOSING",
    ]

    def collect(self):
        gauge = GaugeMetricFamily(
            "process_tcp_connections_total",
            "Number of TCP connections partitioned by direction (inbound, outbound) and TCP state",
            labels=["direction", "state"],
        )

        outbound_conns, inbound_conns, _, _ = _classify_tcp_connections()

        outbound_counts = Counter(conn.status for conn in outbound_conns)
        inbound_counts = Counter(conn.status for conn in inbound_conns)

        # 1. Outbound (외부 서버 호출 소켓)
        for state in self.TRACKED_STATES:
            if state != "LISTEN":  # 외부 호출에는 LISTEN이 존재하지 않음
                gauge.add_metric(["outbound", state], float(outbound_counts.get(state, 0)))

        # 2. Inbound (서버 리스닝 포트 및 클라이언트 접속 소켓)
        for state in self.TRACKED_STATES:
            gauge.add_metric(["inbound", state], float(inbound_counts.get(state, 0)))

        yield gauge


def _format_addr(addr) -> str | None:
    return f"{addr.ip}:{addr.port}" if addr else None


def _socket_info(conn, direction: str) -> dict:
    """psutil 소켓 정보를 API 응답용 dict로 변환"""
    laddr = _format_addr(conn.laddr)
    raddr = _format_addr(conn.raddr)
    if direction == "outbound":
        desc = f"외부 서버({raddr})로의 호출"
    elif conn.status == psutil.CONN_LISTEN:
        desc = "서버 수신 대기 (LISTEN)"
    else:
        desc = f"클라이언트({raddr})의 접속"
    return {
        "fd": conn.fd,
        "direction": direction,
        "status": conn.status,
        "local_address": laddr,
        "remote_address": raddr,
        "description": desc,
    }


def get_tcp_connection_details() -> dict:
    """현재 프로세스의 인바운드 및 아웃바운드 소켓 상세 정보 반환 (API용)"""
    outbound_conns, inbound_conns, listen_ports, pid = _classify_tcp_connections()

    outbound_status_counts = Counter(conn.status for conn in outbound_conns)
    inbound_status_counts = Counter(conn.status for conn in inbound_conns)

    sockets_detail = [_socket_info(c, "outbound") for c in outbound_conns]
    sockets_detail += [_socket_info(c, "inbound") for c in inbound_conns]

    return {
        "pid": pid,
        "server_listen_ports": list(listen_ports),
        "total_connections": len(outbound_conns) + len(inbound_conns),
        "summary": {
            "outbound_total": len(outbound_conns),
            "inbound_total": len(inbound_conns),
            "outbound_states": dict(outbound_status_counts),
            "inbound_states": dict(inbound_status_counts),
        },
        "sockets": sockets_detail,
    }


_collector_registered = False


def init_metrics(app: FastAPI):
    """Prometheus Instrumentator 등록 및 커넥션 풀 / 인아웃바운드 TCP 소켓 메트릭 바인딩"""
    global _collector_registered

    # 1. 커넥션 풀 콜백 바인딩
    HTTP_POOL_ACTIVE_CONNECTIONS.set_function(
        lambda: float(get_pool_status().get("current_active_connections", 0))
    )
    HTTP_POOL_MAX_CONNECTIONS.set_function(
        lambda: float(get_pool_status().get("max_connections", 0) or 0)
    )
    HTTP_POOL_KEEPALIVE_CONNECTIONS.set_function(
        lambda: float(get_pool_status().get("max_keepalive_connections", 0) or 0)
    )

    # 2. 인바운드 / 아웃바운드 구분 실시간 TCP 수집기(Collector) 등록
    if not _collector_registered:
        REGISTRY.register(DirectionalTCPConnectionCollector())
        _collector_registered = True

    # 3. FastAPI 기본 HTTP 요청 메트릭 및 /metrics 노출
    instrumentator = Instrumentator(
        should_group_status_codes=False,
        should_ignore_untemplated=True,
        excluded_handlers=["/metrics", "/health"],
    )
    instrumentator.instrument(app).expose(app, endpoint="/metrics", tags=["Monitoring"])

    logger.info("Prometheus 메트릭(/metrics) 및 인/아웃바운드 구분 TCP 소켓 수집기가 활성화되었습니다.")
