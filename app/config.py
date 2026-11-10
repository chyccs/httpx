from functools import lru_cache
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ELASTICSEARCH_URL: str = "http://localhost:9200"
    DEFAULT_INDEX: str = "documents"
    HTTP_CLIENT_BACKEND: Literal["httpx", "aiohttp"] = "httpx"
    ES_MAX_CONNECTIONS: int = 50
    ES_MAX_KEEPALIVE_CONNECTIONS: int = 20
    ES_REQUEST_TIMEOUT: float = 10.0
    TCP_CACHE_TTL_SECONDS: float = 2.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
