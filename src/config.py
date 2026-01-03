import os
from dataclasses import dataclass
from typing import Optional


def env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def env_bool(key: str, default: bool) -> bool:
    val = os.environ.get(key)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    database_url: str
    concurrency: int = env_int("CONCURRENCY", 8)
    per_host_limit: int = env_int("PER_HOST_LIMIT", 2)
    request_timeout: float = env_float("REQUEST_TIMEOUT", 10.0)
    connect_timeout: float = env_float("CONNECT_TIMEOUT", 5.0)
    max_response_bytes: int = env_int("MAX_RESPONSE_BYTES", 1_048_576)  # 1 MB cap
    max_retries: int = env_int("MAX_RETRIES", 3)
    user_agent: str = os.environ.get(
        "USER_AGENT",
        "disposable-domain-crawler/0.1 (+https://example.invalid; contact=security@example.invalid)",
    )
    sources_json: Optional[str] = os.environ.get("SOURCES_JSON")
    dns_validate: bool = env_bool("DNS_VALIDATE", True)
    dns_timeout: float = env_float("DNS_TIMEOUT", 3.0)
    dns_concurrency: int = env_int("DNS_CONCURRENCY", 20)


def load_settings() -> Settings:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL must be set to a Postgres connection string")
    return Settings(database_url=db_url)
