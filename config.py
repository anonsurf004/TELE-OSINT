"""
config.py — Central configuration using pydantic-settings.
All values loaded from .env automatically.
"""
from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Telegram ───────────────────────────────────────────────────────────────
    telegram_api_id: int = Field(..., description="Telegram API ID from my.telegram.org")
    telegram_api_hash: str = Field(..., description="Telegram API Hash")
    telegram_phone: str = Field(..., description="Phone number with country code")
    telegram_session_name: str = Field("osint_session", description="Telethon session file name")

    # ── Elasticsearch ──────────────────────────────────────────────────────────
    elasticsearch_url: str = Field("http://localhost:9200")
    elasticsearch_index_messages: str = Field("tele_messages")
    elasticsearch_index_channels: str = Field("tele_channels")
    elasticsearch_refresh_interval: str = Field("1s")

    # ── FastAPI ────────────────────────────────────────────────────────────────
    api_host: str = Field("0.0.0.0")
    api_port: int = Field(8000)
    api_reload: bool = Field(False)

    # ── Seed channels ──────────────────────────────────────────────────────────
    seed_channels: str = Field(
        "cybersecuritynews,exploit_db_feed,thehackernews,cve_updates",
        description="Comma-separated seed channel usernames",
    )

    @property
    def seed_channel_list(self) -> List[str]:
        return [c.strip() for c in self.seed_channels.split(",") if c.strip()]

    # ── System tuning ──────────────────────────────────────────────────────────
    log_level: str = Field("INFO")
    max_channels: int = Field(200, description="Max channels to monitor simultaneously")
    crawl_queue_size: int = Field(500)
    index_queue_size: int = Field(2_000, description="Max docs buffered before ES bulk flush")
    broadcast_queue_size: int = Field(5_000, description="Max events buffered for WebSocket fan-out")
    bulk_index_batch: int = Field(500, description="Docs per ES bulk request")
    bulk_index_interval: float = Field(2.0, description="Seconds between bulk flushes")
    dedup_cache_size: int = Field(100_000, description="LRU cache size for seen hashes")
    ws_broadcast_concurrency: int = Field(50, description="Max concurrent WebSocket sends per broadcast")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
