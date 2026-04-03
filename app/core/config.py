"""app/core/config.py — Config cho MCP Gateway project"""
from functools import lru_cache
from typing import Optional

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── App ───────────────────────────────────────────────────
    APP_NAME: str = "MODATA MCP Gateway"
    DEBUG:    bool = False
    LOG_LEVEL: str = "INFO"

    # ── Qdrant ────────────────────────────────────────────────
    QDRANT_HOST:        str          = "localhost"
    QDRANT_PORT:        int          = 6333
    QDRANT_API_KEY:     Optional[str] = None
    QDRANT_COLLECTION:  str          = "modata_rag"
    QDRANT_VECTOR_SIZE: int          = 1024

    @computed_field
    @property
    def QDRANT_URL(self) -> str:
        return f"http://{self.QDRANT_HOST}:{self.QDRANT_PORT}"

    # ── PostgreSQL (session store) ────────────────────────────
    PG_HOST:     str = "localhost"
    PG_PORT:     int = 5432
    PG_USER:     str = "admin"
    PG_PASSWORD: str = "change_me"
    PG_DATABASE: str = "vectordb"

    @computed_field
    @property
    def PG_DSN(self) -> str:
        return (
            f"postgresql://{self.PG_USER}:{self.PG_PASSWORD}"
            f"@{self.PG_HOST}:{self.PG_PORT}/{self.PG_DATABASE}"
        )

    # ── Embed server (remote, OpenAI-compatible) ──────────────
    EMBED_BASE_URL:          str = "http://192.168.100.114:8088/v1"
    EMBED_API_KEY:           str = ""
    EMBED_MODEL:             str = "Qwen/Qwen3-Embedding-0.6B"
    EMBED_QUERY_INSTRUCTION: str = (
        "Instruct: Given a user query about internal company data, "
        "retrieve relevant passages that answer the query\nQuery:"
    )
    EMBED_TIMEOUT: int = 30

    # ── MongoDB ───────────────────────────────────────────────
    MONGO_URI:      str = "mongodb://localhost:27017"
    MONGO_DATABASE: str = "generic_instance_v2"

    MONGO_COL_NHAN_VIEN:     str = "instance_data_thong_tin_nhan_vien"
    MONGO_COL_PHAN_QUYEN:    str = "instance_data_danh_sach_phan_quyen_chuc_nang"
    MONGO_COL_SYS_CONF_VIEW: str = "instance_data_sys_conf_view"

    # ── SMTP ─────────────────────────────────────────────────
    MAIL_HOST:     str  = "mail.hitc.vn"
    MAIL_PORT:     int  = 465
    MAIL_USE_TLS:  bool = False
    MAIL_USE_SSL:  bool = True
    MAIL_USERNAME: str  = ""
    MAIL_PASSWORD: str  = ""
    MAIL_FROM:     str  = "noreply@hitc.vn"

    # ── RAG tuning ────────────────────────────────────────────
    RAG_TOP_K:           int   = 8
    RAG_SCORE_THRESHOLD: float = 0.35

    # ── Company ───────────────────────────────────────────────
    DEFAULT_COMPANY_CODE: str = "HITC"

    # ── Redis (schema cache) ─────────────────────────────────
    REDIS_HOST:     str          = "localhost"
    REDIS_PORT:     int          = 6379
    REDIS_PASSWORD: Optional[str] = None
    REDIS_DB:       int          = 0
    # TTL cho schema cache (giây) — schema thay đổi rất hiếm
    SCHEMA_CACHE_TTL: int = 300

    @computed_field
    @property
    def REDIS_URL(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

    # ── MCP Gateway ───────────────────────────────────────────
    MCP_HOST: str = "0.0.0.0"
    MCP_PORT: int = 8001


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()