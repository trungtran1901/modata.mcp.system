"""
utils/session.py  (modata-mcp)

Session context — đọc quyền từ Redis (primary) hoặc PostgreSQL (fallback).

Luồng:
  1. Redis SISMEMBER O(1) — không đọc dict lớn, không tốn token
  2. Fallback PostgreSQL nếu Redis down hoặc key chưa có
  3. In-process cache 60s — nhiều tool calls chỉ 1 lần IO

LLM không bao giờ thấy danh sách collections — check xảy ra
hoàn toàn ở tầng MCP trước khi tool trả kết quả.
"""
from __future__ import annotations

import json
import logging
import time
import threading
from dataclasses import dataclass, field

import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.config import settings

logger = logging.getLogger(__name__)

_SESSION_CACHE_TTL = 60
_session_cache: dict[str, tuple[object, float]] = {}
_cache_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────
# SESSION CONTEXT
# ─────────────────────────────────────────────────────────────

@dataclass
class SessionContext:
    session_id: str
    accessible: dict[str, list[str]] = field(default_factory=dict)

    def can_access(self, instance_name: str) -> bool:
        return instance_name in self.accessible

    def get_ma_chuc_nang_list(self, instance_name: str) -> list[str]:
        return self.accessible.get(instance_name, [])

    def accessible_instance_names(self) -> list[str]:
        return list(self.accessible.keys())

    @classmethod
    def empty(cls, session_id: str) -> "SessionContext":
        return cls(session_id=session_id, accessible={})


def _parse_accessible(raw) -> dict[str, list[str]]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {k: v if isinstance(v, list) else [] for k, v in raw.items()}
    if isinstance(raw, list):
        return {name: [] for name in raw if isinstance(name, str)}
    if isinstance(raw, str):
        try:
            return _parse_accessible(json.loads(raw))
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}


# ─────────────────────────────────────────────────────────────
# REDIS READER
# ─────────────────────────────────────────────────────────────

def _read_from_redis(session_id: str) -> dict[str, list[str]] | None:
    """
    Đọc permission từ Redis Sets.
    Trả về None nếu Redis down hoặc key không tồn tại.
    """
    try:
        import redis as redis_lib
        r = redis_lib.Redis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=1,
            socket_timeout=1,
            decode_responses=True,
        )
        inst_key = f"perm:{session_id}:instances"
        instances = r.smembers(inst_key)

        if not instances:
            return None   # Key không tồn tại → fallback PG

        # Batch get tất cả ma_chuc_nang trong 1 pipeline
        pipe = r.pipeline(transaction=False)
        for inst in instances:
            pipe.smembers(f"perm:{session_id}:ma:{inst}")
        ma_results = pipe.execute()

        accessible: dict[str, list[str]] = {}
        for inst, ma_set in zip(instances, ma_results):
            accessible[inst] = [m for m in (ma_set or set()) if m != "__any__"]

        logger.debug(
            "Redis perm HIT: session=%s, %d collections",
            session_id, len(accessible),
        )
        return accessible

    except Exception as e:
        logger.debug("Redis perm read failed: %s", e)
        return None


# ─────────────────────────────────────────────────────────────
# POSTGRESQL FALLBACK READER
# ─────────────────────────────────────────────────────────────

def _read_from_pg(session_id: str) -> dict[str, list[str]]:
    try:
        pg = psycopg2.connect(settings.PG_DSN)
        with pg.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT accessible_context FROM rag_sessions WHERE session_id = %s",
                (session_id,),
            )
            row = cur.fetchone()
        pg.close()

        if not row:
            logger.warning("Session not found in PG: %s", session_id)
            return {}

        accessible = _parse_accessible(row.get("accessible_context"))
        logger.debug(
            "PG perm fallback: session=%s, %d collections",
            session_id, len(accessible),
        )
        return accessible

    except Exception as e:
        logger.error("PG perm read error: %s", e)
        return {}


# ─────────────────────────────────────────────────────────────
# PUBLIC API — với in-process cache
# ─────────────────────────────────────────────────────────────

def get_session_context(session_id: str) -> SessionContext:
    """
    Thứ tự: in-process cache → Redis → PostgreSQL
    Nhiều tool calls trong 1 request → chỉ 1 lần IO duy nhất.
    """
    now = time.monotonic()

    # 1. In-process cache
    with _cache_lock:
        if session_id in _session_cache:
            ctx, expires_at = _session_cache[session_id]
            if now < expires_at:
                return ctx
            del _session_cache[session_id]

    # 2. Redis
    accessible = _read_from_redis(session_id)

    # 3. Fallback PostgreSQL
    if accessible is None:
        accessible = _read_from_pg(session_id)


    # Dedup để tránh duplicate từ Redis/PG fallback
    accessible = {k: list(dict.fromkeys(v)) for k, v in accessible.items()}
    ctx = SessionContext(session_id=session_id, accessible=accessible)

    # Lưu in-process cache
    with _cache_lock:
        _session_cache[session_id] = (ctx, now + _SESSION_CACHE_TTL)

    return ctx


# Backward-compat
def get_accessible(session_id: str) -> list[str]:
    return get_session_context(session_id).accessible_instance_names()

def check_access(instance_name: str, session_id: str) -> bool:
    return get_session_context(session_id).can_access(instance_name)