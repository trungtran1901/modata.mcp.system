"""
utils/schema_cache.py

View-aware schema cache — backend Redis.

Cache key  : "schema:{instance_name}:{ma_chuc_nang}"
Cache value: JSON serialized SchemaInfo
TTL        : SCHEMA_CACHE_TTL (mặc định 300s)

Ưu điểm so với OrderedDict in-process:
  - Shared across tất cả uvicorn workers (không mỗi worker load riêng)
  - Survived restart (schema không cần reload ngay sau deploy)
  - Invalidate tức thì với KEYS pattern — tất cả workers đều thấy ngay
  - Redis tự expire theo TTL, không cần eviction logic thủ công

Fallback:
  Nếu Redis không kết nối được → tự động fallback về in-process dict
  (không raise exception, hệ thống vẫn chạy được, chỉ mất shared cache)
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from app.core.config import settings
from app.db.mongo import get_db

logger = logging.getLogger(__name__)

# Prefix tất cả key trong Redis để dễ quản lý + tránh xung đột
_KEY_PREFIX = "modata:schema:"


# ─────────────────────────────────────────────────────────────
# DATA CLASSES  (giữ nguyên — không phụ thuộc backend cache)
# ─────────────────────────────────────────────────────────────

class FieldInfo:
    __slots__ = ("name", "display", "type", "index")

    def __init__(self, name: str, display: str, type_: str, index: int):
        self.name    = name
        self.display = display
        self.type    = type_
        self.index   = index

    def to_dict(self) -> dict:
        return {"name": self.name, "display": self.display, "type": self.type}

    @classmethod
    def from_dict(cls, d: dict) -> "FieldInfo":
        return cls(
            name=d["name"],
            display=d.get("display", d["name"]),
            type_=d.get("type", "String"),
            index=d.get("index", 999),
        )


class SchemaInfo:
    """
    Schema + union fields từ tất cả view của 1 cặp (instance_name, ma_chuc_nang).
    """
    __slots__ = ("instance_name", "display_name", "fields", "field_names")

    def __init__(self, instance_name: str, display_name: str, fields: list):
        self.instance_name            = instance_name
        self.display_name             = display_name
        self.fields: list[FieldInfo]  = fields
        self.field_names: set[str]    = {f.name for f in fields}

    def allowed_projection(self, requested: list[str] | None) -> dict | None:
        """
        Build MongoDB projection chỉ gồm fields được phép.
          requested=None  → toàn bộ allowed fields
          requested=[...] → giao với allowed, loại bỏ field ngoài quyền
        Trả về None nếu không có field nào.
        """
        if not self.fields:
            return None
        if not requested:
            return {f.name: 1 for f in self.fields}
        allowed = [f for f in requested if f in self.field_names]
        return {f: 1 for f in allowed} if allowed else None

    def to_dict(self) -> dict:
        return {
            "instance_name": self.instance_name,
            "display_name":  self.display_name,
            "fields":        [f.to_dict() for f in self.fields],
        }

    def serialize(self) -> str:
        """Serialize sang JSON string để lưu Redis."""
        return json.dumps({
            "instance_name": self.instance_name,
            "display_name":  self.display_name,
            "fields": [
                {
                    "name":    f.name,
                    "display": f.display,
                    "type":    f.type,
                    "index":   f.index,
                }
                for f in self.fields
            ],
        }, ensure_ascii=False)

    @classmethod
    def deserialize(cls, raw: str) -> "SchemaInfo":
        """Deserialize từ JSON string Redis trả về."""
        data   = json.loads(raw)
        fields = [FieldInfo.from_dict(f) for f in data.get("fields", [])]
        return cls(
            instance_name=data["instance_name"],
            display_name=data.get("display_name", data["instance_name"]),
            fields=fields,
        )


# ─────────────────────────────────────────────────────────────
# REDIS CLIENT — singleton, lazy init
# ─────────────────────────────────────────────────────────────

_redis = None
_redis_ok = True   # False sau lần fail đầu tiên, thử lại mỗi 60s
_redis_retry_at = 0.0


def _get_redis():
    """
    Trả về Redis client. Lazy init, tự reconnect.
    Trả về None nếu Redis không khả dụng (fallback mode).
    """
    global _redis, _redis_ok, _redis_retry_at
    import time

    # Nếu đã biết Redis lỗi, chờ 60s rồi thử lại
    if not _redis_ok:
        if time.monotonic() < _redis_retry_at:
            return None
        logger.info("Retrying Redis connection...")

    try:
        if _redis is None:
            import redis as redis_lib
            _redis = redis_lib.Redis.from_url(
                settings.REDIS_URL,
                socket_connect_timeout=2,   # fail nhanh, không block request
                socket_timeout=2,
                decode_responses=True,       # nhận str thay vì bytes
                retry_on_timeout=False,
            )
        # Ping để kiểm tra kết nối thực sự
        _redis.ping()
        _redis_ok = True
        return _redis
    except Exception as e:
        _redis_ok      = False
        _redis_retry_at = time.monotonic() + 60
        _redis         = None
        logger.warning("Redis unavailable, falling back to no-cache: %s", e)
        return None


# ─────────────────────────────────────────────────────────────
# FALLBACK — in-process dict khi Redis không khả dụng
# ─────────────────────────────────────────────────────────────

_fallback: dict[str, SchemaInfo] = {}


# ─────────────────────────────────────────────────────────────
# LOADER — đọc từ MongoDB (giữ nguyên logic)
# ─────────────────────────────────────────────────────────────

def _load_from_db(instance_name: str, ma_chuc_nang: str) -> SchemaInfo:
    """
    3 MongoDB queries cố định bất kể số view_name:
      1. sys_conf_schema  → _id + display_name
      2. sys_conf_view    → tất cả view_name của (instance_name, ma_chuc_nang)
      3. sys_conf_cac_truong_view → batch $in view_ids → union fields
    """
    db = get_db()

    # 1. Schema
    schema = db["instance_data_sys_conf_schema"].find_one(
        {"name": instance_name, "is_deleted": {"$ne": True}},
        {"_id": 1, "display_name": 1},
    )
    if not schema:
        logger.warning("Schema không tìm thấy: %s", instance_name)
        return SchemaInfo(instance_name, instance_name, [])

    display_name = schema.get("display_name") or instance_name

    # 2. Views của (instance_name, ma_chuc_nang)
    views = list(db["instance_data_sys_conf_view"].find(
        {
            "instance_name": instance_name,
            "ma_chuc_nang":  ma_chuc_nang,
            "is_deleted":    {"$ne": True},
            "is_active":     {"$ne": False},
        },
        {"_id": 1, "view_name": 1},
    ))

    if not views:
        logger.debug("Không tìm thấy view: %s/%s", instance_name, ma_chuc_nang)
        return SchemaInfo(instance_name, display_name, [])

    view_ids = [str(v["_id"]) for v in views]
    logger.debug("Found %d views for %s/%s", len(views), instance_name, ma_chuc_nang)

    # 3. Batch-load fields — 1 query $in
    raw_fields = list(db["instance_data_sys_conf_cac_truong_view"].find(
        {
            "parent_id":  {"$in": view_ids},
            "is_deleted": {"$ne": True},
        },
        {"name": 1, "display": 1, "type": 1, "index": 1},
    ))

    # Union + dedup: giữ index nhỏ nhất nếu trùng tên
    seen: dict[str, FieldInfo] = {}
    for f in raw_fields:
        fname = (f.get("name") or "").strip()
        if not fname:
            continue
        idx = f.get("index") or 999
        if fname not in seen or idx < seen[fname].index:
            seen[fname] = FieldInfo(
                name=fname,
                display=f.get("display") or fname,
                type_=f.get("type", "String"),
                index=idx,
            )

    fields = sorted(seen.values(), key=lambda x: x.index)
    logger.debug(
        "SchemaInfo built: %s/%s → %d fields (union of %d views)",
        instance_name, ma_chuc_nang, len(fields), len(views),
    )
    return SchemaInfo(instance_name, display_name, fields)


# ─────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────

def _redis_key(instance_name: str, ma_chuc_nang: str) -> str:
    return f"{_KEY_PREFIX}{instance_name}:{ma_chuc_nang}"


def get_schema_info(instance_name: str, ma_chuc_nang: str) -> SchemaInfo:
    """
    Lấy SchemaInfo theo (instance_name, ma_chuc_nang).
    Thứ tự:  Redis HIT → return
             Redis MISS → load DB → SET Redis với TTL → return
             Redis DOWN → fallback in-process dict → return
    """
    rkey = _redis_key(instance_name, ma_chuc_nang)
    r    = _get_redis()

    # ── Redis path ────────────────────────────────────────────
    if r is not None:
        try:
            cached = r.get(rkey)
            if cached:
                logger.debug("Redis HIT: %s/%s", instance_name, ma_chuc_nang)
                return SchemaInfo.deserialize(cached)

            logger.debug("Redis MISS: %s/%s", instance_name, ma_chuc_nang)
            info = _load_from_db(instance_name, ma_chuc_nang)
            r.setex(rkey, settings.SCHEMA_CACHE_TTL, info.serialize())
            return info

        except Exception as e:
            logger.warning("Redis error during get/set, fallback: %s", e)
            # Fall through to in-process fallback

    # ── Fallback path (Redis down) ────────────────────────────
    fkey = f"{instance_name}:{ma_chuc_nang}"
    if fkey in _fallback:
        logger.debug("Fallback HIT: %s/%s", instance_name, ma_chuc_nang)
        return _fallback[fkey]

    info = _load_from_db(instance_name, ma_chuc_nang)
    _fallback[fkey] = info   # không có TTL nhưng chỉ dùng khi Redis down
    return info


def invalidate(instance_name: str):
    """
    Xoá tất cả cache entries của 1 instance_name.
    Dùng KEYS pattern — phù hợp vì invalidate rất hiếm, không hot path.
    Đồng thời xoá fallback dict để đồng bộ.
    """
    r = _get_redis()
    if r is not None:
        try:
            pattern  = f"{_KEY_PREFIX}{instance_name}:*"
            keys     = r.keys(pattern)
            if keys:
                r.delete(*keys)
                logger.info(
                    "Redis invalidated %d key(s) for '%s'",
                    len(keys), instance_name,
                )
        except Exception as e:
            logger.warning("Redis invalidate error: %s", e)

    # Xoá fallback
    stale = [k for k in _fallback if k.startswith(f"{instance_name}:")]
    for k in stale:
        del _fallback[k]
    if stale:
        logger.info("Fallback invalidated %d entries for '%s'", len(stale), instance_name)


def invalidate_all():
    """Xoá toàn bộ schema cache (dùng khi deploy lớn)."""
    r = _get_redis()
    if r is not None:
        try:
            keys = r.keys(f"{_KEY_PREFIX}*")
            if keys:
                r.delete(*keys)
                logger.info("Redis invalidated all %d schema keys", len(keys))
        except Exception as e:
            logger.warning("Redis invalidate_all error: %s", e)
    _fallback.clear()


def cache_stats() -> dict:
    """Stats cho admin_server monitoring."""
    r = _get_redis()

    if r is not None:
        try:
            keys        = r.keys(f"{_KEY_PREFIX}*")
            info        = r.info("memory")
            redis_alive = True
        except Exception:
            keys        = []
            info        = {}
            redis_alive = False
    else:
        keys        = []
        info        = {}
        redis_alive = False

    return {
        "backend":          "redis" if redis_alive else "fallback_dict",
        "redis_url":        settings.REDIS_URL,
        "redis_alive":      redis_alive,
        "cached_keys":      len(keys),
        "ttl_seconds":      settings.SCHEMA_CACHE_TTL,
        "key_prefix":       _KEY_PREFIX,
        "fallback_entries": len(_fallback),
        "redis_used_memory": info.get("used_memory_human", "n/a"),
    }