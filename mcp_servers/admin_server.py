"""
mcp_servers/admin_server.py
MCP Admin Server — cache management, health, monitoring.

Tools (prefix: admin_):
  cache_stats       — thống kê LRU cache hiện tại
  invalidate_schema — xoá cache khi schema thay đổi
  health            — kiểm tra kết nối MongoDB + PostgreSQL
"""
from __future__ import annotations

import json
import logging

from fastmcp import FastMCP

from app.core.config import settings
from utils.schema_cache import cache_stats, invalidate, invalidate_all

logger = logging.getLogger(__name__)
mcp    = FastMCP("modata-admin")


@mcp.tool()
def get_cache_stats() -> str:
    """Thống kê Redis schema cache."""
    stats = cache_stats()
    return json.dumps(stats, ensure_ascii=False)


@mcp.tool()
def invalidate_schema_cache(instance_name: str) -> str:
    """Xoá schema cache khi cấu trúc view thay đổi."""
    invalidate(instance_name)
    return json.dumps({
        "status":        "ok",
        "instance_name": instance_name,
        "message":       f"Cache invalidated for '{instance_name}'. Sẽ reload từ DB trong lần query tiếp theo.",
    }, ensure_ascii=False)


@mcp.tool()
def invalidate_all_schema_cache() -> str:
    """Xoá toàn bộ schema cache."""
    invalidate_all()
    return json.dumps({
        "status":  "ok",
        "message": "Toàn bộ schema cache đã được xoá. Sẽ reload từ DB theo từng request.",
    }, ensure_ascii=False)


@mcp.tool()
def health_check() -> str:
    """Kiểm tra kết nối MongoDB và PostgreSQL."""
    results: dict[str, str] = {}

    # MongoDB
    try:
        from app.db.mongo import get_db
        get_db().command("ping")
        results["mongodb"] = "ok"
    except Exception as e:
        results["mongodb"] = f"error: {e}"

    # PostgreSQL
    try:
        import psycopg2
        pg = psycopg2.connect(settings.PG_DSN)
        pg.close()
        results["postgresql"] = "ok"
    except Exception as e:
        results["postgresql"] = f"error: {e}"

    results["cache"] = json.dumps(cache_stats())
    overall = "ok" if all(v == "ok" for k, v in results.items() if k != "cache") else "degraded"
    results["status"] = overall

    return json.dumps(results, ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8016)