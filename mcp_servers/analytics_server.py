"""
mcp_servers/analytics_server.py
MCP Analytics Server — aggregation, thống kê, báo cáo.

Tools (prefix: analytics_):
  count  aggregate  group_by_field  compare_periods
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from fastmcp import FastMCP

from app.db.mongo import get_db
from utils.session import get_session_context

logger = logging.getLogger(__name__)
mcp    = FastMCP("modata-analytics")


@mcp.tool()
def count(session_id: str, instance_name: str, filter: dict = None) -> str:
    """Đếm số lượng records theo điều kiện."""
    if not get_session_context(session_id).can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})
    db  = get_db()
    flt = filter or {}
    flt["is_deleted"] = {"$ne": True}
    total = db[f"instance_data_{instance_name}"].count_documents(flt)
    return json.dumps({"count": total}, ensure_ascii=False)


@mcp.tool()
def aggregate(session_id: str, instance_name: str, pipeline: list) -> str:
    """Chạy MongoDB aggregation pipeline."""
    if not get_session_context(session_id).can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})
    db     = get_db()
    result = list(db[f"instance_data_{instance_name}"].aggregate(pipeline))
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def group_by_field(
    session_id:    str,
    instance_name: str,
    field:         str,
    filter:        dict = None,
    top_n:         int  = 20,
) -> str:
    """Thống kê phân bổ theo field. Trả về [{value, count}]."""
    if not get_session_context(session_id).can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})
    db  = get_db()
    flt = filter or {}
    flt["is_deleted"] = {"$ne": True}
    pipeline = [
        {"$match":   flt},
        {"$group":   {"_id": f"${field}", "count": {"$sum": 1}}},
        {"$sort":    {"count": -1}},
        {"$limit":   top_n},
        {"$project": {"value": "$_id", "count": 1, "_id": 0}},
    ]
    result = list(db[f"instance_data_{instance_name}"].aggregate(pipeline))
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
def compare_periods(
    session_id:    str,
    instance_name: str,
    date_field:    str,
    period_1_from: str,
    period_1_to:   str,
    period_2_from: str,
    period_2_to:   str,
) -> str:
    """
    So sánh số lượng records giữa 2 khoảng thời gian.
    Định dạng ngày: YYYY-MM-DD
    """
    if not get_session_context(session_id).can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})
    db  = get_db()
    col = db[f"instance_data_{instance_name}"]

    def _count(f: str, t: str) -> int:
        return col.count_documents({
            "is_deleted": {"$ne": True},
            date_field: {
                "$gte": datetime.fromisoformat(f),
                "$lte": datetime.fromisoformat(t),
            },
        })

    c1   = _count(period_1_from, period_1_to)
    c2   = _count(period_2_from, period_2_to)
    diff = c2 - c1
    pct  = round(diff / c1 * 100, 1) if c1 > 0 else None
    return json.dumps({
        "period_1":       {"from": period_1_from, "to": period_1_to, "count": c1},
        "period_2":       {"from": period_2_from, "to": period_2_to, "count": c2},
        "difference":     diff,
        "percent_change": pct,
    }, ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8012)