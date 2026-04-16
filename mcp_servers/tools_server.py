"""
mcp_servers/tools_server.py
MCP Tools Server — utility tools: thời gian, danh mục, tổ chức.

KHÔNG còn search_employee_by_name hardcode.
Thay bằng: data_search_records(session_id, "thong_tin_nhan_vien", "keyword")
  → tự load searchable fields từ schema, flatten result theo display_name.

Tools (prefix: tools_):
  get_current_time
  calculate_working_days
  calculate_service_time
  lookup_danhmuc
  get_org_tree
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from fastmcp import FastMCP

from app.db.mongo import get_db

logger = logging.getLogger(__name__)
mcp    = FastMCP("modata-tools")


@mcp.tool()
def get_current_time(format: str = "full") -> str:
    """
    Lấy thời gian hiện tại.
    format: 'full' | 'date' | 'time' | 'timestamp' | 'unix_ms'
    """
    now = datetime.now()
    if format in ("full", "all"):
        result = {
            "datetime":    now.strftime("%Y-%m-%d %H:%M:%S"),
            "date":        now.strftime("%Y-%m-%d"),
            "time":        now.strftime("%H:%M:%S"),
            "day_of_week": ["Thứ 2","Thứ 3","Thứ 4","Thứ 5","Thứ 6","Thứ 7","Chủ nhật"][now.weekday()],
            "timestamp":   int(now.timestamp()),
            "weekday":     now.weekday(),
            "iso":         now.isoformat(),
        }
    elif format == "date":
        result = {"date": now.strftime("%Y-%m-%d")}
    elif format == "time":
        result = {"time": now.strftime("%H:%M:%S")}
    elif format == "timestamp":
        result = {"timestamp": int(now.timestamp())}
    elif format == "unix_ms":
        result = {"timestamp_ms": int(now.timestamp() * 1000)}
    else:
        result = {"datetime": now.strftime("%Y-%m-%d %H:%M:%S"), "timestamp": int(now.timestamp())}
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
def calculate_working_days(from_date: str, to_date: str) -> str:
    """Số ngày làm việc giữa 2 ngày (bỏ T7/CN). Định dạng YYYY-MM-DD."""
    try:
        import datetime as dt
        d1   = datetime.strptime(from_date, "%Y-%m-%d").date()
        d2   = datetime.strptime(to_date,   "%Y-%m-%d").date()
        days = sum(1 for i in range((d2 - d1).days + 1)
                   if (d1 + dt.timedelta(i)).weekday() < 5)
        return json.dumps({"working_days": days, "from": from_date, "to": to_date})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def calculate_service_time(start_date: str, format: str = "full") -> str:
    """Thời gian công tác từ start_date đến nay. format: full|years|months|days|working_days."""
    try:
        import datetime as dt
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        now   = datetime.now().date()

        if start > now:
            return json.dumps({"error": f"Ngày bắt đầu ({start_date}) lớn hơn hiện tại."})

        total_days   = (now - start).days
        working_days = sum(1 for i in range(total_days + 1)
                           if (start + dt.timedelta(i)).weekday() < 5)
        years  = total_days / 365.25
        months = total_days / 30.44

        day_diff   = now.day   - start.day
        month_diff = now.month - start.month
        year_diff  = now.year  - start.year
        if day_diff < 0:
            month_diff -= 1
            day_diff   += (now.replace(day=1) - dt.timedelta(days=1)).day
        if month_diff < 0:
            year_diff  -= 1
            month_diff += 12

        full = {
            "start_date":     str(start),
            "current_date":   str(now),
            "total_days":     total_days,
            "working_days":   working_days,
            "years_decimal":  round(years, 2),
            "months_decimal": round(months, 2),
            "years_full":     year_diff,
            "months_full":    month_diff,
            "days_full":      day_diff,
            "total_hours":    working_days * 8,
            "description":    (
                f"Đã làm việc {year_diff} năm {month_diff} tháng {day_diff} ngày "
                f"(≈ {round(years, 1)} năm / {total_days} ngày)"
            ),
        }
        if format == "full":         return json.dumps(full, ensure_ascii=False)
        elif format == "years":      return json.dumps({"years": round(years, 2)})
        elif format == "months":     return json.dumps({"months": round(months, 1)})
        elif format == "days":       return json.dumps({"days": total_days})
        elif format == "working_days": return json.dumps({"working_days": working_days, "total_hours": working_days * 8})
        return json.dumps(full, ensure_ascii=False)
    except ValueError as e:
        return json.dumps({"error": f"Định dạng ngày không hợp lệ: {e}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def lookup_danhmuc(
    loai_danh_muc: str,
    search:        str = "",
    company_code:  str = "HITC",
) -> str:
    """Tra cứu danh mục: chức vụ, đơn vị, loại hợp đồng, trạng thái..."""
    db  = get_db()
    flt: dict = {"is_deleted": {"$ne": True}, "company_code": company_code}
    if loai_danh_muc:
        flt["loai_danh_muc"] = loai_danh_muc
    if search:
        flt["ten"] = {"$regex": search, "$options": "i"}
    docs = list(
        db["instance_data_danh_muc_he_thong"]
        .find(flt, {"ma": 1, "ten": 1})
        .limit(50)
    )
    return json.dumps(
        [{"ma": d.get("ma"), "ten": d.get("ten")} for d in docs],
        ensure_ascii=False,
    )


@mcp.tool()
def get_org_tree(
    path:               str = "/HTC/",
    company_code:       str = "HITC",
    ten_don_vi_to_chuc: str = "",
    depth:              int = 2,
) -> str:
    """
    Cây tổ chức theo path và/hoặc tên đơn vị.
    
    Tham số:
    - path: prefix path để lọc (VD: "/HTC/" hoặc "/HTC/PHONG_KT/"). Mặc định "/HTC/"
    - company_code: mã công ty (VD: "HITC")
    - ten_don_vi_to_chuc: tên đơn vị cần tìm kiếm (VD: "Phòng phát triển sản phẩm",
      "Khối kỹ thuật"). Nếu để trống thì trả về toàn bộ cây theo path.
    - depth: độ sâu tối đa tính từ path gốc (mặc định 2)
    
    Trả về danh sách đơn vị khớp với _id, code, tên và path.
    """
    import re

    db = get_db()

    # ── Build query ───────────────────────────────────────────────────────────
    query: dict = {
        "is_deleted":   {"$ne": True},
        "kich_hoat":   True,
        "company_code": company_code,
    }

    # Nếu có tên tìm kiếm → dùng regex case-insensitive, bỏ dấu-safe
    # MongoDB không hỗ trợ collation tiếng Việt nên dùng regex unicode
    if ten_don_vi_to_chuc.strip():
        # Tách từ khóa thành các token, mỗi token là một điều kiện AND
        tokens = [t.strip() for t in ten_don_vi_to_chuc.split() if t.strip()]
        if tokens:
            query["$and"] = [
                {"ten_don_vi_to_chuc": {"$regex": token, "$options": "i"}}
                for token in tokens
            ]
        # Khi tìm theo tên → không giới hạn path (để tìm được toàn công ty)
        # Nhưng vẫn filter company_code
    else:
        # Không có tên → filter theo path prefix như cũ
        query["path"] = {"$regex": f"^{re.escape(path)}"}

    # ── Query MongoDB ─────────────────────────────────────────────────────────
    docs = list(
        db["instance_data_danh_muc_don_vi_to_chuc"]
        .find(query, {"code": 1, "ten_don_vi_to_chuc": 1, "path": 1})
        .sort("path", 1)
        .limit(50)  # giới hạn nhỏ hơn khi search tên để tránh nhiễu
    )

    # ── Filter depth (chỉ áp dụng khi KHÔNG search theo tên) ─────────────────
    if not ten_don_vi_to_chuc.strip():
        docs = [
            d for d in docs
            if d.get("path", "").count("/") - path.count("/") <= depth
        ]

    # ── Format kết quả ────────────────────────────────────────────────────────
    result = [
        {
            "_id":                str(d.get("_id")),
            "code":               d.get("code"),
            "ten_don_vi_to_chuc": d.get("ten_don_vi_to_chuc"),
            "path":               d.get("path"),
        }
        for d in docs
    ]

    # Nếu không tìm thấy gì khi search tên → fallback về path search
    if not result and ten_don_vi_to_chuc.strip():
        fallback_query = {
            "is_deleted":   {"$ne": True},
            "kich_hoat":   True,
            "company_code": company_code,
            "path":         {"$regex": f"^{re.escape(path)}"},
        }
        fallback_docs = list(
            db["instance_data_danh_muc_don_vi_to_chuc"]
            .find(fallback_query, {"code": 1, "ten_don_vi_to_chuc": 1, "path": 1})
            .sort("path", 1)
            .limit(200)
        )
        result = [
            {
                "_id":                str(d.get("_id")),
                "code":               d.get("code"),
                "ten_don_vi_to_chuc": d.get("ten_don_vi_to_chuc"),
                "path":               d.get("path"),
            }
            for d in fallback_docs
            if d.get("path", "").count("/") - path.count("/") <= depth
        ]

    return json.dumps(result, ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8014)