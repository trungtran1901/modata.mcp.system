"""
mcp_servers/hrm_attendance_server.py
MCP HRM Attendance Server — tra cứu dữ liệu chấm công.

Collection: instance_data_lich_su_cham_cong_tong_hop_cong
View: lich_su_cham_cong_nhan_vien_list (toàn bộ NV đều có quyền, lọc theo ten_dang_nhap)

Schema thực tế:
  ten_dang_nhap : "minhnb"          — khóa lọc chính, map với NV table
  ten_nhan_vien : "Nguyễn Bình Minh"
  ma_nhan_vien  : "B0003"
  ngay_chot_cong: "2026-03"         — kỳ chốt công, format YYYY-MM
  day           : "2026-04-06"      — ngày cụ thể, format YYYY-MM-DD (string)
  firstIn       : ISODate(...)      — giờ vào (UTC), +7h → giờ VN
  lastOut       : ISODate(...)      — giờ ra (UTC), +7h → giờ VN

Tools (prefix sau mount: hrm_att_):
  get_attendance_today     — chấm công hôm nay của 1 NV
  get_attendance_by_date   — chấm công 1 ngày cụ thể
  get_attendance_by_month  — toàn bộ chấm công trong tháng (kỳ chốt công)
  get_attendance_summary   — thống kê tháng: số ngày công, muộn, sớm
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from fastmcp import FastMCP

from app.db.mongo import get_db
from utils.session import get_session_context

logger = logging.getLogger(__name__)
mcp    = FastMCP("modata-hrm-attendance")

COLLECTION    = "instance_data_lich_su_cham_cong_tong_hop_cong"
INSTANCE_NAME = "lich_su_cham_cong_tong_hop_cong"

# View name — toàn bộ NV đều có quyền
ATTENDANCE_VIEW = "lich_su_cham_cong_nhan_vien_list"


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _session_ok(session_id: str) -> bool:
    """Session hợp lệ nếu có bất kỳ accessible collection nào."""
    ctx = get_session_context(session_id)
    return bool(ctx.accessible_instance_names())


def _utc_to_vn_time(dt: Any) -> str | None:
    """ISODate UTC → giờ Việt Nam HH:MM."""
    if dt is None:
        return None
    if hasattr(dt, "strftime"):
        return (dt + timedelta(hours=7)).strftime("%H:%M")
    return str(dt)


def _utc_to_vn_datetime(dt: Any) -> str | None:
    """ISODate UTC → DD/MM/YYYY HH:MM (giờ VN)."""
    if dt is None:
        return None
    if hasattr(dt, "strftime"):
        return (dt + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")
    return str(dt)


def _calc_work_hours(first_in: Any, last_out: Any) -> float | None:
    """Tính số giờ làm từ firstIn đến lastOut. Trả về None nếu thiếu dữ liệu."""
    if first_in is None or last_out is None:
        return None
    if not (hasattr(first_in, "timestamp") and hasattr(last_out, "timestamp")):
        return None
    diff = (last_out - first_in).total_seconds()
    if diff <= 0:
        return None
    return round(diff / 3600, 2)


def _flatten_record(doc: dict) -> dict:
    """Flatten 1 bản ghi chấm công → dict dễ đọc."""
    r: dict = {}

    if doc.get("day"):
        # day lưu dạng string "YYYY-MM-DD"
        try:
            d = datetime.strptime(doc["day"], "%Y-%m-%d")
            r["Ngày"] = d.strftime("%d/%m/%Y")
        except (ValueError, TypeError):
            r["Ngày"] = doc["day"]

    if doc.get("ten_nhan_vien"):
        r["Nhân viên"] = doc["ten_nhan_vien"]
    if doc.get("ten_dang_nhap"):
        r["Username"] = doc["ten_dang_nhap"]
    if doc.get("ma_nhan_vien"):
        r["Mã NV"] = doc["ma_nhan_vien"]

    first_in  = doc.get("firstIn")
    last_out  = doc.get("lastOut")
    vn_in     = _utc_to_vn_time(first_in)
    vn_out    = _utc_to_vn_time(last_out)

    if vn_in:
        r["Giờ vào"] = vn_in
    else:
        r["Giờ vào"] = "Chưa có dữ liệu"

    if vn_out:
        r["Giờ ra"] = vn_out
    else:
        r["Giờ ra"] = "Chưa có dữ liệu"

    hours = _calc_work_hours(first_in, last_out)
    if hours is not None:
        r["Số giờ làm"] = hours

    if doc.get("ngay_chot_cong"):
        r["Kỳ chốt công"] = doc["ngay_chot_cong"]

    return r

# ─────────────────────────────────────────────────────────────
# HELPER: convert tháng thực tế → kỳ chốt công
# ─────────────────────────────────────────────────────────────

def _to_ky_chot_cong(year_month: str) -> str:
    """
    Tự động convert tháng thực tế → kỳ chốt công (lùi 1 tháng).
    Input:  "2026-04"  → Output: "2026-03"
    Input:  "2026-01"  → Output: "2025-12"
    Nếu input đã là kỳ chốt công thật (agent đôi khi truyền đúng) thì
    hàm này vẫn lùi thêm 1 — nên cần detect xem có nên convert không.
    → Giải pháp: LUÔN nhận tháng thực tế từ agent, LUÔN trừ 1.
    """
    try:
        dt = datetime.strptime(year_month, "%Y-%m")
        # Trừ 1 tháng
        if dt.month == 1:
            return f"{dt.year - 1}-12"
        return f"{dt.year}-{dt.month - 1:02d}"
    except ValueError:
        raise ValueError(f"Định dạng không hợp lệ: {year_month}. Dùng YYYY-MM.")
# ─────────────────────────────────────────────────────────────
# TOOLS
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_attendance_today(
    session_id:   str,
    username:     str,
    company_code: str = "HITC",
) -> str:
    """
    Lấy dữ liệu chấm công hôm nay của 1 nhân viên.
    Filter: ten_dang_nhap=username, day=ngày hôm nay (YYYY-MM-DD).
    Trả về: giờ vào (firstIn+7h), giờ ra (lastOut+7h), số giờ làm.

    Args:
        session_id: Session ID
        username:   ten_dang_nhap của nhân viên
        company_code: Mã công ty
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    today = datetime.now().strftime("%Y-%m-%d")
    db    = get_db()
    doc   = db[COLLECTION].find_one({
        "is_deleted":    {"$ne": True},
        "ten_dang_nhap": username,
        "day":           today,
    })

    if not doc:
        return json.dumps({
            "username": username,
            "day":      today,
            "message":  f"Không có dữ liệu chấm công ngày {today}.",
        }, ensure_ascii=False)

    return json.dumps(_flatten_record(doc), ensure_ascii=False, default=str)


@mcp.tool()
def get_attendance_by_date(
    session_id:   str,
    username:     str,
    date:         str,
    company_code: str = "HITC",
) -> str:
    """
    Lấy dữ liệu chấm công 1 ngày cụ thể của 1 nhân viên.
    Filter: ten_dang_nhap=username, day=date.
    Trả về: giờ vào, giờ ra, số giờ làm.

    Args:
        session_id: Session ID
        username:   ten_dang_nhap của nhân viên
        date:       Ngày cần xem, format YYYY-MM-DD
        company_code: Mã công ty
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    # Validate và chuẩn hoá date
    try:
        d = datetime.strptime(date, "%Y-%m-%d")
        date_str = d.strftime("%Y-%m-%d")
    except ValueError:
        return json.dumps({"error": "Định dạng ngày không hợp lệ. Dùng YYYY-MM-DD."}, ensure_ascii=False)

    db  = get_db()
    doc = db[COLLECTION].find_one({
        "is_deleted":    {"$ne": True},
        "ten_dang_nhap": username,
        "day":           date_str,
    })

    if not doc:
        return json.dumps({
            "username": username,
            "day":      date_str,
            "message":  f"Không có dữ liệu chấm công ngày {date_str}.",
        }, ensure_ascii=False)

    return json.dumps(_flatten_record(doc), ensure_ascii=False, default=str)


@mcp.tool()
def get_attendance_by_month(
    session_id:   str,
    username:     str,
    year_month:   str,          # Agent truyền tháng THỰC TẾ, tool tự convert
    company_code: str = "HITC",
) -> str:
    """
    Lấy toàn bộ dữ liệu chấm công trong 1 tháng.
    QUAN TRỌNG: year_month là tháng THỰC TẾ (vd "2026-04"),
    tool sẽ tự động convert sang kỳ chốt công (lùi 1 tháng).

    Args:
        session_id:  Session ID
        username:    ten_dang_nhap của nhân viên
        year_month:  Tháng THỰC TẾ cần xem, format YYYY-MM (vd: "2026-04")
        company_code: Mã công ty
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    try:
        ky_chot_cong = _to_ky_chot_cong(year_month)
    except ValueError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    db   = get_db()
    docs = list(db[COLLECTION].find({
        "is_deleted":     {"$ne": True},
        "ten_dang_nhap":  username,
        "ngay_chot_cong": ky_chot_cong,   # ← dùng kỳ đã convert
    }).sort("day", 1))

    if not docs:
        return json.dumps({
            "username":         username,
            "thang_thuc_te":    year_month,
            "ngay_chot_cong":   ky_chot_cong,
            "message":          f"Không có dữ liệu chấm công tháng {year_month} (kỳ chốt công: {ky_chot_cong}).",
        }, ensure_ascii=False)

    records    = [_flatten_record(d) for d in docs]
    ngay_du    = sum(1 for d in docs if d.get("firstIn") and d.get("lastOut"))
    ngay_thieu = sum(1 for d in docs if not d.get("firstIn") or not d.get("lastOut"))
    tong_gio   = sum(
        _calc_work_hours(d.get("firstIn"), d.get("lastOut")) or 0
        for d in docs
    )

    return json.dumps({
        "username":         username,
        "thang_thuc_te":    year_month,
        "ngay_chot_cong":   ky_chot_cong,
        "tong_ngay":        len(docs),
        "ngay_du_lieu":     ngay_du,
        "ngay_thieu_lieu":  ngay_thieu,
        "tong_gio_lam":     round(tong_gio, 2),
        "records":          records,
    }, ensure_ascii=False, default=str)



@mcp.tool()
def get_attendance_summary(
    session_id:   str,
    username:     str,
    year_month:   str,          # Agent truyền tháng THỰC TẾ, tool tự convert
    company_code: str = "HITC",
) -> str:
    """
    Thống kê tổng hợp chấm công trong 1 tháng.
    QUAN TRỌNG: year_month là tháng THỰC TẾ (vd "2026-04"),
    tool sẽ tự động convert sang kỳ chốt công (lùi 1 tháng).

    Args:
        session_id:  Session ID
        username:    ten_dang_nhap của nhân viên
        year_month:  Tháng THỰC TẾ cần xem, format YYYY-MM (vd: "2026-04")
        company_code: Mã công ty
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    try:
        ky_chot_cong = _to_ky_chot_cong(year_month)
    except ValueError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    db   = get_db()
    docs = list(db[COLLECTION].find({
        "is_deleted":     {"$ne": True},
        "ten_dang_nhap":  username,
        "ngay_chot_cong": ky_chot_cong,   # ← dùng kỳ đã convert
    }).sort("day", 1))

    if not docs:
        return json.dumps({
            "username":       username,
            "thang_thuc_te":  year_month,
            "ngay_chot_cong": ky_chot_cong,
            "message":        f"Không có dữ liệu chấm công tháng {year_month} (kỳ chốt công: {ky_chot_cong}).",
        }, ensure_ascii=False)

    # ... phần tính toán giữ nguyên, chỉ thay ngay_chot_cong → ky_chot_cong khi query ...
    tong_ngay  = len(docs)
    ngay_du    = [d for d in docs if d.get("firstIn") and d.get("lastOut")]
    ngay_thieu = [d for d in docs if not d.get("firstIn") or not d.get("lastOut")]
    tong_gio   = sum(
        _calc_work_hours(d.get("firstIn"), d.get("lastOut")) or 0
        for d in ngay_du
    )

    gio_vao_list = [d["firstIn"] for d in ngay_du if d.get("firstIn")]
    gio_ra_list  = [d["lastOut"] for d in ngay_du if d.get("lastOut")]

    gio_vao_som_nhat  = _utc_to_vn_datetime(min(gio_vao_list)) if gio_vao_list else None
    gio_vao_muon_nhat = _utc_to_vn_datetime(max(gio_vao_list)) if gio_vao_list else None
    gio_ra_som_nhat   = _utc_to_vn_datetime(min(gio_ra_list))  if gio_ra_list  else None
    gio_ra_muon_nhat  = _utc_to_vn_datetime(max(gio_ra_list))  if gio_ra_list  else None

    ngay_thieu_list = []
    for d in ngay_thieu:
        try:
            dt = datetime.strptime(d["day"], "%Y-%m-%d")
            ngay_thieu_list.append(dt.strftime("%d/%m/%Y"))
        except (ValueError, KeyError):
            ngay_thieu_list.append(d.get("day", "?"))

    return json.dumps({
        "username":           username,
        "thang_thuc_te":      year_month,
        "ngay_chot_cong":     ky_chot_cong,
        "ten_nhan_vien":      docs[0].get("ten_nhan_vien", ""),
        "tong_ngay_cong":     tong_ngay,
        "ngay_du_lieu":       len(ngay_du),
        "ngay_thieu_lieu":    len(ngay_thieu),
        "tong_gio_lam":       round(tong_gio, 2),
        "gio_vao_som_nhat":   gio_vao_som_nhat,
        "gio_vao_muon_nhat":  gio_vao_muon_nhat,
        "gio_ra_som_nhat":    gio_ra_som_nhat,
        "gio_ra_muon_nhat":   gio_ra_muon_nhat,
        "ngay_khong_du_lieu": ngay_thieu_list,
        "summary": (
            f"Tháng {year_month}: {len(ngay_du)}/{tong_ngay} ngày có đủ dữ liệu, "
            f"tổng {round(tong_gio, 2)} giờ làm việc."
        ),
    }, ensure_ascii=False, default=str)

@mcp.tool()
def get_attendance_range(
    session_id:   str,
    username:     str,
    from_date:    str,
    to_date:      str,
    company_code: str = "HITC",
) -> str:
    """
    Lấy dữ liệu chấm công trong khoảng ngày bất kỳ (không giới hạn kỳ chốt công).
    Filter: ten_dang_nhap=username, day >= from_date AND day <= to_date.
    Dùng khi cần xem chấm công nhiều ngày không theo kỳ chốt công.

    Args:
        session_id:  Session ID
        username:    ten_dang_nhap của nhân viên
        from_date:   Từ ngày YYYY-MM-DD
        to_date:     Đến ngày YYYY-MM-DD
        company_code: Mã công ty
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    try:
        datetime.strptime(from_date, "%Y-%m-%d")
        datetime.strptime(to_date,   "%Y-%m-%d")
    except ValueError:
        return json.dumps({"error": "Định dạng ngày không hợp lệ. Dùng YYYY-MM-DD."}, ensure_ascii=False)

    db   = get_db()
    # day là string "YYYY-MM-DD" → so sánh string lexicographic đúng với ISO format
    docs = list(db[COLLECTION].find({
        "is_deleted":    {"$ne": True},
        "ten_dang_nhap": username,
        "day":           {"$gte": from_date, "$lte": to_date},
    }).sort("day", 1))

    if not docs:
        return json.dumps({
            "username":  username,
            "from_date": from_date,
            "to_date":   to_date,
            "message":   f"Không có dữ liệu chấm công từ {from_date} đến {to_date}.",
        }, ensure_ascii=False)

    records  = [_flatten_record(d) for d in docs]
    tong_gio = sum(
        _calc_work_hours(d.get("firstIn"), d.get("lastOut")) or 0
        for d in docs
    )

    return json.dumps({
        "username":    username,
        "from_date":   from_date,
        "to_date":     to_date,
        "tong_ngay":   len(docs),
        "tong_gio_lam": round(tong_gio, 2),
        "records":     records,
    }, ensure_ascii=False, default=str)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8019)