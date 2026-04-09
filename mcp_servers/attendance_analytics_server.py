"""
mcp_servers/attendance_analytics_server.py  (v2)
MCP Attendance Analytics Server — tổng hợp bảng chấm công, xuất Excel, gửi mail.

Thay đổi so với v1:
  - Fix lấy đơn từ: dùng tu_ngay/den_ngay thay vì ngay_nop_don
  - Fix holidays: query theo kỳ chấm công (26/M-1 → 25/M), không chỉ tháng M
  - Thêm daily_detail: danh sách chi tiết từng ngày (thứ, ngày, check-in, check-out, ghi chú)
  - Excel dynamic columns: sheet "Tổng hợp" + sheet "Chi tiết ngày"
  - Data tool riêng biệt để LLM có thể xử lý thêm

Tools (prefix att_ana_):
  get_attendance_data          — trả JSON đầy đủ (tổng hợp + chi tiết ngày)
  export_attendance_excel      — xuất Excel 2 sheet từ data JSON
  compute_and_export           — all-in-one: tính + xuất Excel
  send_attendance_report       — tính + xuất + gửi mail
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, date
from typing import Any, Optional

from fastmcp import FastMCP

from app.db.mongo import get_db
from utils.session import get_session_context

logger = logging.getLogger(__name__)
mcp = FastMCP("modata-attendance-analytics")

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
WORK_START_H, WORK_START_M = 8, 30    # 08:30
WORK_END_H,   WORK_END_M   = 17, 30   # 17:30
WORK_MINUTES = (WORK_END_H * 60 + WORK_END_M) - (WORK_START_H * 60 + WORK_START_M)  # 540
STD_CONG = 26

WEEKDAY_VI = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"]
WEEKDAY_FULL = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ nhật"]


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _session_ok(session_id: str) -> bool:
    return bool(get_session_context(session_id).accessible_instance_names())


def _ev(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, dict):
        return v.get("label") or v.get("value")
    if isinstance(v, list):
        items = [_ev(i) for i in v if i is not None]
        items = [i for i in items if i is not None]
        return items[0] if len(items) == 1 else (items or None)
    if type(v).__name__ in ("ObjectId", "datetime"):
        return str(v)
    return v


def _utc_to_vn(dt: Any) -> datetime | None:
    """MongoDB UTC datetime → Vietnam time (UTC+7)."""
    if dt is None:
        return None
    if hasattr(dt, "strftime"):
        return dt + timedelta(hours=7)
    return None


def _hm(dt: datetime | None) -> tuple[int, int] | None:
    """datetime → (hour, minute) tuple."""
    return (dt.hour, dt.minute) if dt else None


def _fmt_time(hm: tuple | None) -> str:
    """(hour, minute) → "HH:MM", None → "-:-"."""
    if hm is None:
        return "-:-"
    return f"{hm[0]:02d}:{hm[1]:02d}"


def _minutes_late(fi: tuple | None) -> int:
    """Phút đi muộn so với 08:30."""
    if fi is None:
        return 0
    return max(0, fi[0] * 60 + fi[1] - (WORK_START_H * 60 + WORK_START_M))


def _minutes_early(lo: tuple | None) -> int:
    """Phút về sớm so với 17:30."""
    if lo is None:
        return 0
    return max(0, (WORK_END_H * 60 + WORK_END_M) - (lo[0] * 60 + lo[1]))


# ─────────────────────────────────────────────────────────────
# DATA LOADERS — fix query logic
# ─────────────────────────────────────────────────────────────

def _get_period_dates(year: int, month: int) -> tuple[date, date]:
    """Kỳ chấm công: 26/M-1 → 25/M."""
    if month == 1:
        start = date(year - 1, 12, 26)
    else:
        start = date(year, month - 1, 26)
    end = date(year, month, 25)
    return start, end


def _get_work_days_in_period(period_start: date, period_end: date,
                              off_weekdays: set[int], holidays: set[str]) -> list[date]:
    """Danh sách ngày làm việc chuẩn trong kỳ."""
    result = []
    cur = period_start
    while cur <= period_end:
        ds = cur.strftime("%Y-%m-%d")
        if cur.weekday() not in off_weekdays and ds not in holidays:
            result.append(cur)
        cur += timedelta(days=1)
    return result


def _get_all_days_in_period(period_start: date, period_end: date) -> list[date]:
    """Tất cả ngày trong kỳ (kể cả T7/CN)."""
    result = []
    cur = period_start
    while cur <= period_end:
        result.append(cur)
        cur += timedelta(days=1)
    return result


def _get_holidays_in_period(period_start: date, period_end: date,
                              company_code: str) -> set[str]:
    """
    Lấy tập ngày nghỉ lễ trong kỳ chấm công.
    Fix: query theo khoảng kỳ, không chỉ theo tháng.
    """
    db = get_db()
    # UTC: period_start 00:00 VN = period_start - 7h UTC
    from_utc = datetime(period_start.year, period_start.month, period_start.day) - timedelta(hours=7)
    to_utc   = datetime(period_end.year,   period_end.month,   period_end.day,   23, 59, 59)

    docs = list(db["instance_data_ngay_nghi_le"].find({
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "tu_ngay":  {"$lte": to_utc},
        "den_ngay": {"$gte": from_utc},
    }))

    result = set()
    for doc in docs:
        tu_vn  = _utc_to_vn(doc.get("tu_ngay"))
        den_vn = _utc_to_vn(doc.get("den_ngay"))
        if tu_vn and den_vn:
            cur = tu_vn.date()
            end = den_vn.date()
            # Chỉ lấy ngày nằm trong kỳ
            while cur <= end:
                if period_start <= cur <= period_end:
                    result.add(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)
    return result


def _get_holiday_names(period_start: date, period_end: date,
                        company_code: str) -> dict[str, str]:
    """dict[YYYY-MM-DD, tên ngày nghỉ lễ]."""
    db = get_db()
    from_utc = datetime(period_start.year, period_start.month, period_start.day) - timedelta(hours=7)
    to_utc   = datetime(period_end.year,   period_end.month,   period_end.day,   23, 59, 59)

    docs = list(db["instance_data_ngay_nghi_le"].find({
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "tu_ngay":  {"$lte": to_utc},
        "den_ngay": {"$gte": from_utc},
    }))

    result: dict[str, str] = {}
    for doc in docs:
        ten      = doc.get("ten_ngay_nghi", "Nghỉ lễ")
        tu_vn    = _utc_to_vn(doc.get("tu_ngay"))
        den_vn   = _utc_to_vn(doc.get("den_ngay"))
        if tu_vn and den_vn:
            cur = tu_vn.date()
            end = den_vn.date()
            while cur <= end:
                result[cur.strftime("%Y-%m-%d")] = ten
                cur += timedelta(days=1)
    return result


def _get_off_weekdays(company_code: str) -> set[int]:
    """Weekday indices của ngày nghỉ tuần (0=Mon…6=Sun)."""
    db = get_db()
    docs = list(db["instance_data_ngay_nghi_tuan"].find({
        "is_deleted":   {"$ne": True},
        "is_active":    {"$ne": False},
        "company_code": company_code,
    }))
    _MAP = {
        "thứ 2": 0, "thứ hai": 0, "t2": 0,
        "thứ 3": 1, "thứ ba": 1,  "t3": 1,
        "thứ 4": 2, "thứ tư": 2,  "t4": 2,
        "thứ 5": 3, "thứ năm": 3, "t5": 3,
        "thứ 6": 4, "thứ sáu": 4, "t6": 4,
        "thứ 7": 5, "thứ bảy": 5, "t7": 5,
        "chủ nhật": 6, "cn": 6,
    }
    off = set()
    for doc in docs:
        name = str(_ev(doc.get("loai_nghi_tuan")) or "").lower().strip()
        if name in _MAP:
            off.add(_MAP[name])
    return off


def _get_approved_leaves_in_period(
    username: str, period_start: date, period_end: date, company_code: str
) -> dict[str, list[dict]]:
    """
    Lấy đơn từ đã duyệt, overlap với kỳ chấm công.
    Fix: filter theo tu_ngay/den_ngay thay vì ngay_nop_don.
    Returns: {YYYY-MM-DD: [{"loai_don": ..., "ten_don": ...}, ...]}
    """
    db = get_db()

    # Query: đơn có tu_ngay <= period_end VÀ den_ngay >= period_start
    # UTC: +7h để ra VN
    from_utc = datetime(period_start.year, period_start.month, period_start.day) - timedelta(hours=7)
    to_utc   = datetime(period_end.year,   period_end.month,   period_end.day,   23, 59, 59)

    docs = list(db["instance_data_danh_sach_quan_ly_don_xin_nghi"].find({
        "is_deleted":                 {"$ne": True},
        "company_code":               company_code,
        "nguoi_nop_don.value":        username,
        "trang_thai_phe_duyet.value": "Đã duyệt",
        "tu_ngay":  {"$lte": to_utc},
        "den_ngay": {"$gte": from_utc},
    }))

    result: dict[str, list[dict]] = {}
    for doc in docs:
        loai    = doc.get("loai_don", "")
        tu_vn   = _utc_to_vn(doc.get("tu_ngay"))
        den_vn  = _utc_to_vn(doc.get("den_ngay"))
        if not tu_vn:
            continue
        if not den_vn:
            den_vn = tu_vn

        cur = tu_vn.date()
        end = den_vn.date()
        while cur <= end:
            # Chỉ lấy ngày trong kỳ
            if period_start <= cur <= period_end:
                ds = cur.strftime("%Y-%m-%d")
                result.setdefault(ds, [])
                result[ds].append({"loai_don": loai})
            cur += timedelta(days=1)

    return result


def _get_attendance_records(username: str, period_start: date,
                              period_end: date) -> dict[str, dict]:
    """
    Chấm công thô trong kỳ.
    Returns: {YYYY-MM-DD: {"firstIn": (h,m)|None, "lastOut": (h,m)|None}}
    
    Fix: query bằng day string range thay vì chỉ ngay_chot_cong
    vì một số record có thể lưu khác kỳ.
    """
    db = get_db()
    from_str = period_start.strftime("%Y-%m-%d")
    to_str   = period_end.strftime("%Y-%m-%d")

    # Kỳ chốt công = tháng của period_end - 1
    ky_end_month   = period_end.month
    ky_end_year    = period_end.year
    if ky_end_month == 1:
        ky_chot = f"{ky_end_year - 1}-12"
    else:
        ky_chot = f"{ky_end_year}-{ky_end_month - 1:02d}"

    docs = list(db["instance_data_lich_su_cham_cong_tong_hop_cong"].find({
        "is_deleted":    {"$ne": True},
        "ten_dang_nhap": username,
        "$or": [
            {"ngay_chot_cong": ky_chot},
            {"day": {"$gte": from_str, "$lte": to_str}},
        ],
    }))

    result: dict[str, dict] = {}
    for doc in docs:
        day_str = doc.get("day", "")
        if not day_str:
            continue
        # Chỉ lấy ngày trong kỳ
        try:
            d = date.fromisoformat(day_str)
            if not (period_start <= d <= period_end):
                continue
        except ValueError:
            continue

        fi = _hm(_utc_to_vn(doc.get("firstIn")))
        lo = _hm(_utc_to_vn(doc.get("lastOut")))
        result[day_str] = {"firstIn": fi, "lastOut": lo}
    return result


def _get_employees(filter_type: str, filter_value: str,
                   company_code: str) -> list[dict]:
    """Danh sách nhân viên theo bộ lọc."""
    db  = get_db()
    flt: dict = {
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "trang_thai_lao_dong.value": "Đang làm việc",
    }
    if filter_type == "username":
        flt["ten_dang_nhap"] = filter_value
    elif filter_type == "don_vi":
        flt["$or"] = [
            {"don_vi_cong_tac.value":       filter_value},
            {"don_vi_cong_tac.option.code": filter_value},
            {"phong_ban_phu_trach.value":   filter_value},
            {"path_don_vi_cong_tac": {"$regex": filter_value, "$options": "i"}},
        ]

    docs = list(db["instance_data_thong_tin_nhan_vien"].find(flt, {
        "ten_dang_nhap": 1, "ma_nhan_vien": 1,
        "ho_va_ten_co_dau": 1, "ho_va_ten": 1,
        "don_vi_cong_tac": 1, "vi_tri_cong_viec": 1,
    }).sort("ma_nhan_vien", 1))

    return [{
        "username":  d.get("ten_dang_nhap", ""),
        "ma_nv":     d.get("ma_nhan_vien", ""),
        "ho_va_ten": d.get("ho_va_ten_co_dau") or d.get("ho_va_ten", ""),
        "don_vi":    _ev(d.get("don_vi_cong_tac")) or "",
        "vi_tri":    _ev(d.get("vi_tri_cong_viec")) or "",
    } for d in docs]


# ─────────────────────────────────────────────────────────────
# CORE CALCULATOR
# ─────────────────────────────────────────────────────────────

def _calc_one_employee(
    username: str, period_start: date, period_end: date,
    off_weekdays: set[int], holidays: set[str], holiday_names: dict[str, str],
    company_code: str,
) -> dict:
    """
    Tính toán đầy đủ cho 1 nhân viên.
    Returns dict với:
      - summary: tổng hợp số liệu
      - daily_detail: danh sách chi tiết từng ngày trong kỳ
    """
    leaves    = _get_approved_leaves_in_period(username, period_start, period_end, company_code)
    att_recs  = _get_attendance_records(username, period_start, period_end)
    all_days  = _get_all_days_in_period(period_start, period_end)
    work_days = _get_work_days_in_period(period_start, period_end, off_weekdays, holidays)
    work_days_set = {d.strftime("%Y-%m-%d") for d in work_days}

    # Phân loại ngày nghỉ từ đơn từ
    nghi_phep_days: set[str] = set()
    nghi_le_days:   set[str] = {d for d in holidays if d in work_days_set}
    wfh_days:       set[str] = set()
    cong_tac_days:  set[str] = set()

    for ds, leave_list in leaves.items():
        for lv in leave_list:
            loai = lv["loai_don"]
            if loai in ("Nghỉ phép", "Nghỉ ốm"):
                nghi_phep_days.add(ds)
            elif loai == "Làm việc từ xa":
                wfh_days.add(ds)
            elif loai == "Đề nghị đi công tác":
                cong_tac_days.add(ds)

    # Tính từng ngày
    tong_cong_thuc_te = 0
    nghi_kl_days: list[str] = []
    late_gt240_days: list[str] = []
    late_60_240_days: list[str] = []
    late_lt60_pool_minutes = 0

    daily_detail: list[dict] = []

    for d in all_days:
        ds      = d.strftime("%Y-%m-%d")
        weekday = WEEKDAY_VI[d.weekday()]
        day_no  = d.day

        # Xác định trạng thái ngày
        is_weekend = d.weekday() in off_weekdays
        is_holiday = ds in holidays
        is_leave   = ds in nghi_phep_days
        is_wfh     = ds in wfh_days
        is_ct      = ds in cong_tac_days

        rec = att_recs.get(ds)
        fi  = rec["firstIn"]  if rec else None
        lo  = rec["lastOut"]  if rec else None

        note = ""
        day_type = ""  # Loại ngày

        if is_weekend:
            day_type = "Nghỉ tuần"
        elif is_holiday:
            day_type = f"Nghỉ lễ ({holiday_names.get(ds, 'Nghỉ lễ')})"
        elif is_leave:
            day_type = "Nghỉ phép/ốm"
        elif is_wfh:
            day_type = "WFH"
            tong_cong_thuc_te += 1
        elif is_ct:
            day_type = "Công tác"
            tong_cong_thuc_te += 1
        else:
            # Ngày làm việc thường — kiểm tra chấm công
            if fi is None and lo is None:
                day_type = "Nghỉ không lương"
                nghi_kl_days.append(ds)
            else:
                # Tính giờ làm thực tế
                if fi is not None and lo is not None:
                    actual_min = (lo[0] * 60 + lo[1]) - (fi[0] * 60 + fi[1])
                elif fi is not None:
                    actual_min = WORK_MINUTES  # về đủ giờ
                else:
                    actual_min = WORK_MINUTES  # vào đủ giờ

                if actual_min < 120:
                    day_type = "Nghỉ không lương"
                    nghi_kl_days.append(ds)
                else:
                    day_type = "Đi làm"
                    tong_cong_thuc_te += 1

                    # Phân tích đi muộn / về sớm
                    late_min  = _minutes_late(fi)
                    early_min = _minutes_early(lo)
                    total_dev = late_min + early_min

                    if total_dev > 0:
                        if total_dev >= 240:
                            late_gt240_days.append(ds)
                            note = f"ĐM/VS {total_dev}ph (>4h, -1)"
                        elif total_dev >= 60:
                            late_60_240_days.append(ds)
                            note = f"ĐM/VS {total_dev}ph (1-4h, -0.5)"
                        else:
                            late_lt60_pool_minutes += total_dev
                            note = f"ĐM/VS {total_dev}ph"

        daily_detail.append({
            "date":      ds,
            "day_no":    day_no,
            "weekday":   weekday,
            "check_in":  _fmt_time(fi),
            "check_out": _fmt_time(lo),
            "day_type":  day_type,
            "note":      note,
        })

    # Tính summary
    nghi_phep_count = len([d for d in nghi_phep_days if d in work_days_set])
    nghi_le_count   = len([d for d in nghi_le_days   if d in work_days_set])
    wfh_count       = len([d for d in wfh_days       if d in work_days_set])
    cong_tac_count  = len([d for d in cong_tac_days  if d in work_days_set])
    nghi_kl_count   = len(nghi_kl_days)

    tong_cong_huong_luong = (
        STD_CONG
        - nghi_kl_count
        + nghi_phep_count
        + nghi_le_count
        + wfh_count
        + cong_tac_count
    )

    tru_sm = (
        len(late_gt240_days)  * 1.0
        + len(late_60_240_days) * 0.5
        + round(late_lt60_pool_minutes / 480, 4)
    )
    cong_tinh_luong = round(tong_cong_huong_luong - tru_sm, 4)

    return {
        "summary": {
            "so_cong_chuan":         STD_CONG,
            "nghi_phep":             nghi_phep_count,
            "nghi_le":               nghi_le_count,
            "wfh":                   wfh_count,
            "cong_tac":              cong_tac_count,
            "nghi_khong_luong":      nghi_kl_count,
            "tong_cong_thuc_te":     tong_cong_thuc_te,
            "tong_cong_huong_luong": tong_cong_huong_luong,
            "late_gt240_count":      len(late_gt240_days),
            "late_60_240_count":     len(late_60_240_days),
            "late_lt60_minutes":     late_lt60_pool_minutes,
            "tru_sm":                round(tru_sm, 4),
            "cong_tinh_luong":       cong_tinh_luong,
        },
        "daily_detail": daily_detail,
    }
def _recalculate_dependent_formulas(summary: dict, overrides_keys: set[str]) -> None:
    """
    Tính lại tất cả công thức phụ thuộc khi agent gửi data_overrides.
    
    Logic:
      - Nếu agent sửa dm_gt_4h/dm_1h_4h/phut_muon_lt_1h
        → tính lại tru_sm (trừ sớm về muộn)
      
      - Nếu agent sửa nghi_phep/nghi_le/wfh/cong_tac/nghi_khong_luong
        → tính lại tong_cong_huong_luong
      
      - Nếu có thay đổi trong các fields trên hoặc tru_sm
        → tính lại cong_tinh_luong
    
    Args:
        summary: dict summary của 1 nhân viên (direct reference, sửa là sửa gốc)
        overrides_keys: set các field mà agent gửi để sửa
    """
    
    # Nhóm fields phụ thuộc
    LATE_FIELDS = {'dm_gt_4h', 'dm_1h_4h', 'phut_muon_lt_1h'}
    LEAVE_FIELDS = {'nghi_phep', 'nghi_le', 'wfh', 'cong_tac', 'nghi_khong_luong', 'so_cong_chuan'}
    
    # Step 1: Tính lại tru_sm nếu late fields bị sửa
    if LATE_FIELDS & overrides_keys and 'tru_sm' not in overrides_keys:
        dm_gt_240 = int(summary.get('dm_gt_4h', 0))
        dm_60_240 = int(summary.get('dm_1h_4h', 0))
        phut_lt60 = float(summary.get('phut_muon_lt_1h', 0))
        
        new_tru_sm = (
            dm_gt_240 * 1.0 +
            dm_60_240 * 0.5 +
            round(phut_lt60 / 480, 4)
        )
        summary['tru_sm'] = round(new_tru_sm, 4)
    
    # Step 2: Tính lại tong_cong_huong_luong nếu leave fields bị sửa
    if LEAVE_FIELDS & overrides_keys:
        so_cong_chuan = float(summary.get('so_cong_chuan', 26))
        nghi_kl = float(summary.get('nghi_khong_luong', 0))
        nghi_phep = float(summary.get('nghi_phep', 0))
        nghi_le = float(summary.get('nghi_le', 0))
        wfh = float(summary.get('wfh', 0))
        cong_tac = float(summary.get('cong_tac', 0))
        
        # Công hưởng lương = 26 - Nghỉ KL + (các loại nghỉ được hưởng lương)
        new_cong_huong = so_cong_chuan - nghi_kl + nghi_phep + nghi_le + wfh + cong_tac
        summary['tong_cong_huong_luong'] = new_cong_huong
    
    # Step 3: Tính lại cong_tinh_luong (LUÔN, vì có sự thay đổi ở trên)
    cong_huong = float(summary.get('tong_cong_huong_luong', 26))
    tru_sm = float(summary.get('tru_sm', 0))
    new_cong_tinh_luong = cong_huong - tru_sm
    summary['cong_tinh_luong'] = round(new_cong_tinh_luong, 4)
# ─────────────────────────────────────────────────────────────
# TOOLS
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_attendance_data(
    session_id:   str,
    year_month:   str,          # Tháng THỰC TẾ YYYY-MM
    filter_type:  str = "all",  # "all" | "username" | "don_vi"
    filter_value: str = "",
    company_code: str = "HITC",
) -> str:
    """
    Tính toán và trả về dữ liệu bảng chấm công đầy đủ dạng JSON.
    Dùng khi LLM cần xử lý/phân tích thêm trước khi xuất Excel hoặc trả lời.

    year_month: tháng THỰC TẾ (vd "2026-02") → kỳ 26/01→25/02.
    filter_type: "all" | "username" | "don_vi".
    filter_value: username hoặc tên/mã đơn vị.

    Trả về JSON gồm:
      - period_info: thông tin kỳ chấm công
      - employees: [{ma_nv, ho_va_ten, don_vi, vi_tri, summary{...}, daily_detail[...]}]
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    try:
        dt = datetime.strptime(year_month, "%Y-%m")
    except ValueError:
        return json.dumps({"error": "year_month phải có định dạng YYYY-MM"}, ensure_ascii=False)

    year, month = dt.year, dt.month
    period_start, period_end = _get_period_dates(year, month)

    # Load shared data once
    holidays      = _get_holidays_in_period(period_start, period_end, company_code)
    holiday_names = _get_holiday_names(period_start, period_end, company_code)
    off_weekdays  = _get_off_weekdays(company_code)
    employees     = _get_employees(filter_type, filter_value, company_code)

    if not employees:
        return json.dumps({
            "error": f"Không tìm thấy nhân viên (filter_type={filter_type}, filter_value={filter_value})",
        }, ensure_ascii=False)

    results = []
    for emp in employees:
        try:
            calc = _calc_one_employee(
                emp["username"], period_start, period_end,
                off_weekdays, holidays, holiday_names, company_code,
            )
            results.append({
                "username":    emp["username"],
                "ma_nv":       emp["ma_nv"],
                "ho_va_ten":   emp["ho_va_ten"],
                "don_vi":      emp["don_vi"],
                "vi_tri":      emp["vi_tri"],
                "summary":     calc["summary"],
                "daily_detail": calc["daily_detail"],
            })
        except Exception as e:
            logger.warning("Calc error for %s: %s", emp["username"], e, exc_info=True)
            results.append({**emp, "error": str(e), "summary": {}, "daily_detail": []})

    ky_str = f"26/{period_start.month:02d}/{period_start.year} - 25/{period_end.month:02d}/{period_end.year}"

    return json.dumps({
        "year_month":    year_month,
        "period_info": {
            "ky_cham_cong":  ky_str,
            "period_start":  str(period_start),
            "period_end":    str(period_end),
            "total_days":    (period_end - period_start).days + 1,
            "holidays":      sorted(holidays),
            "holiday_names": holiday_names,
            "off_weekdays":  sorted(off_weekdays),
        },
        "so_nhan_vien": len(results),
        "employees":    results,
    }, ensure_ascii=False, default=str)


@mcp.tool()
def export_attendance_excel(
    session_id:        str,
    year_month:        str,
    filter_type:       str = "all",
    filter_value:      str = "",
    company_code:      str = "HITC",
    output_path:       str = "",
    extra_columns:     str = "",  # JSON string: [{"key": "field_name", "label": "Tên cột"}]
    custom_formula_notes: str = "", # Ghi chú công thức tùy chỉnh từ user/LLM
    data_overrides:    str = "",
) -> str:
    """
    Xuất file Excel bảng chấm công tổng hợp với 2 sheets:
      Sheet 1 "Tổng hợp": các cột tổng hợp + công tính lương
      Sheet 2 "Chi tiết ngày": mỗi ngày trong kỳ là 1 cột, hiển thị check-in/out

    extra_columns: JSON array các cột bổ sung, vd:
      '[{"key":"tong_gio_lam","label":"Tổng giờ làm"}]'

    custom_formula_notes: LLM có thể truyền ghi chú về công thức điều chỉnh.
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    # Lấy data
    raw = json.loads(get_attendance_data(
        session_id=session_id, year_month=year_month,
        filter_type=filter_type, filter_value=filter_value,
        company_code=company_code,
    ))
    if "error" in raw:
        return json.dumps(raw, ensure_ascii=False)

    employees   = raw["employees"]

    if data_overrides:
        try:
            overrides = json.loads(data_overrides) if data_overrides else {}
 
            for emp in employees:
                if "summary" not in emp:
                    emp["summary"] = {}
                summary = emp["summary"]
                mnv = emp.get("ma_nv", "")
                
                emp_override = overrides.get(mnv)
                if emp_override and isinstance(emp_override, dict):
                    # 1️⃣ GHI ĐÈ TRỰC TIẾP TẤT CẢ FIELDS MÀ AGENT GỬI
                    for field, val in emp_override.items():
                        try:
                            if isinstance(val, str) and val.replace('.','',1).isdigit():
                                summary[field] = float(val)
                            else:
                                summary[field] = val
                        except:
                            summary[field] = val
                    
                    # 2️⃣ TÍNH LẠI CÁC CÔNG THỨC PHỤ THUỘC TỰ ĐỘNG
                    _recalculate_dependent_formulas(summary, set(emp_override.keys()))
                    
                    logger.info(
                        "Applied overrides for %s: %s → cong_tinh_luong=%.4f",
                        mnv, list(emp_override.keys()), summary.get('cong_tinh_luong', 0)
                    )
                    
        except Exception as e:
            logger.error(f"Dynamic override error: {e}", exc_info=True)
            
    period_info = raw["period_info"]
    ky_str      = period_info["ky_cham_cong"]
    period_start = date.fromisoformat(period_info["period_start"])
    period_end   = date.fromisoformat(period_info["period_end"])
    holidays     = set(period_info["holidays"])
    off_wdays    = set(period_info["off_weekdays"])  # 0-6

    # Parse extra columns
    extra_cols = {}
    if extra_columns:
        try:
            parsed_extra = json.loads(extra_columns)
            if isinstance(parsed_extra, list):
                # Nếu LLM gửi List, tự động convert sang Dict
                extra_cols = {col_name: "" for col_name in parsed_extra}
            elif isinstance(parsed_extra, dict):
                extra_cols = parsed_extra
        except Exception as e:
            logger.error(f"Lỗi parse extra_columns: {e}")

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
        from openpyxl.styles.fills import PatternFill
    except ImportError:
        return json.dumps({"error": "Thiếu openpyxl. Cài: pip install openpyxl"}, ensure_ascii=False)

    wb = Workbook()

    # ─────────────────────────────────────────────────────────
    # STYLES
    # ─────────────────────────────────────────────────────────
    def _fill(hex_color: str) -> PatternFill:
        return PatternFill("solid", start_color=hex_color)

    def _font(bold=False, color="000000", size=9) -> Font:
        return Font(bold=bold, color=color, name="Arial", size=size)

    def _border() -> Border:
        s = Side(style="thin", color="BFBFBF")
        return Border(left=s, right=s, top=s, bottom=s)

    C_HDR1  = "1F4E79"   # Dark blue
    C_HDR2  = "2E75B6"   # Medium blue
    C_WE    = "D6E4F0"   # Weekend - light blue
    C_HOL   = "FCE4D6"   # Holiday - light orange
    C_NP    = "E2EFDA"   # Nghỉ phép - light green
    C_WFH   = "FFF2CC"   # WFH - yellow
    C_CT    = "EAD1DC"   # Công tác - light pink
    C_NKL   = "F4CCCC"   # Nghỉ KL - light red
    C_TOTAL = "FFFDE7"   # Total row
    C_DL    = "DEEBF7"   # Đi làm nhẹ

    ca  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    la  = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    ra  = Alignment(horizontal="right",  vertical="center")
    bd  = _border()

    # ─────────────────────────────────────────────────────────
    # SHEET 1: TỔNG HỢP
    # ─────────────────────────────────────────────────────────
    ws1 = wb.active
    ws1.title = "Tổng hợp"

    # Title
    base_cols = 18
    total_cols = base_cols + len(extra_cols)
    end_col_letter = get_column_letter(total_cols)
    ws1.merge_cells(f"A1:{end_col_letter}1")
    ws1["A1"] = f"BẢNG CHẤM CÔNG TỔNG HỢP — {company_code}"
    ws1["A1"].font      = _font(bold=True, color="1F4E79", size=14)
    ws1["A1"].alignment = ca

    ws1.merge_cells(f"A2:{end_col_letter}2")
    ws1["A2"] = f"Kỳ chấm công: {ky_str}  |  Xuất ngày: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    ws1["A2"].font      = _font(size=10)
    ws1["A2"].alignment = ca

    if custom_formula_notes:
        ws1.merge_cells(f"A3:{end_col_letter}3")
        ws1["A3"] = f"Ghi chú: {custom_formula_notes}"
        ws1["A3"].font      = _font(size=9, color="FF0000")
        ws1["A3"].alignment = la
        hdr_row = 5
    else:
        hdr_row = 4

    # Fixed headers
    fixed_headers = [
        ("STT",              5),
        ("Mã NV",            9),
        ("Họ và tên",        22),
        ("Đơn vị công tác",  22),
        ("Vị trí công việc", 20),
        ("Công chuẩn",       9),
        ("Nghỉ phép",        9),
        ("Nghỉ lễ",          7),
        ("WFH",              7),
        ("Công tác",         8),
        ("Nghỉ KL",          8),
        ("Công TT",          8),
        ("Công HLương",      10),
        ("ĐM >4h\n(ngày)",   9),
        ("ĐM 1-4h\n(ngày)",  9),
        ("Phút muộn\n<1h",   9),
        ("Trừ SM",           8),
        ("Công tính lương",  13),
    ]
    all_headers = fixed_headers + [(col_name, 12) for col_name in extra_cols.keys()]

    for ci, (lbl, wid) in enumerate(all_headers, 1):
        cell = ws1.cell(row=hdr_row, column=ci, value=lbl)
        cell.font      = _font(bold=True, color="FFFFFF", size=10)
        cell.fill      = _fill(C_HDR1)
        cell.alignment = ca
        cell.border    = bd
        ws1.column_dimensions[get_column_letter(ci)].width = wid
    ws1.row_dimensions[hdr_row].height = 40

    # Summary key mapping
    FIELD_MAP = [
        None,               # STT
        "ma_nv",
        "ho_va_ten",
        "don_vi",
        "vi_tri",
        "so_cong_chuan",
        "nghi_phep",
        "nghi_le",
        "wfh",
        "cong_tac",
        "nghi_khong_luong",
        "tong_cong_thuc_te",
        "tong_cong_huong_luong",
        "late_gt240_count",
        "late_60_240_count",
        "late_lt60_minutes",
        "tru_sm",
        "cong_tinh_luong",
    ]

    totals = {k: 0.0 for k in [
        "nghi_phep","nghi_le","wfh","cong_tac","nghi_khong_luong",
        "tong_cong_thuc_te","tong_cong_huong_luong","late_gt240_count",
        "late_60_240_count","late_lt60_minutes","tru_sm","cong_tinh_luong",
    ]}

    for idx, emp in enumerate(employees, 1):
        r = hdr_row + idx
        s = emp.get("summary", {})

        row_fill = _fill("EBF3FB") if idx % 2 == 0 else None

        values = [idx]
        for field in FIELD_MAP[1:]:
            if field in ("ma_nv","ho_va_ten","don_vi","vi_tri"):
                values.append(emp.get(field, ""))
            else:
                values.append(s.get(field, ""))

        # Extra columns
        for col_name in extra_cols.keys():
            # Lấy giá trị từ summary, nếu không có thì lấy giá trị mặc định LLM đưa ra
            values.append(s.get(col_name, extra_cols[col_name]))
        for ci, val in enumerate(values, 1):
            cell = ws1.cell(row=r, column=ci, value=val)
            cell.font   = _font()
            cell.border = bd
            if row_fill:
                cell.fill = row_fill
            if ci in (1, 6, 7, 8, 9, 10, 11, 14, 15, 16):
                cell.alignment = ca
                cell.number_format = "0"
            elif ci in (12, 13, 17, 18):
                cell.alignment = ra
                cell.number_format = "0.00"
            else:
                cell.alignment = la

        # Tô màu cột Công tính lương
        cong_val = s.get("cong_tinh_luong", 0)
        ws1.cell(row=r, column=18).fill = (
            _fill("C6EFCE") if cong_val >= STD_CONG else
            _fill("FFEB9C") if cong_val >= STD_CONG * 0.8 else
            _fill("FFC7CE")
        )

        for k in totals:
            totals[k] += s.get(k, 0)

    # Total row
    total_row = hdr_row + len(employees) + 1
    ws1.merge_cells(f"A{total_row}:E{total_row}")
    tc = ws1.cell(row=total_row, column=1, value="TỔNG CỘNG")
    tc.font      = _font(bold=True)
    tc.fill      = _fill(C_TOTAL)
    tc.alignment = ca
    tc.border    = bd

    total_values = [None] * 5 + [
        "",
        totals["nghi_phep"], totals["nghi_le"], totals["wfh"], totals["cong_tac"],
        totals["nghi_khong_luong"], round(totals["tong_cong_thuc_te"], 2),
        round(totals["tong_cong_huong_luong"], 2),
        int(totals["late_gt240_count"]), int(totals["late_60_240_count"]),
        int(totals["late_lt60_minutes"]),
        round(totals["tru_sm"], 4), round(totals["cong_tinh_luong"], 2),
    ]
    for ci, val in enumerate(total_values, 1):
        if val is None:
            continue
        cell = ws1.cell(row=total_row, column=ci, value=val)
        cell.font      = _font(bold=True)
        cell.fill      = _fill(C_TOTAL)
        cell.alignment = ca
        cell.border    = bd

    ws1.freeze_panes = f"F{hdr_row + 1}"

    # ─────────────────────────────────────────────────────────
    # SHEET 2: CHI TIẾT NGÀY
    # ─────────────────────────────────────────────────────────
    ws2 = wb.create_sheet("Chi tiết ngày")

    # Build danh sách ngày trong kỳ
    all_days_in_period = _get_all_days_in_period(period_start, period_end)
    n_days = len(all_days_in_period)

    # Title
    total_detail_cols = 5 + n_days
    end_col2 = get_column_letter(total_detail_cols)
    ws2.merge_cells(f"A1:{end_col2}1")
    ws2["A1"] = f"CHI TIẾT CHẤM CÔNG THEO NGÀY — Kỳ: {ky_str}"
    ws2["A1"].font      = _font(bold=True, color="1F4E79", size=13)
    ws2["A1"].alignment = ca

    # Header row 1: cột cố định + header ngày
    FIXED_HDR = [
        ("STT",            5),
        ("Mã NV",          9),
        ("Họ và tên",      22),
        ("Đơn vị",         18),
        ("Vị trí",         18),
    ]
    for ci, (lbl, wid) in enumerate(FIXED_HDR, 1):
        cell = ws2.cell(row=3, column=ci, value=lbl)
        cell.font      = _font(bold=True, color="FFFFFF", size=9)
        cell.fill      = _fill(C_HDR1)
        cell.alignment = ca
        cell.border    = bd
        ws2.column_dimensions[get_column_letter(ci)].width = wid

    # Header ngày: row 3 = Thứ, row 4 = Ngày/Tháng
    for di, d in enumerate(all_days_in_period):
        col = 6 + di
        ds  = d.strftime("%Y-%m-%d")
        wday = WEEKDAY_VI[d.weekday()]
        is_weekend = d.weekday() in off_wdays
        is_hol     = ds in holidays

        fill_color = C_WE if is_weekend else (C_HOL if is_hol else C_HDR2)
        txt_color  = "666666" if is_weekend else "FFFFFF"

        # Row 3: Thứ
        c3 = ws2.cell(row=3, column=col, value=wday)
        c3.font      = _font(bold=True, color=txt_color, size=8)
        c3.fill      = _fill(fill_color)
        c3.alignment = ca
        c3.border    = bd

        # Row 4: Ngày
        day_label = f"{d.day:02d}/{d.month:02d}" if d.month != period_start.month else f"{d.day:02d}\n/{period_start.month:02d}"
        c4 = ws2.cell(row=4, column=col, value=f"{d.day:02d}/{d.month:02d}")
        c4.font      = _font(bold=True, color=txt_color, size=8)
        c4.fill      = _fill(fill_color)
        c4.alignment = ca
        c4.border    = bd

        ws2.column_dimensions[get_column_letter(col)].width = 9

    ws2.row_dimensions[3].height = 20
    ws2.row_dimensions[4].height = 20

    # Data rows — mỗi NV 2 rows: check-in và check-out
    row_ptr = 5
    for idx, emp in enumerate(employees, 1):
        detail_map = {dd["date"]: dd for dd in emp.get("daily_detail", [])}

        # Row A: check-in
        # Row B: check-out  (merge STT/Mã/Tên/Đơn vị/Vị trí)
        r_in  = row_ptr
        r_out = row_ptr + 1

        # Merge cột cố định
        for ci in range(1, 6):
            ws2.merge_cells(start_row=r_in, start_column=ci, end_row=r_out, end_column=ci)

        ws2.cell(row=r_in, column=1, value=idx).alignment = ca
        ws2.cell(row=r_in, column=2, value=emp["ma_nv"]).alignment = ca
        ws2.cell(row=r_in, column=3, value=emp["ho_va_ten"]).alignment = la
        ws2.cell(row=r_in, column=4, value=emp["don_vi"]).alignment   = la
        ws2.cell(row=r_in, column=5, value=emp["vi_tri"]).alignment   = la

        for ci in range(1, 6):
            for r in (r_in, r_out):
                cell = ws2.cell(row=r, column=ci)
                cell.font   = _font(size=8)
                cell.border = bd

        # Label cột check-in/out
        label_in  = ws2.cell(row=r_in,  column=1)  # "Vào"
        label_out = ws2.cell(row=r_out, column=1)  # "Ra"

        # Điền từng ngày
        for di, d in enumerate(all_days_in_period):
            ds   = d.strftime("%Y-%m-%d")
            col  = 6 + di
            dd   = detail_map.get(ds, {})
            dtype = dd.get("day_type", "")
            ci_   = dd.get("check_in",  "-:-")
            co_   = dd.get("check_out", "-:-")
            note  = dd.get("note", "")

            is_weekend = d.weekday() in off_wdays
            is_hol     = ds in holidays

            # Màu nền theo loại ngày
            if is_weekend:
                fc = C_WE
            elif is_hol:
                fc = C_HOL
            elif dtype == "Nghỉ phép/ốm":
                fc = C_NP
            elif dtype == "WFH":
                fc = C_WFH
            elif dtype == "Công tác":
                fc = C_CT
            elif dtype == "Nghỉ không lương":
                fc = C_NKL
            elif dtype == "Đi làm":
                fc = C_DL if not note else "FFF2CC"
            else:
                fc = "FFFFFF"

            # Hiển thị check-in
            display_in = ci_ if ci_ != "-:-" else ""
            if dtype in ("Nghỉ phép/ốm", "WFH", "Công tác"):
                display_in = dtype[:5]  # "Nghỉ", "WFH", "CT"
            elif is_hol:
                display_in = "Lễ"
            elif is_weekend:
                display_in = ""

            cell_in = ws2.cell(row=r_in, column=col, value=display_in)
            cell_in.font      = _font(size=8)
            cell_in.fill      = _fill(fc)
            cell_in.alignment = ca
            cell_in.border    = bd

            # Hiển thị check-out
            display_out = co_ if co_ != "-:-" else ""
            if dtype in ("Nghỉ phép/ốm", "WFH", "Công tác"):
                display_out = ""
            elif is_hol or is_weekend:
                display_out = ""

            if note and dtype == "Đi làm":
                display_out = co_ if co_ != "-:-" else note[:10]

            cell_out = ws2.cell(row=r_out, column=col, value=display_out)
            cell_out.font      = _font(size=8)
            cell_out.fill      = _fill(fc)
            cell_out.alignment = ca
            cell_out.border    = bd

        ws2.row_dimensions[r_in].height  = 14
        ws2.row_dimensions[r_out].height = 14
        row_ptr += 2

    ws2.freeze_panes = "F5"

    # ─────────────────────────────────────────────────────────
    # LEGEND SHEET
    # ─────────────────────────────────────────────────────────
    ws3 = wb.create_sheet("Chú giải")
    ws3["A1"] = "CHÚ GIẢI MÀU SẮC VÀ KÝ HIỆU"
    ws3["A1"].font = _font(bold=True, size=11)

    legend = [
        (C_WE,  "Ngày nghỉ tuần (T7/CN)"),
        (C_HOL, "Ngày nghỉ lễ"),
        (C_NP,  "Nghỉ phép/ốm (đơn đã duyệt)"),
        (C_WFH, "Làm việc từ xa - WFH (đơn đã duyệt)"),
        (C_CT,  "Đi công tác (đơn đã duyệt)"),
        (C_NKL, "Nghỉ không lương (thiếu dữ liệu CC)"),
        (C_DL,  "Đi làm bình thường"),
        ("FFF2CC", "Đi muộn/về sớm"),
        ("C6EFCE", "Công tính lương ≥ 26"),
        ("FFEB9C", "Công tính lương ≥ 20"),
        ("FFC7CE", "Công tính lương < 20"),
    ]
    for ri, (color, desc) in enumerate(legend, 3):
        ws3.cell(row=ri, column=1, value="  ").fill = _fill(color)
        ws3.cell(row=ri, column=2, value=desc).font = _font(size=9)

    ws3["A15"] = "CÔNG THỨC TÍNH"
    ws3["A15"].font = _font(bold=True)
    formulas = [
        ("Công hưởng lương",  "= 26 - Nghỉ KL + Nghỉ phép + Nghỉ lễ + WFH + Công tác"),
        ("Trừ SM",            "= (Ngày ĐM>4h × 1) + (Ngày ĐM 1-4h × 0.5) + (Tổng phút <1h / 480)"),
        ("Công tính lương",   "= Công hưởng lương - Trừ SM"),
        ("Nghỉ KL",           "Ngày không có dữ liệu CC hoặc làm < 2 tiếng"),
        ("ĐM = đi muộn",      "Phút check-in sau 08:30 + phút check-out trước 17:30"),
    ]
    for ri, (k, v) in enumerate(formulas, 16):
        ws3.cell(row=ri, column=1, value=k).font   = _font(bold=True, size=9)
        ws3.cell(row=ri, column=2, value=v).font   = _font(size=9)

    ws3.column_dimensions["A"].width = 22
    ws3.column_dimensions["B"].width = 60

    # ─────────────────────────────────────────────────────────
    # SAVE
    # ─────────────────────────────────────────────────────────
    if not output_path:
        safe = re.sub(r"[^\w]", "_", filter_value or "all")
        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/tmp/bang_cham_cong_{year_month}_{now_str}_{safe}.xlsx"

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    wb.save(output_path)
    logger.info("Excel saved: %s (%d employees)", output_path, len(employees))

    return json.dumps({
        "status":       "success",
        "file_path":    output_path,
        "year_month":   year_month,
        "ky_cham_cong": ky_str,
        "so_nhan_vien": len(employees),
        "sheets":       ["Tổng hợp", "Chi tiết ngày", "Chú giải"],
        "message":      f"Đã xuất {len(employees)} NV, kỳ {ky_str}, tại: {output_path}",
    }, ensure_ascii=False)


@mcp.tool()
def compute_and_export(
    session_id:   str,
    year_month:   str,
    filter_type:  str = "all",
    filter_value: str = "",
    company_code: str = "HITC",
    output_path:  str = "",
    extra_columns: str = "",
    custom_formula_notes: str = "",
    data_overrides: str = "",
) -> str:
    """
    All-in-one: tính toán bảng chấm công và xuất Excel ngay.
    Dùng khi LLM muốn thực hiện 1 bước duy nhất.

    Trả về: đường dẫn file Excel + tóm tắt số liệu.
    """
    return export_attendance_excel(
        session_id=session_id,
        year_month=year_month,
        filter_type=filter_type,
        filter_value=filter_value,
        company_code=company_code,
        output_path=output_path,
        extra_columns=extra_columns,
        custom_formula_notes=custom_formula_notes,
        data_overrides=data_overrides,
    )


@mcp.tool()
def send_attendance_report(
    session_id:       str,
    year_month:       str,
    filter_type:      str = "all",
    filter_value:     str = "",
    to_emails:        list[str] = None,
    send_to_don_vi:   str = "",
    subject:          str = "",
    body:             str = "",
    company_code:     str = "HITC",
    extra_columns:    str = "",
    custom_formula_notes: str = "",
    data_overrides:   str = "",
) -> str:
    """
    Xuất bảng chấm công Excel và gửi email đính kèm.
    to_emails: list email hoặc username nội bộ.
    send_to_don_vi: gửi cho toàn bộ nhân viên phòng ban này.
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    if not to_emails and not send_to_don_vi:
        return json.dumps({"error": "Cần truyền to_emails hoặc send_to_don_vi."}, ensure_ascii=False)

    # 1. Xuất Excel
    export_res = json.loads(export_attendance_excel(
        session_id=session_id, year_month=year_month,
        filter_type=filter_type, filter_value=filter_value,
        company_code=company_code,
        extra_columns=extra_columns,
        custom_formula_notes=custom_formula_notes,
        data_overrides=data_overrides,
    ))
    if export_res.get("status") != "success":
        return json.dumps(export_res, ensure_ascii=False)

    file_path = export_res["file_path"]
    ky_str    = export_res["ky_cham_cong"]
    so_nv     = export_res["so_nhan_vien"]

    if not subject:
        subject = f"[{company_code}] Bảng chấm công tổng hợp — Kỳ {ky_str}"
    if not body:
        body = (
            f"Kính gửi,\n\n"
            f"Đính kèm bảng chấm công tổng hợp kỳ {ky_str} ({so_nv} nhân viên).\n"
            f"File gồm 3 sheets: Tổng hợp, Chi tiết ngày, Chú giải.\n\n"
            f"Mọi thắc mắc vui lòng liên hệ bộ phận Nhân sự.\n\nTrân trọng."
        )

    # 2. Resolve recipients
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        from email.utils import formataddr
        from app.core.config import settings

        db = get_db()
        recipients: list[str] = []

        for r in (to_emails or []):
            if "@" in r:
                recipients.append(r)
            else:
                nv = db["instance_data_thong_tin_nhan_vien"].find_one(
                    {"ten_dang_nhap": r, "is_deleted": {"$ne": True}}, {"email": 1}
                )
                if nv and nv.get("email"):
                    recipients.append(nv["email"])

        if send_to_don_vi:
            for nv in db["instance_data_thong_tin_nhan_vien"].find(
                {
                    "is_deleted": {"$ne": True}, "company_code": company_code,
                    "trang_thai_lao_dong.value": "Đang làm việc",
                    "$or": [
                        {"don_vi_cong_tac.value":     send_to_don_vi},
                        {"phong_ban_phu_trach.value": send_to_don_vi},
                        {"path_don_vi_cong_tac": {"$regex": send_to_don_vi, "$options": "i"}},
                    ],
                },
                {"email": 1},
            ):
                if nv.get("email"):
                    recipients.append(nv["email"])

        recipients = list(set(recipients))
        if not recipients:
            return json.dumps({
                "status":    "exported_only",
                "file_path": file_path,
                "message":   f"Xuất Excel thành công nhưng không tìm thấy email để gửi.",
            }, ensure_ascii=False)

        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"]    = formataddr(("MODATA AI System", settings.MAIL_FROM))
        msg["To"]      = ", ".join(recipients[:50])
        msg.attach(MIMEText(body, "plain", "utf-8"))

        with open(file_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(file_path)}"')
        msg.attach(part)

        smtp_cls = smtplib.SMTP_SSL if settings.MAIL_PORT == 465 else smtplib.SMTP
        with smtp_cls(settings.MAIL_HOST, settings.MAIL_PORT, timeout=30) as smtp:
            if settings.MAIL_PORT != 465 and settings.MAIL_USE_TLS:
                smtp.starttls()
            if settings.MAIL_USERNAME and settings.MAIL_PASSWORD:
                smtp.login(settings.MAIL_USERNAME, settings.MAIL_PASSWORD)
            smtp.sendmail(settings.MAIL_FROM, recipients, msg.as_string())

        return json.dumps({
            "status":       "sent",
            "file_path":    file_path,
            "recipients":   recipients,
            "so_nhan_vien": so_nv,
            "message":      f"Đã gửi bảng chấm công đến {len(recipients)} người.",
        }, ensure_ascii=False)

    except Exception as e:
        logger.error("send_attendance_report error: %s", e, exc_info=True)
        return json.dumps({
            "status":    "exported_only",
            "file_path": file_path,
            "message":   f"Xuất Excel OK nhưng gửi mail thất bại: {e}",
        }, ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8020)