"""
mcp_servers/attendance_analytics_server.py
MCP Attendance Analytics Server — tổng hợp bảng chấm công, xuất Excel, gửi mail.

Tools (prefix sau mount: att_ana_):
  compute_attendance_report   — tính toán bảng chấm công tổng hợp cho NV/phòng ban/công ty
  export_attendance_excel     — xuất file Excel bảng chấm công
  send_attendance_report      — gửi mail kèm file Excel đến NV / phòng ban
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import tempfile
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
WORK_START = (8, 30)   # 08:30
WORK_END   = (17, 30)  # 17:30
WORK_MINUTES = (17 * 60 + 30) - (8 * 60 + 30)  # 540 phút = 9h
STD_CONG = 26


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _session_ok(session_id: str) -> bool:
    ctx = get_session_context(session_id)
    return bool(ctx.accessible_instance_names())


def _ev(v: Any) -> Any:
    """Extract value from MongoDB lookup field."""
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
    if dt is None:
        return None
    if hasattr(dt, "strftime"):
        return dt + timedelta(hours=7)
    return None


def _parse_time_str(s: str) -> tuple[int, int] | None:
    """Parse 'HH:MM' → (hour, minute). Returns None nếu invalid."""
    if not s or s.strip() in ("-:-", "", "-"):
        return None
    s = s.strip()
    m = re.match(r"(\d{1,2}):(\d{2})", s)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return None


def _minutes_late(check_in: tuple[int, int] | None) -> int:
    """Số phút đi muộn so với 08:30."""
    if check_in is None:
        return 0
    ci = check_in[0] * 60 + check_in[1]
    std = WORK_START[0] * 60 + WORK_START[1]
    return max(0, ci - std)


def _minutes_early(check_out: tuple[int, int] | None) -> int:
    """Số phút về sớm so với 17:30."""
    if check_out is None:
        return 0
    co = check_out[0] * 60 + check_out[1]
    std = WORK_END[0] * 60 + WORK_END[1]
    return max(0, std - co)


def _parse_firstin_lastout(doc: dict) -> tuple[tuple | None, tuple | None]:
    """Extract firstIn và lastOut từ MongoDB doc → (HH, MM) tuples (giờ VN)."""
    def _to_hm(dt_field):
        if dt_field is None:
            return None
        vn = _utc_to_vn(dt_field)
        if vn is None:
            return None
        return (vn.hour, vn.minute)

    return _to_hm(doc.get("firstIn")), _to_hm(doc.get("lastOut"))


def _get_holidays_in_month(year: int, month: int, company_code: str) -> set[str]:
    """Lấy tập hợp ngày nghỉ lễ trong tháng (dạng YYYY-MM-DD)."""
    db = get_db()
    # Kỳ chấm công tháng M thực tế là từ 26/M-1 đến 25/M
    # Nhưng holidays tra theo tháng thực tế
    month_start = datetime(year, month, 1)
    month_end   = datetime(year, month, 28) + timedelta(days=4)
    month_end   = month_end.replace(day=1) - timedelta(days=1)
    month_end   = datetime(year, month, month_end.day, 23, 59, 59)

    holidays = set()
    docs = list(db["instance_data_ngay_nghi_le"].find({
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "tu_ngay":  {"$lte": month_end},
        "den_ngay": {"$gte": month_start - timedelta(hours=7)},
    }))
    for doc in docs:
        tu_ngay  = _utc_to_vn(doc.get("tu_ngay"))
        den_ngay = _utc_to_vn(doc.get("den_ngay"))
        if tu_ngay and den_ngay:
            cur = tu_ngay.date()
            end = den_ngay.date()
            while cur <= end:
                holidays.add(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)
    return holidays


def _get_off_days_of_week(company_code: str) -> set[int]:
    """Lấy weekday indices của ngày nghỉ tuần (0=Mon, 6=Sun)."""
    db = get_db()
    docs = list(db["instance_data_ngay_nghi_tuan"].find({
        "is_deleted":   {"$ne": True},
        "is_active":    {"$ne": False},
        "company_code": company_code,
    }))
    off_days = set()
    _MAP = {
        "thứ 2": 0, "thứ hai": 0, "t2": 0,
        "thứ 3": 1, "thứ ba": 1,  "t3": 1,
        "thứ 4": 2, "thứ tư": 2,  "t4": 2,
        "thứ 5": 3, "thứ năm": 3, "t5": 3,
        "thứ 6": 4, "thứ sáu": 4, "t6": 4,
        "thứ 7": 5, "thứ bảy": 5, "t7": 5,
        "chủ nhật": 6, "cn": 6,
    }
    for doc in docs:
        name = _ev(doc.get("loai_nghi_tuan")) or ""
        key = str(name).lower().strip()
        if key in _MAP:
            off_days.add(_MAP[key])
    return off_days


def _get_approved_leaves(username: str, year: int, month: int,
                          company_code: str) -> dict[str, list[str]]:
    """
    Lấy đơn từ đã duyệt của NV trong tháng.
    Returns: {loai_don: [YYYY-MM-DD, ...]}
    Loại: "Nghỉ phép", "Nghỉ ốm", "Làm việc từ xa", "Đề nghị đi công tác", "Đi muộn, về sớm"
    """
    db = get_db()
    # Kỳ chấm công: 26 tháng trước đến 25 tháng này
    if month == 1:
        from_dt = datetime(year - 1, 12, 26) - timedelta(hours=7)
    else:
        from_dt = datetime(year, month - 1, 26) - timedelta(hours=7)
    to_dt = datetime(year, month, 25, 23, 59, 59)

    docs = list(db["instance_data_danh_sach_quan_ly_don_xin_nghi"].find({
        "is_deleted":              {"$ne": True},
        "company_code":            company_code,
        "nguoi_nop_don.value":     username,
        "trang_thai_phe_duyet.value": "Đã duyệt",
        "ngay_nop_don": {"$gte": from_dt, "$lte": to_dt},
    }))

    result: dict[str, list[str]] = {}
    for doc in docs:
        loai = doc.get("loai_don", "")
        tu_ngay  = _utc_to_vn(doc.get("tu_ngay"))
        den_ngay = _utc_to_vn(doc.get("den_ngay"))
        if not tu_ngay:
            continue
        if not den_ngay:
            den_ngay = tu_ngay

        cur = tu_ngay.date()
        end = den_ngay.date()
        while cur <= end:
            result.setdefault(loai, [])
            result[loai].append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)

    return result


def _get_attendance_records(username: str, year: int, month: int,
                             company_code: str) -> dict[str, dict]:
    """
    Lấy dữ liệu chấm công trong kỳ (26 tháng trước → 25 tháng này).
    Returns: {YYYY-MM-DD: {"firstIn": (h,m)|None, "lastOut": (h,m)|None}}
    """
    db = get_db()
    # Kỳ chốt công = tháng trước
    if month == 1:
        ky_chot = f"{year - 1}-12"
    else:
        ky_chot = f"{year}-{month - 1:02d}"

    docs = list(db["instance_data_lich_su_cham_cong_tong_hop_cong"].find({
        "is_deleted":     {"$ne": True},
        "ten_dang_nhap":  username,
        "ngay_chot_cong": ky_chot,
    }))

    result = {}
    for doc in docs:
        day_str = doc.get("day", "")
        if not day_str:
            continue
        fi, lo = _parse_firstin_lastout(doc)
        result[day_str] = {"firstIn": fi, "lastOut": lo}
    return result


def _get_employees(filter_type: str, filter_value: str,
                   company_code: str) -> list[dict]:
    """
    Lấy danh sách nhân viên theo bộ lọc.
    filter_type: "all" | "username" | "don_vi"
    """
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

    result = []
    for d in docs:
        result.append({
            "username":    d.get("ten_dang_nhap", ""),
            "ma_nv":       d.get("ma_nhan_vien", ""),
            "ho_va_ten":   d.get("ho_va_ten_co_dau") or d.get("ho_va_ten", ""),
            "don_vi":      _ev(d.get("don_vi_cong_tac")) or "",
            "vi_tri":      _ev(d.get("vi_tri_cong_viec")) or "",
        })
    return result


# ─────────────────────────────────────────────────────────────
# CORE CALCULATOR
# ─────────────────────────────────────────────────────────────

def _calc_employee_attendance(
    username: str, year: int, month: int, company_code: str,
    holidays: set[str], off_weekdays: set[int]   # <-- THÊM 2 THAM SỐ NÀY
) -> dict:
    """
    Tính bảng chấm công cho 1 nhân viên.
    """
    if month == 1:
        ky_start = date(year - 1, 12, 26)
    else:
        ky_start = date(year, month - 1, 26)
    ky_end = date(year, month, 25)

    # XOÁ HOẶC COMMENT 2 DÒNG NÀY (Vì đã lấy từ ngoài truyền vào)
    # holidays    = _get_holidays_in_month(year, month, company_code)
    # off_weekdays = _get_off_days_of_week(company_code)
    
    leaves      = _get_approved_leaves(username, year, month, company_code)
    att_records = _get_attendance_records(username, year, month, company_code)

    # Flatten leave dates
    nghi_phep_days: set[str] = set()
    nghi_le_days:   set[str] = set()
    wfh_days:       set[str] = set()
    cong_tac_days:  set[str] = set()
    di_muon_ve_som_days: list[dict] = []  # [{date, loai_don, ...}]

    for loai, days in leaves.items():
        if loai in ("Nghỉ phép", "Nghỉ ốm"):
            nghi_phep_days.update(days)
        elif loai == "Làm việc từ xa":
            wfh_days.update(days)
        elif loai == "Đề nghị đi công tác":
            cong_tac_days.update(days)

    # Nghỉ lễ = holidays intersect kỳ làm việc (ngày thường)
    cur = ky_start
    while cur <= ky_end:
        ds = cur.strftime("%Y-%m-%d")
        if ds in holidays and cur.weekday() not in off_weekdays:
            nghi_le_days.add(ds)
        cur += timedelta(days=1)

    # Đếm ngày làm việc chuẩn trong kỳ (bỏ T7/CN và nghỉ lễ)
    work_days_in_period: list[str] = []
    cur = ky_start
    while cur <= ky_end:
        ds = cur.strftime("%Y-%m-%d")
        if cur.weekday() not in off_weekdays and ds not in holidays:
            work_days_in_period.append(ds)
        cur += timedelta(days=1)

    # Phân tích từng ngày làm việc
    tong_cong_thuc_te = 0.0
    nghi_khong_luong_days: list[str] = []
    late_gt60_count  = 0   # ngày đi muộn/về sớm > 60 phút
    late_gt240_count = 0   # ngày đi muộn/về sớm > 240 phút
    late_lt60_minutes = 0  # tổng phút đi muộn/về sớm < 60 phút

    for ds in work_days_in_period:
        # Bỏ qua ngày đã có đơn từ
        if ds in nghi_phep_days or ds in nghi_le_days:
            continue
        if ds in wfh_days or ds in cong_tac_days:
            tong_cong_thuc_te += 1
            continue

        rec = att_records.get(ds)
        if not rec:
            # Không có dữ liệu chấm công → nghỉ không lương
            nghi_khong_luong_days.append(ds)
            continue

        fi = rec["firstIn"]
        lo = rec["lastOut"]

        # Tính công thực tế
        if fi is None and lo is None:
            nghi_khong_luong_days.append(ds)
            continue

        # Tính số phút đi muộn + về sớm
        late_min  = _minutes_late(fi)
        early_min = _minutes_early(lo)
        total_deviation = late_min + early_min

        # Kiểm tra thời gian làm việc thực tế
        if fi is not None and lo is not None:
            actual_minutes = (lo[0] * 60 + lo[1]) - (fi[0] * 60 + fi[1])
        elif fi is not None:
            actual_minutes = WORK_MINUTES - late_min  # giả sử về đúng giờ
        else:
            actual_minutes = WORK_MINUTES - early_min

        # Nếu làm dưới 2 tiếng → nghỉ không lương
        if actual_minutes < 120:
            nghi_khong_luong_days.append(ds)
            continue

        tong_cong_thuc_te += 1

        # Phân loại đi muộn/về sớm
        if total_deviation > 0:
            if total_deviation >= 240:
                late_gt240_count += 1
            elif total_deviation >= 60:
                late_gt60_count += 1
            else:
                late_lt60_minutes += total_deviation

    # Tính Trừ SM
    # - Ngày đi muộn/về sớm > 240 phút: trừ 1 công
    # - Ngày đi muộn/về sớm 60-240 phút: trừ 0.5 công
    # - Tổng phút < 60 phút: tổng_phút / 480
    tru_sm = (
        late_gt240_count * 1.0
        + late_gt60_count * 0.5
        + round(late_lt60_minutes / 480, 4)
    )

    nghi_phep_count = len([d for d in nghi_phep_days if d in work_days_in_period])
    nghi_le_count   = len([d for d in nghi_le_days   if d in work_days_in_period])
    wfh_count       = len([d for d in wfh_days       if d in work_days_in_period])
    cong_tac_count  = len([d for d in cong_tac_days  if d in work_days_in_period])
    nghi_kl_count   = len(nghi_khong_luong_days)

    # Tổng công hưởng lương = Chuẩn - NKL + NP + NL + WFH + CT
    tong_cong_huong_luong = (
        STD_CONG
        - nghi_kl_count
        + nghi_phep_count
        + nghi_le_count
        + wfh_count
        + cong_tac_count
    )

    # Công tính lương = Tổng công hưởng lương - Trừ SM
    cong_tinh_luong = round(tong_cong_huong_luong - tru_sm, 4)

    return {
        "so_cong_chuan":          STD_CONG,
        "nghi_phep":              nghi_phep_count,
        "nghi_le":                nghi_le_count,
        "wfh":                    wfh_count,
        "cong_tac":               cong_tac_count,
        "nghi_khong_luong":       nghi_kl_count,
        "tong_cong_thuc_te":      round(tong_cong_thuc_te, 2),
        "tong_cong_huong_luong":  tong_cong_huong_luong,
        "late_gt240_days":        late_gt240_count,
        "late_60_240_days":       late_gt60_count,
        "late_lt60_minutes":      late_lt60_minutes,
        "tru_sm":                 round(tru_sm, 4),
        "cong_tinh_luong":        cong_tinh_luong,
        "nghi_phep_days":         sorted(nghi_phep_days),
        "nghi_le_days":           sorted(nghi_le_days),
        "wfh_days":               sorted(wfh_days),
        "cong_tac_days":          sorted(cong_tac_days),
        "nghi_khong_luong_days":  sorted(nghi_khong_luong_days),
    }


# ─────────────────────────────────────────────────────────────
# TOOLS
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def compute_attendance_report(
    session_id:   str,
    year_month:   str,          # Tháng THỰC TẾ YYYY-MM
    filter_type:  str = "all",  # "all" | "username" | "don_vi"
    filter_value: str = "",     # username hoặc mã/tên đơn vị
    company_code: str = "HITC",
) -> str:
    """
    Tính toán bảng chấm công tổng hợp.
    year_month: tháng THỰC TẾ (vd "2026-02") — kỳ = 26/01 đến 25/02.
    filter_type: "all" (cả công ty), "username" (1 NV), "don_vi" (phòng ban).
    filter_value: username hoặc mã/tên đơn vị (để trống nếu all).

    Returns JSON với danh sách nhân viên và các chỉ số chấm công.
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    try:
        dt = datetime.strptime(year_month, "%Y-%m")
        year, month = dt.year, dt.month
    except ValueError:
        return json.dumps({"error": "year_month phải có định dạng YYYY-MM"}, ensure_ascii=False)

    employees = _get_employees(filter_type, filter_value, company_code)
    if not employees:
        return json.dumps({
            "error": f"Không tìm thấy nhân viên với filter_type={filter_type}, filter_value={filter_value}",
        }, ensure_ascii=False)

    results = []
    for emp in employees:
        try:
            calc = _calc_employee_attendance(emp["username"], year, month, company_code)
            results.append({**emp, **calc})
        except Exception as e:
            logger.warning("Calc error for %s: %s", emp["username"], e)
            results.append({**emp, "error": str(e)})

    # Kỳ chấm công
    if month == 1:
        ky_str = f"26/12/{year - 1} - 25/01/{year}"
    else:
        ky_str = f"26/{month - 1:02d}/{year} - 25/{month:02d}/{year}"

    return json.dumps({
        "year_month":    year_month,
        "ky_cham_cong":  ky_str,
        "so_nhan_vien":  len(results),
        "employees":     results,
    }, ensure_ascii=False, default=str)


@mcp.tool()
def export_attendance_excel(
    session_id:   str,
    year_month:   str,          # Tháng THỰC TẾ YYYY-MM
    filter_type:  str = "all",
    filter_value: str = "",
    company_code: str = "HITC",
    output_path:  str = "",     # Để trống = tự tạo tên file
) -> str:
    """
    Xuất file Excel bảng chấm công tổng hợp.
    Trả về đường dẫn file Excel đã tạo.

    Các cột: STT, Mã NV, Họ tên, Đơn vị, Vị trí, Công chuẩn,
    Nghỉ phép, Nghỉ lễ, WFH, Công tác, Nghỉ KL, Công thực tế,
    Công hưởng lương, Đi muộn > 4h, Đi muộn 1-4h, Phút muộn < 1h, Trừ SM,
    Công tính lương.
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    try:
        dt = datetime.strptime(year_month, "%Y-%m")
        year, month = dt.year, dt.month
    except ValueError:
        return json.dumps({"error": "year_month phải có định dạng YYYY-MM"}, ensure_ascii=False)

    employees = _get_employees(filter_type, filter_value, company_code)
    if not employees:
        return json.dumps({"error": "Không tìm thấy nhân viên."}, ensure_ascii=False)

    # LẤY DATA DÙNG CHUNG Ở ĐÂY (Chỉ query 2 lần thay vì 2 * N lần)
    holidays = _get_holidays_in_month(year, month, company_code)
    off_weekdays = _get_off_days_of_week(company_code)

    rows = []
    for emp in employees:
        try:
            # TRUYỀN THÊM VÀO HÀM
            calc = _calc_employee_attendance(
                emp["username"], year, month, company_code, holidays, off_weekdays
            )
            rows.append({**emp, **calc})
        except Exception as e:
            logger.warning("Calc error for %s: %s", emp["username"], e)

    if not rows:
        return json.dumps({"error": "Không có dữ liệu để xuất."}, ensure_ascii=False)

    # Kỳ chấm công
    if month == 1:
        ky_str = f"26/12/{year - 1} - 25/01/{year}"
    else:
        ky_str = f"26/{month - 1:02d}/{year} - 25/{month:02d}/{year}"

    # Build Excel
    try:
        from openpyxl import Workbook
        from openpyxl.styles import (
            Font, PatternFill, Alignment, Border, Side, numbers
        )
        from openpyxl.utils import get_column_letter
    except ImportError:
        return json.dumps({"error": "Thiếu thư viện openpyxl. Cài: pip install openpyxl"}, ensure_ascii=False)

    wb = Workbook()
    ws = wb.active
    ws.title = f"CC Tháng {month:02d}-{year}"

    # ── Styles ────────────────────────────────────────────────
    header_fill   = PatternFill("solid", start_color="1F4E79")
    subhdr_fill   = PatternFill("solid", start_color="2E75B6")
    alt_fill      = PatternFill("solid", start_color="EBF3FB")
    total_fill    = PatternFill("solid", start_color="FFF2CC")
    header_font   = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    body_font     = Font(name="Arial", size=9)
    total_font    = Font(bold=True, name="Arial", size=9)
    center_align  = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left_align    = Alignment(horizontal="left",   vertical="center", wrap_text=True)
    right_align   = Alignment(horizontal="right",  vertical="center")

    thin = Side(style="thin", color="BFBFBF")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    # ── Title rows ────────────────────────────────────────────
    ws.merge_cells("A1:S1")
    ws["A1"] = f"BẢNG CHẤM CÔNG TỔNG HỢP — {company_code}"
    ws["A1"].font = Font(bold=True, name="Arial", size=14, color="1F4E79")
    ws["A1"].alignment = center_align

    ws.merge_cells("A2:S2")
    ws["A2"] = f"Kỳ chấm công: {ky_str}  |  Xuất ngày: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    ws["A2"].font = Font(name="Arial", size=10, italic=True)
    ws["A2"].alignment = center_align

    # ── Column headers ────────────────────────────────────────
    headers = [
        ("STT",              5),
        ("Mã NV",            9),
        ("Họ và tên",        22),
        ("Đơn vị công tác",  22),
        ("Vị trí công việc", 22),
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
        ("Phút\nmuộn <1h",   9),
        ("Trừ SM",           8),
        ("Công tính lương",  12),
        ("Ghi chú",          20),
    ]

    hdr_row = 4
    for col_idx, (hdr, width) in enumerate(headers, 1):
        cell = ws.cell(row=hdr_row, column=col_idx, value=hdr)
        cell.font      = header_font
        cell.fill      = header_fill
        cell.alignment = center_align
        cell.border    = border
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    ws.row_dimensions[hdr_row].height = 36

    # ── Data rows ─────────────────────────────────────────────
    num_fmt_2d = "0.00"
    num_fmt_4d = "0.0000"

    totals = {
        "nghi_phep": 0, "nghi_le": 0, "wfh": 0, "cong_tac": 0,
        "nghi_kl": 0, "cong_tt": 0, "cong_hl": 0,
        "dm_gt240": 0, "dm_60_240": 0, "lt60_min": 0,
        "tru_sm": 0.0, "cong_tinh": 0.0,
    }

    for idx, row in enumerate(rows, 1):
        r = hdr_row + idx
        fill = PatternFill("solid", start_color="EBF3FB") if idx % 2 == 0 else None

        values = [
            idx,
            row.get("ma_nv", ""),
            row.get("ho_va_ten", ""),
            row.get("don_vi", ""),
            row.get("vi_tri", ""),
            row.get("so_cong_chuan", STD_CONG),
            row.get("nghi_phep", 0),
            row.get("nghi_le", 0),
            row.get("wfh", 0),
            row.get("cong_tac", 0),
            row.get("nghi_khong_luong", 0),
            row.get("tong_cong_thuc_te", 0),
            row.get("tong_cong_huong_luong", 0),
            row.get("late_gt240_days", 0),
            row.get("late_60_240_days", 0),
            row.get("late_lt60_minutes", 0),
            row.get("tru_sm", 0),
            row.get("cong_tinh_luong", 0),
            row.get("error", ""),  # Ghi chú / lỗi
        ]

        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=r, column=col_idx, value=val)
            cell.font   = body_font
            cell.border = border
            if fill:
                cell.fill = fill
            if col_idx in (1, 6, 7, 8, 9, 10, 11, 14, 15, 16):
                cell.alignment = center_align
                cell.number_format = "0"
            elif col_idx in (12, 13, 17, 18):
                cell.alignment = right_align
                cell.number_format = num_fmt_2d
            elif col_idx in (2,):
                cell.alignment = center_align
            else:
                cell.alignment = left_align

        # Totals accumulation
        totals["nghi_phep"]  += row.get("nghi_phep", 0)
        totals["nghi_le"]    += row.get("nghi_le", 0)
        totals["wfh"]        += row.get("wfh", 0)
        totals["cong_tac"]   += row.get("cong_tac", 0)
        totals["nghi_kl"]    += row.get("nghi_khong_luong", 0)
        totals["cong_tt"]    += row.get("tong_cong_thuc_te", 0)
        totals["cong_hl"]    += row.get("tong_cong_huong_luong", 0)
        totals["dm_gt240"]   += row.get("late_gt240_days", 0)
        totals["dm_60_240"]  += row.get("late_60_240_days", 0)
        totals["lt60_min"]   += row.get("late_lt60_minutes", 0)
        totals["tru_sm"]     += row.get("tru_sm", 0)
        totals["cong_tinh"]  += row.get("cong_tinh_luong", 0)

    # ── Total row ─────────────────────────────────────────────
    total_row = hdr_row + len(rows) + 1
    ws.merge_cells(f"A{total_row}:E{total_row}")
    tc = ws.cell(row=total_row, column=1, value="TỔNG CỘNG")
    tc.font      = total_font
    tc.fill      = total_fill
    tc.alignment = center_align
    tc.border    = border

    total_values = [
        None, None, None, None, None,  # merged
        "",  # cong chuan
        totals["nghi_phep"], totals["nghi_le"], totals["wfh"], totals["cong_tac"],
        totals["nghi_kl"],   round(totals["cong_tt"], 2),
        round(totals["cong_hl"], 2),
        totals["dm_gt240"], totals["dm_60_240"], totals["lt60_min"],
        round(totals["tru_sm"], 4), round(totals["cong_tinh"], 2), "",
    ]
    for col_idx in range(6, 20):
        val = total_values[col_idx - 1]
        if val is None:
            continue
        cell = ws.cell(row=total_row, column=col_idx, value=val)
        cell.font      = total_font
        cell.fill      = total_fill
        cell.alignment = center_align
        cell.border    = border

    # Freeze header
    ws.freeze_panes = f"A{hdr_row + 1}"

    # ── Save ──────────────────────────────────────────────────
    if not output_path:
        safe_filter = re.sub(r"[^\w]", "_", filter_value or "all")
        output_path = f"/tmp/bang_cham_cong_{year_month}_{safe_filter}.xlsx"

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)
    wb.save(output_path)
    logger.info("Excel saved: %s", output_path)

    return json.dumps({
        "status":      "success",
        "file_path":   output_path,
        "year_month":  year_month,
        "ky_cham_cong": ky_str,
        "so_nhan_vien": len(rows),
        "message":     f"Đã xuất bảng chấm công {len(rows)} nhân viên vào: {output_path}",
    }, ensure_ascii=False)


@mcp.tool()
def send_attendance_report(
    session_id:   str,
    year_month:   str,
    filter_type:  str = "all",
    filter_value: str = "",
    to_emails:    list[str] = None,
    send_to_don_vi: str = "",
    subject:      str = "",
    body:         str = "",
    company_code: str = "HITC",
) -> str:
    """
    Xuất bảng chấm công Excel và gửi email đính kèm.

    to_emails: list email hoặc username nội bộ.
    send_to_don_vi: mã/tên đơn vị để gửi cho cả phòng ban.
    Nếu không truyền to_emails và send_to_don_vi → trả về lỗi.
    """
    if not _session_ok(session_id):
        return json.dumps({"error": "Session không hợp lệ."}, ensure_ascii=False)

    if not to_emails and not send_to_don_vi:
        return json.dumps({
            "error": "Cần truyền to_emails hoặc send_to_don_vi để gửi mail."
        }, ensure_ascii=False)

    # 1. Xuất Excel
    export_result = json.loads(export_attendance_excel(
        session_id=session_id,
        year_month=year_month,
        filter_type=filter_type,
        filter_value=filter_value,
        company_code=company_code,
    ))
    if export_result.get("status") != "success":
        return json.dumps(export_result, ensure_ascii=False)

    file_path  = export_result["file_path"]
    ky_str     = export_result["ky_cham_cong"]
    so_nv      = export_result["so_nhan_vien"]

    if not subject:
        subject = f"[{company_code}] Bảng chấm công tổng hợp - Kỳ {ky_str}"
    if not body:
        body = (
            f"Kính gửi,\n\n"
            f"Đính kèm bảng chấm công tổng hợp kỳ {ky_str} "
            f"với {so_nv} nhân viên.\n\n"
            f"Mọi thắc mắc vui lòng liên hệ bộ phận Nhân sự.\n\n"
            f"Trân trọng."
        )

    # 2. Gửi mail
    try:
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        from email.utils import formataddr

        from app.core.config import settings
        from app.db.mongo import get_db as _get_db

        db = _get_db()

        # Resolve recipients
        recipients: list[str] = []

        if to_emails:
            for r in to_emails:
                if "@" in r:
                    recipients.append(r)
                else:
                    nv = db["instance_data_thong_tin_nhan_vien"].find_one(
                        {"ten_dang_nhap": r, "is_deleted": {"$ne": True}},
                        {"email": 1},
                    )
                    if nv and nv.get("email"):
                        recipients.append(nv["email"])

        if send_to_don_vi:
            flt: dict = {
                "is_deleted":   {"$ne": True},
                "company_code": company_code,
                "trang_thai_lao_dong.value": "Đang làm việc",
                "$or": [
                    {"don_vi_cong_tac.value":       send_to_don_vi},
                    {"phong_ban_phu_trach.value":   send_to_don_vi},
                    {"path_don_vi_cong_tac": {"$regex": send_to_don_vi, "$options": "i"}},
                ],
            }
            for nv in db["instance_data_thong_tin_nhan_vien"].find(flt, {"email": 1}):
                if nv.get("email"):
                    recipients.append(nv["email"])

        recipients = list(set(recipients))  # dedup
        if not recipients:
            return json.dumps({"error": "Không tìm thấy địa chỉ email nào để gửi."}, ensure_ascii=False)

        # Build email
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"]    = formataddr(("MODATA AI System", settings.MAIL_FROM))
        msg["To"]      = ", ".join(recipients[:50])
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # Attach Excel
        with open(file_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        filename = os.path.basename(file_path)
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
        msg.attach(part)

        smtp_cls = smtplib.SMTP_SSL if settings.MAIL_PORT == 465 else smtplib.SMTP
        with smtp_cls(settings.MAIL_HOST, settings.MAIL_PORT, timeout=30) as smtp:
            if settings.MAIL_PORT != 465 and settings.MAIL_USE_TLS:
                smtp.starttls()
            if settings.MAIL_USERNAME and settings.MAIL_PASSWORD:
                smtp.login(settings.MAIL_USERNAME, settings.MAIL_PASSWORD)
            smtp.sendmail(settings.MAIL_FROM, recipients, msg.as_string())

        return json.dumps({
            "status":     "sent",
            "recipients": recipients,
            "file_path":  file_path,
            "subject":    subject,
            "so_nhan_vien": so_nv,
            "message":    f"Đã gửi bảng chấm công đến {len(recipients)} người.",
        }, ensure_ascii=False)

    except Exception as e:
        logger.error("Send attendance report error: %s", e, exc_info=True)
        return json.dumps({
            "status":    "error",
            "file_path": file_path,
            "message":   f"Xuất Excel thành công nhưng gửi mail thất bại: {str(e)}",
        }, ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8020)