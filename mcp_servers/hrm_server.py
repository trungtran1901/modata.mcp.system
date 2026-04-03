"""
mcp_servers/hrm_server.py
MCP HRM Server — tools chuyên biệt cho Human Resource Management.

Thiết kế theo cùng pattern với data_server.py:
  - Mỗi tool nhận session_id → get_session_context() → kiểm tra quyền
  - Kết quả flatten theo cùng chuẩn _extract_value()
  - Không inject collection list vào LLM context

Collections phục vụ:
  instance_data_thong_tin_nhan_vien          → thông tin nhân viên
  instance_data_ngay_nghi_le                 → ngày nghỉ lễ
  instance_data_ngay_nghi_tuan               → quy định ngày nghỉ tuần
  instance_data_danh_sach_loai_nghi_phep     → danh mục loại nghỉ phép

Tools (prefix sau khi mount: hrm_):
  get_employee_info        — thông tin 1 nhân viên theo username/tên
  search_employees         — tìm kiếm nhân viên full-text
  list_employees           — danh sách nhân viên theo bộ lọc
  get_holidays             — ngày nghỉ lễ theo năm / khoảng thời gian
  get_weekly_off_rules     — quy định ngày nghỉ trong tuần
  get_leave_types          — danh mục loại nghỉ phép + số ngày tối đa
  check_working_schedule   — kiểm tra ngày có phải ngày làm việc không
  get_leave_policy_summary — tổng hợp toàn bộ chính sách nghỉ của công ty
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
mcp    = FastMCP("modata-hrm")


# ─────────────────────────────────────────────────────────────
# VALUE EXTRACTOR — giống data_server.py để nhất quán
# ─────────────────────────────────────────────────────────────

def _extract_value(v: Any) -> Any:
    """
    Chuyển giá trị MongoDB → human-readable.
    Cùng logic với data_server._extract_value() để kết quả nhất quán.

    Lookup field  : {"label": "...", "value": "..."} → label
    MultiSelect   : [{label, value}, ...] → list of labels
    ObjectId/date : str(v)
    Primitive     : giữ nguyên
    """
    if v is None:
        return None
    if isinstance(v, dict):
        label = v.get("label") or v.get("value")
        if label is not None:
            return label
        # Dict khác: serialize bỏ metadata kỹ thuật
        return {
            k: _extract_value(sub)
            for k, sub in v.items()
            if not k.startswith("_")
            and k not in ("data_source", "view_to_open_link", "option",
                          "display_member", "value_member")
        }
    if isinstance(v, list):
        items = [_extract_value(i) for i in v if i is not None]
        items = [i for i in items if i is not None and i != ""]
        if not items:
            return None
        return items if len(items) > 1 else items[0]
    if type(v).__name__ in ("ObjectId", "datetime"):
        return str(v)
    return v


def _isodate_to_vn(dt: Any) -> str | None:
    """
    Chuyển ISODate MongoDB (UTC) → ngày Việt Nam (UTC+7, định dạng DD/MM/YYYY).

    Dữ liệu mẫu:
      tu_ngay: ISODate("2025-08-31T17:00:00.000+0000")
               = 2025-09-01 00:00:00 ICT  → hiển thị 01/09/2025

    Rule: +7 giờ để ra giờ VN, rồi format DD/MM/YYYY.
    """
    if dt is None:
        return None
    if hasattr(dt, "strftime"):
        vn_dt = dt + timedelta(hours=7)
        return vn_dt.strftime("%d/%m/%Y")
    return str(dt)


# ─────────────────────────────────────────────────────────────
# FIELD FLATTEN — chuyên biệt từng collection
# ─────────────────────────────────────────────────────────────

def _flatten_nhan_vien(doc: dict) -> dict:
    """
    Flatten document nhân viên → {display_name: human_value}.
    Chỉ giữ fields nghiệp vụ quan trọng, loại bỏ metadata kỹ thuật.
    """
    result: dict = {}

    # ── Thông tin cơ bản ─────────────────────────────────────
    ten = doc.get("ho_va_ten_co_dau") or doc.get("ho_va_ten")
    if ten:
        result["Họ và tên"] = ten
    if doc.get("ten_dang_nhap"):
        result["Tên đăng nhập"] = doc["ten_dang_nhap"]
    if doc.get("email"):
        result["Email"] = doc["email"]
    if doc.get("so_dien_thoai"):
        result["Số điện thoại"] = doc["so_dien_thoai"]

    gioi_tinh = _extract_value(doc.get("gioi_tinh"))
    if gioi_tinh:
        result["Giới tính"] = gioi_tinh

    if doc.get("ngay_sinh"):
        result["Ngày sinh"] = _isodate_to_vn(doc["ngay_sinh"])

    so_cccd = doc.get("so_can_cuoc") or doc.get("so_cmnd")
    if so_cccd:
        result["CCCD/CMND"] = so_cccd

    # ── Công việc ─────────────────────────────────────────────
    dv = _extract_value(doc.get("don_vi_cong_tac"))
    if dv:
        result["Đơn vị công tác"] = dv

    pb = _extract_value(doc.get("phong_ban_phu_trach"))
    if pb:
        result["Phòng ban"] = pb

    cv = _extract_value(doc.get("chuc_vu"))
    if cv:
        result["Chức vụ"] = cv

    vt = _extract_value(doc.get("vi_tri_cong_viec"))
    if vt:
        result["Vị trí công việc"] = vt

    if doc.get("ngay_vao_lam"):
        result["Ngày vào làm"] = _isodate_to_vn(doc["ngay_vao_lam"])

    trang_thai = _extract_value(doc.get("trang_thai_lao_dong"))
    if trang_thai:
        result["Trạng thái lao động"] = trang_thai

    loai_hd = _extract_value(doc.get("loai_hop_dong"))
    if loai_hd:
        result["Loại hợp đồng"] = loai_hd

    # ── Lương (chỉ hiển thị nếu field tồn tại trong doc) ─────
    if doc.get("luong_co_ban") is not None:
        result["Lương cơ bản"] = doc["luong_co_ban"]
    if doc.get("he_so_luong") is not None:
        result["Hệ số lương"] = doc["he_so_luong"]

    return result


def _flatten_ngay_nghi_le(doc: dict) -> dict:
    """Flatten document ngày nghỉ lễ."""
    result: dict = {}
    if doc.get("ten_ngay_nghi"):
        result["Tên ngày nghỉ"] = doc["ten_ngay_nghi"]
    if doc.get("tu_ngay"):
        result["Từ ngày"] = _isodate_to_vn(doc["tu_ngay"])
    if doc.get("den_ngay"):
        result["Đến ngày"] = _isodate_to_vn(doc["den_ngay"])
    if doc.get("so_ngay_nghi") is not None:
        result["Số ngày nghỉ"] = doc["so_ngay_nghi"]
    doi_tuong = _extract_value(doc.get("doi_tuong_ap_dung"))
    if doi_tuong:
        result["Đối tượng áp dụng"] = doi_tuong
    if doc.get("ghi_chu"):
        result["Ghi chú"] = doc["ghi_chu"]
    return result


def _flatten_ngay_nghi_tuan(doc: dict) -> dict:
    """Flatten document quy định ngày nghỉ tuần."""
    result: dict = {}
    loai = _extract_value(doc.get("loai_nghi_tuan"))
    if loai:
        result["Ngày nghỉ"] = loai
    don_vi = _extract_value(doc.get("don_vi_ap_dung"))
    if don_vi:
        result["Đơn vị áp dụng"] = don_vi
    if doc.get("ghi_chu"):
        result["Ghi chú"] = doc["ghi_chu"]
    return result


def _flatten_loai_nghi_phep(doc: dict) -> dict:
    """Flatten document loại nghỉ phép."""
    result: dict = {}
    if doc.get("ten_loai_nghi"):
        result["Tên loại nghỉ"] = doc["ten_loai_nghi"]
    if doc.get("ky_hieu"):
        result["Ký hiệu"] = doc["ky_hieu"]
    if doc.get("so_ngay_toi_da") is not None:
        result["Số ngày tối đa/năm"] = doc["so_ngay_toi_da"]
    doi_tuong = _extract_value(doc.get("doi_tuong_ap_dung"))
    if doi_tuong:
        result["Đối tượng áp dụng"] = doi_tuong
    tinh_luong = _extract_value(doc.get("tinh_vao_luong"))
    if tinh_luong:
        result["Tính vào lương"] = tinh_luong
    return result


# ─────────────────────────────────────────────────────────────
# SESSION GUARD — kiểm tra session hợp lệ trước khi query
# ─────────────────────────────────────────────────────────────

def _require_valid_session(session_id: str, instance_name: str | None = None) -> str | None:
    """
    Kiểm tra session hợp lệ.
    - instance_name=None: chỉ cần session có context (không empty)
    - instance_name="xyz": kiểm tra thêm can_access()

    Trả về None nếu OK, JSON error string nếu fail.
    """
    ctx = get_session_context(session_id)

    # Session hoàn toàn empty → chưa login hoặc hết hạn
    if not ctx.accessible_instance_names():
        return json.dumps({
            "error": "Session không hợp lệ hoặc đã hết hạn. Vui lòng đăng nhập lại.",
        }, ensure_ascii=False)

    # Strict permission check khi instance_name được chỉ định
    if instance_name and not ctx.can_access(instance_name):
        return json.dumps({
            "error": f"Bạn không có quyền truy cập dữ liệu này.",
            "hint":  f"Collection '{instance_name}' không nằm trong danh sách được phép.",
        }, ensure_ascii=False)

    return None


# ─────────────────────────────────────────────────────────────
# TOOLS — NHÂN VIÊN
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_employee_info(session_id: str, username_or_name: str) -> str:
    """
    Lấy thông tin chi tiết 1 nhân viên theo username hoặc họ tên.

    Tìm theo thứ tự ưu tiên:
      1. ten_dang_nhap (username) — exact match (nhanh, chính xác nhất)
      2. ho_va_ten_co_dau / ho_va_ten — regex case-insensitive

    Kết quả đã flatten: key = tiếng Việt, value = human-readable.
    Dùng trước tools_calculate_service_time để tính thâm niên công tác.

    Args:
        session_id:       Session ID để kiểm tra quyền
        username_or_name: Username (ten_dang_nhap) HOẶC họ và tên nhân viên
    """
    # Strict check: collection nhân viên cần quyền rõ ràng
    err = _require_valid_session(session_id, "thong_tin_nhan_vien")
    if err:
        return err

    db  = get_db()
    col = db["instance_data_thong_tin_nhan_vien"]

    # 1. Tìm theo username (exact match)
    doc = col.find_one(
        {"ten_dang_nhap": username_or_name, "is_deleted": {"$ne": True}},
    )

    # 2. Fallback: tìm theo tên (regex)
    if not doc:
        doc = col.find_one({
            "is_deleted": {"$ne": True},
            "$or": [
                {"ho_va_ten":        {"$regex": username_or_name, "$options": "i"}},
                {"ho_va_ten_co_dau": {"$regex": username_or_name, "$options": "i"}},
            ],
        })

    if not doc:
        return json.dumps(
            {"error": f"Không tìm thấy nhân viên: '{username_or_name}'"},
            ensure_ascii=False,
        )

    return json.dumps(
        _flatten_nhan_vien(doc),
        ensure_ascii=False,
        default=str,
    )


@mcp.tool()
def search_employees(
    session_id:   str,
    keyword:      str,
    limit:        int = 10,
    company_code: str = "HITC",
) -> str:
    """
    Tìm kiếm nhân viên theo tên, username, email, hoặc số điện thoại.

    Tìm kiếm đồng thời trên nhiều fields — trả về tất cả kết quả khớp.
    Kết quả đã flatten, kèm tổng số tìm được (total) và số trả về (count).

    Args:
        session_id:   Session ID
        keyword:      Từ khóa tìm kiếm (tên, username, email, SĐT)
        limit:        Số kết quả tối đa (mặc định 10, tối đa 50)
        company_code: Mã công ty
    """
    err = _require_valid_session(session_id, "thong_tin_nhan_vien")
    if err:
        return err

    db  = get_db()
    col = db["instance_data_thong_tin_nhan_vien"]
    flt = {
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "$or": [
            {"ho_va_ten":        {"$regex": keyword, "$options": "i"}},
            {"ho_va_ten_co_dau": {"$regex": keyword, "$options": "i"}},
            {"ten_dang_nhap":    {"$regex": keyword, "$options": "i"}},
            {"email":            {"$regex": keyword, "$options": "i"}},
            {"so_dien_thoai":    {"$regex": keyword, "$options": "i"}},
        ],
    }
    limit = max(1, min(limit, 50))
    docs  = list(col.find(flt).limit(limit))
    total = col.count_documents(flt)

    return json.dumps(
        {
            "keyword":   keyword,
            "total":     total,
            "count":     len(docs),
            "employees": [_flatten_nhan_vien(d) for d in docs],
        },
        ensure_ascii=False,
        default=str,
    )


@mcp.tool()
def list_employees(
    session_id:   str,
    don_vi_code:  Optional[str] = None,
    trang_thai:   str           = "Đang làm việc",
    limit:        int           = 20,
    skip:         int           = 0,
    company_code: str           = "HITC",
) -> str:
    """
    Danh sách nhân viên với bộ lọc đơn vị và trạng thái lao động.

    Hỗ trợ phân trang qua skip/limit. Dùng để liệt kê nhân viên
    theo phòng ban hoặc lấy tổng danh sách toàn công ty.

    Args:
        session_id:   Session ID
        don_vi_code:  Mã hoặc tên đơn vị/phòng ban (None = tất cả)
        trang_thai:   Trạng thái lao động (mặc định 'Đang làm việc', None = tất cả)
        limit:        Số bản ghi/trang (mặc định 20, tối đa 100)
        skip:         Bỏ qua N bản ghi đầu (phân trang)
        company_code: Mã công ty
    """
    err = _require_valid_session(session_id, "thong_tin_nhan_vien")
    if err:
        return err

    db  = get_db()
    col = db["instance_data_thong_tin_nhan_vien"]
    flt: dict = {
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
    }

    if trang_thai:
        flt["trang_thai_lao_dong.value"] = trang_thai

    if don_vi_code:
        flt["$or"] = [
            {"don_vi_cong_tac.value":       don_vi_code},
            {"don_vi_cong_tac.option.code": don_vi_code},
            {"phong_ban_phu_trach.value":   don_vi_code},
            {"path_don_vi_cong_tac": {"$regex": don_vi_code, "$options": "i"}},
        ]

    limit = max(1, min(limit, 100))
    docs  = list(col.find(flt).skip(skip).limit(limit))
    total = col.count_documents(flt)

    return json.dumps(
        {
            "total":     total,
            "count":     len(docs),
            "skip":      skip,
            "employees": [_flatten_nhan_vien(d) for d in docs],
        },
        ensure_ascii=False,
        default=str,
    )


# ─────────────────────────────────────────────────────────────
# TOOLS — NGÀY NGHỈ LỄ
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_holidays(
    session_id:   str,
    year:         Optional[int] = None,
    from_date:    Optional[str] = None,
    to_date:      Optional[str] = None,
    company_code: str           = "HITC",
) -> str:
    """
    Lấy danh sách ngày nghỉ lễ chính thức.
    Collection: instance_data_ngay_nghi_le

    Lưu ý múi giờ: MongoDB lưu UTC, tu_ngay = 17:00 UTC ngày trước
    = 00:00 ICT của ngày nghỉ. Tool tự xử lý và hiển thị đúng ngày VN.

    Ưu tiên from_date/to_date hơn year nếu cả hai được truyền vào.

    Args:
        session_id:   Session ID
        year:         Năm cần xem (None = năm hiện tại)
        from_date:    Lọc từ ngày (YYYY-MM-DD)
        to_date:      Lọc đến ngày (YYYY-MM-DD), bắt buộc khi có from_date
        company_code: Mã công ty
    """
    err = _require_valid_session(session_id)
    if err:
        return err

    db  = get_db()
    col = db["instance_data_ngay_nghi_le"]
    flt: dict = {
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
    }

    if from_date and to_date:
        try:
            dt_from = datetime.strptime(from_date, "%Y-%m-%d")
            dt_to   = datetime.strptime(to_date,   "%Y-%m-%d")
            # Tu_ngay lưu là 17:00 UTC (= 00:00 ICT của ngày nghỉ bắt đầu)
            # Để bắt đúng kỳ nghỉ overlap với khoảng [from_date, to_date]:
            #   tu_ngay  <= to_date 23:59:59 UTC
            #   den_ngay >= from_date 00:00:00 UTC (trừ 7h để ra UTC từ ICT)
            flt["tu_ngay"]  = {"$lte": datetime(dt_to.year,   dt_to.month,   dt_to.day,   23, 59, 59)}
            flt["den_ngay"] = {"$gte": datetime(dt_from.year, dt_from.month, dt_from.day) - timedelta(hours=7)}
        except ValueError:
            return json.dumps(
                {"error": "Định dạng ngày không hợp lệ. Dùng YYYY-MM-DD"},
                ensure_ascii=False,
            )
    else:
        target_year = year or datetime.now().year
        year_start  = datetime(target_year, 1, 1)
        year_end    = datetime(target_year, 12, 31, 23, 59, 59)
        flt["$or"] = [
            {"tu_ngay":  {"$gte": year_start, "$lte": year_end}},
            {"den_ngay": {"$gte": year_start, "$lte": year_end}},
        ]

    docs  = list(col.find(flt).sort("tu_ngay", 1))
    items = [_flatten_ngay_nghi_le(d) for d in docs]
    total_days = sum(
        d.get("so_ngay_nghi", 0) for d in docs
        if isinstance(d.get("so_ngay_nghi"), (int, float))
    )

    return json.dumps(
        {
            "year":           year or datetime.now().year,
            "total_holidays": len(items),
            "total_days_off": total_days,
            "holidays":       items,
            "summary": (
                f"Có {len(items)} đợt nghỉ lễ với tổng {total_days} ngày nghỉ"
                if items else "Không có dữ liệu ngày nghỉ lễ."
            ),
        },
        ensure_ascii=False,
        default=str,
    )


# ─────────────────────────────────────────────────────────────
# TOOLS — QUY ĐỊNH NGÀY NGHỈ TUẦN
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_weekly_off_rules(
    session_id:   str,
    company_code: str = "HITC",
) -> str:
    """
    Lấy quy định ngày nghỉ hàng tuần (Thứ 7, Chủ nhật...).
    Collection: instance_data_ngay_nghi_tuan

    Một công ty có thể có nhiều quy định khác nhau theo đơn vị.
    Kết quả trả về đã tổng hợp danh sách ngày nghỉ và chi tiết theo đơn vị.

    Args:
        session_id:   Session ID
        company_code: Mã công ty
    """
    err = _require_valid_session(session_id)
    if err:
        return err

    db  = get_db()
    col = db["instance_data_ngay_nghi_tuan"]
    flt = {
        "is_deleted":   {"$ne": True},
        "is_active":    {"$ne": False},
        "company_code": company_code,
    }

    docs  = list(col.find(flt).sort("muc_do_uu_tien", -1))
    items = [_flatten_ngay_nghi_tuan(d) for d in docs]

    # Tổng hợp danh sách tên ngày nghỉ (deduplicate)
    seen: set    = set()
    ngay_nghi:   list[str] = []
    for d in docs:
        name = _extract_value(d.get("loai_nghi_tuan"))
        if name and str(name) not in seen:
            ngay_nghi.append(str(name))
            seen.add(str(name))

    return json.dumps(
        {
            "total":     len(items),
            "ngay_nghi": ngay_nghi,
            "chi_tiet":  items,
            "summary": (
                f"Nghỉ hàng tuần vào: {', '.join(ngay_nghi)}"
                if ngay_nghi else "Chưa có quy định ngày nghỉ tuần."
            ),
        },
        ensure_ascii=False,
        default=str,
    )


# ─────────────────────────────────────────────────────────────
# TOOLS — LOẠI NGHỈ PHÉP
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_leave_types(
    session_id:   str,
    company_code: str = "HITC",
) -> str:
    """
    Lấy danh mục tất cả loại nghỉ phép của công ty.
    Collection: instance_data_danh_sach_loai_nghi_phep

    Trả về: tên loại nghỉ, số ngày tối đa/năm,
    có tính vào lương không, đối tượng áp dụng.

    Args:
        session_id:   Session ID
        company_code: Mã công ty
    """
    err = _require_valid_session(session_id)
    if err:
        return err

    db  = get_db()
    col = db["instance_data_danh_sach_loai_nghi_phep"]
    flt = {
        "is_deleted":   {"$ne": True},
        "is_active":    {"$ne": False},
        "company_code": company_code,
    }

    docs  = list(col.find(flt).sort("ten_loai_nghi", 1))
    items = [_flatten_loai_nghi_phep(d) for d in docs]

    return json.dumps(
        {
            "total":       len(items),
            "leave_types": items,
            "summary": (
                f"Công ty có {len(items)} loại nghỉ phép"
                if items else "Chưa có dữ liệu loại nghỉ phép."
            ),
        },
        ensure_ascii=False,
        default=str,
    )


# ─────────────────────────────────────────────────────────────
# TOOLS — KIỂM TRA NGÀY LÀM VIỆC
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def check_working_schedule(
    session_id:   str,
    check_date:   Optional[str] = None,
    company_code: str           = "HITC",
) -> str:
    """
    Kiểm tra một ngày cụ thể có phải ngày làm việc không.

    Kết hợp 2 nguồn:
      1. instance_data_ngay_nghi_le  — ngày nghỉ lễ chính thức
      2. instance_data_ngay_nghi_tuan — quy định nghỉ tuần

    Thứ tự kiểm tra: nghỉ lễ → nghỉ tuần → ngày làm việc.

    Args:
        session_id:   Session ID
        check_date:   Ngày cần kiểm tra (YYYY-MM-DD, None = hôm nay)
        company_code: Mã công ty

    Returns JSON:
        {
          "date": "2025-09-01",
          "day_of_week": "Thứ 2",
          "is_working_day": false,
          "reason": "Nghỉ lễ: Nghỉ lễ 2-9",
          "holiday_name": "Nghỉ lễ 2-9"
        }
    """
    err = _require_valid_session(session_id)
    if err:
        return err

    try:
        target = (
            datetime.strptime(check_date, "%Y-%m-%d").date()
            if check_date else datetime.now().date()
        )
    except ValueError:
        return json.dumps(
            {"error": "Định dạng ngày không hợp lệ. Dùng YYYY-MM-DD"},
            ensure_ascii=False,
        )

    weekday_names = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ nhật"]
    weekday_name  = weekday_names[target.weekday()]
    db            = get_db()

    # ── 1. Kiểm tra ngày nghỉ lễ ─────────────────────────────
    # Tu_ngay = 17:00 UTC ngày trước = 00:00 ICT ngày nghỉ.
    # Den_ngay = 17:00 UTC ngày nghỉ cuối = 00:00 ICT ngày kế tiếp.
    # Để kiểm tra target có trong kỳ nghỉ:
    #   tu_ngay  <= target 23:59 UTC  (bắt cả ngày ICT)
    #   den_ngay >= (target - 7h) UTC (đảm bảo den_ngay chưa qua)
    dt_start  = datetime(target.year, target.month, target.day, 0, 0, 0)
    dt_end    = datetime(target.year, target.month, target.day, 23, 59, 59)
    dt_target_utc = dt_start - timedelta(hours=7)

    holiday = db["instance_data_ngay_nghi_le"].find_one({
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "tu_ngay":      {"$lte": dt_end},
        "den_ngay":     {"$gte": dt_target_utc},
    })

    if holiday:
        return json.dumps({
            "date":           str(target),
            "day_of_week":    weekday_name,
            "is_working_day": False,
            "reason":         f"Nghỉ lễ: {holiday.get('ten_ngay_nghi', 'Ngày nghỉ lễ')}",
            "holiday_name":   holiday.get("ten_ngay_nghi"),
        }, ensure_ascii=False)

    # ── 2. Kiểm tra quy định nghỉ tuần ───────────────────────
    off_rules = list(db["instance_data_ngay_nghi_tuan"].find({
        "is_deleted":   {"$ne": True},
        "is_active":    {"$ne": False},
        "company_code": company_code,
    }))

    # Map để match tên ngày (hệ thống lưu nhiều cách gọi khác nhau)
    day_variations: dict[str, list[str]] = {
        "Thứ 2":    ["Thứ Hai",  "Thứ 2",  "T2"],
        "Thứ 3":    ["Thứ Ba",   "Thứ 3",  "T3"],
        "Thứ 4":    ["Thứ Tư",   "Thứ 4",  "T4"],
        "Thứ 5":    ["Thứ Năm",  "Thứ 5",  "T5"],
        "Thứ 6":    ["Thứ Sáu",  "Thứ 6",  "T6"],
        "Thứ 7":    ["Thứ Bảy",  "Thứ 7",  "T7"],
        "Chủ nhật": ["Chủ Nhật", "Chủ nhật", "CN"],
    }
    current_variations = day_variations.get(weekday_name, [weekday_name])

    for rule in off_rules:
        off_name = _extract_value(rule.get("loai_nghi_tuan"))
        if not off_name:
            continue
        off_str = str(off_name)
        if any(
            v.lower() in off_str.lower() or off_str.lower() in v.lower()
            for v in current_variations
        ):
            return json.dumps({
                "date":           str(target),
                "day_of_week":    weekday_name,
                "is_working_day": False,
                "reason":         f"Ngày nghỉ tuần: {off_str}",
            }, ensure_ascii=False)

    # ── 3. Ngày làm việc ─────────────────────────────────────
    return json.dumps({
        "date":           str(target),
        "day_of_week":    weekday_name,
        "is_working_day": True,
        "reason":         "Ngày làm việc bình thường",
    }, ensure_ascii=False)


# ─────────────────────────────────────────────────────────────
# TOOLS — TỔNG HỢP CHÍNH SÁCH NGHỈ
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_leave_policy_summary(
    session_id:   str,
    company_code: str = "HITC",
) -> str:
    """
    Tổng hợp toàn bộ chính sách nghỉ của công ty trong 1 lần gọi.

    Bao gồm:
      1. Ngày nghỉ hàng tuần (Thứ 7, Chủ nhật...)
      2. Danh mục loại nghỉ phép + số ngày tối đa/năm
      3. Tất cả ngày nghỉ lễ của năm hiện tại

    Dùng khi user hỏi tổng quát về chế độ nghỉ phép thay vì gọi 3 tools riêng.

    Args:
        session_id:   Session ID
        company_code: Mã công ty
    """
    err = _require_valid_session(session_id)
    if err:
        return err

    db           = get_db()
    current_year = datetime.now().year

    # 1. Ngày nghỉ tuần
    weekly_docs = list(db["instance_data_ngay_nghi_tuan"].find(
        {"is_deleted": {"$ne": True}, "is_active": {"$ne": False}, "company_code": company_code},
    ))
    seen_days: set  = set()
    weekly_off:     list[str] = []
    for d in weekly_docs:
        name = _extract_value(d.get("loai_nghi_tuan"))
        if name and str(name) not in seen_days:
            weekly_off.append(str(name))
            seen_days.add(str(name))

    # 2. Loại nghỉ phép
    leave_docs  = list(db["instance_data_danh_sach_loai_nghi_phep"].find(
        {"is_deleted": {"$ne": True}, "is_active": {"$ne": False}, "company_code": company_code},
    ).sort("ten_loai_nghi", 1))
    leave_types = [_flatten_loai_nghi_phep(d) for d in leave_docs]

    # 3. Ngày nghỉ lễ năm hiện tại
    year_start    = datetime(current_year, 1, 1)
    year_end      = datetime(current_year, 12, 31, 23, 59, 59)
    holiday_docs  = list(db["instance_data_ngay_nghi_le"].find({
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "$or": [
            {"tu_ngay":  {"$gte": year_start, "$lte": year_end}},
            {"den_ngay": {"$gte": year_start, "$lte": year_end}},
        ],
    }).sort("tu_ngay", 1))
    holidays          = [_flatten_ngay_nghi_le(d) for d in holiday_docs]
    total_holiday_days = sum(
        d.get("so_ngay_nghi", 0) for d in holiday_docs
        if isinstance(d.get("so_ngay_nghi"), (int, float))
    )

    return json.dumps({
        "company_code": company_code,
        "year":         current_year,

        "ngay_nghi_tuan": {
            "summary":   f"Nghỉ vào: {', '.join(weekly_off)}" if weekly_off else "Chưa cấu hình",
            "ngay_nghi": weekly_off,
        },

        "loai_nghi_phep": {
            "summary":  f"Có {len(leave_types)} loại nghỉ phép",
            "chi_tiet": leave_types,
        },

        "ngay_nghi_le": {
            "summary":   f"Năm {current_year}: {len(holidays)} đợt, tổng {total_holiday_days} ngày nghỉ",
            "tong_dot":  len(holidays),
            "tong_ngay": total_holiday_days,
            "chi_tiet":  holidays,
        },
    }, ensure_ascii=False, default=str)


# ─────────────────────────────────────────────────────────────
# STANDALONE
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8017)