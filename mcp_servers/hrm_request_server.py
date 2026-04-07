"""
mcp_servers/hrm_request_server.py  (v2 — fix filter field names)

Từ logs, agent gọi filter sai:
  SAI: {"don_vi_code": ..., "created_by": ..., "month": ..., "year": ...}
  ĐÚNG: {"nguoi_nop_don.value": ..., "loai_don": ..., "ngay_nop_don": {...}}

Fix:
  - Tool description nêu rõ field names thực trong MongoDB
  - Filter chỉ dùng field names đúng: nguoi_nop_don.value, loai_don,
    trang_thai_phe_duyet.value, ngay_nop_don, don_vi_cong_tac.value
  - Date filter dùng ngay_nop_don (ngày nộp đơn), không phải tu_ngay
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
mcp    = FastMCP("modata-hrm-request")

COLLECTION    = "instance_data_danh_sach_quan_ly_don_xin_nghi"
INSTANCE_NAME = "danh_sach_quan_ly_don_xin_nghi"

HR_VIEW_NAMES = {
    "danh_sach_quan_ly_don_xin_nghi_list",
    "quan_ly_don_di_muon_ve_som_list",
    "quan_ly_don_lam_viec_tu_xa_list",
    "danh_sach_don_de_nghi_cong_tac_list",
}
NV_VIEW_NAMES = {
    "danh_sach_don_xin_nghi_cua_toi_list",
    "danh_sach_don_di_muon_ve_som_list",
    "danh_sach_don_xin_lam_viec_tu_xa_list",
    "de_nghi_di_cong_tac_list",
}

_LOAI_DON_MAP = {
    "nghỉ phép": "Nghỉ phép", "nghi phep": "Nghỉ phép", "phep": "Nghỉ phép",
    "nghỉ ốm": "Nghỉ ốm", "nghi om": "Nghỉ ốm",
    "đi muộn": "Đi muộn, về sớm", "di muon": "Đi muộn, về sớm",
    "về sớm": "Đi muộn, về sớm", "đi muộn, về sớm": "Đi muộn, về sớm",
    "làm việc từ xa": "Làm việc từ xa", "remote": "Làm việc từ xa",
    "wfh": "Làm việc từ xa", "từ xa": "Làm việc từ xa",
    "công tác": "Đề nghị đi công tác", "cong tac": "Đề nghị đi công tác",
    "đề nghị đi công tác": "Đề nghị đi công tác",
}
_LOAI_DON_EXACT = {"Nghỉ phép", "Nghỉ ốm", "Đi muộn, về sớm", "Làm việc từ xa", "Đề nghị đi công tác"}


def _norm(val: str | None) -> list[str] | None:
    if not val:
        return None
    key = val.lower().strip()
    if key in ("xin nghỉ", "xin nghi", "nghỉ", "nghi"):
        return ["Nghỉ phép", "Nghỉ ốm"]
    if key in _LOAI_DON_MAP:
        return [_LOAI_DON_MAP[key]]
    if val in _LOAI_DON_EXACT:
        return [val]
    for v in _LOAI_DON_EXACT:
        if key in v.lower():
            return [v]
    return None


def _ev(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, dict):
        return v.get("label") or v.get("value")
    if isinstance(v, list):
        items = [_ev(i) for i in v if i]
        items = [i for i in items if i]
        return items[0] if len(items) == 1 else (items or None)
    if type(v).__name__ in ("ObjectId", "datetime"):
        return str(v)
    return v


def _vn_date(dt: Any) -> str | None:
    if dt is None:
        return None
    if hasattr(dt, "strftime"):
        return (dt + timedelta(hours=7)).strftime("%d/%m/%Y")
    return str(dt)


def _flatten(doc: dict) -> dict:
    r: dict = {}
    loai = doc.get("loai_don", "")
    r["Loại đơn"] = loai
    nop = doc.get("nguoi_nop_don", {})
    if isinstance(nop, dict):
        r["Người nộp"] = nop.get("label") or nop.get("value", "")
    if doc.get("ngay_nop_don"):
        r["Ngày nộp"] = _vn_date(doc["ngay_nop_don"])
    if doc.get("tu_ngay"):
        r["Từ ngày"] = _vn_date(doc["tu_ngay"])
    if doc.get("den_ngay"):
        r["Đến ngày"] = _vn_date(doc["den_ngay"])
    tt = _ev(doc.get("trang_thai_phe_duyet"))
    if tt:
        r["Trạng thái"] = tt
    dv = _ev(doc.get("don_vi_cong_tac"))
    if dv:
        r["Đơn vị"] = dv
    if doc.get("ly_do"):
        r["Lý do"] = doc["ly_do"]
    pd = _ev(doc.get("nguoi_phe_duyet") or doc.get("nguoi_duyet"))
    if pd:
        r["Người phê duyệt"] = pd
    if loai in ("Nghỉ phép", "Nghỉ ốm"):
        ln = _ev(doc.get("loai_nghi"))
        if ln:
            r["Loại nghỉ"] = ln
        if doc.get("so_ngay_nghi"):
            r["Số ngày"] = doc["so_ngay_nghi"]
    elif loai == "Đi muộn, về sớm":
        if doc.get("di_muon_dau_ca"):
            r["Đi muộn (phút)"] = doc["di_muon_dau_ca"]
        if doc.get("ve_som_cuoi_ca"):
            r["Về sớm (phút)"] = doc["ve_som_cuoi_ca"]
    elif loai == "Làm việc từ xa":
        ca = _ev(doc.get("ca_lam_viec"))
        if ca:
            r["Ca làm việc"] = ca
    elif loai == "Đề nghị đi công tác":
        if doc.get("so_ngay_di_cong_tac"):
            r["Số ngày CT"] = doc["so_ngay_di_cong_tac"]
    return {k: v for k, v in r.items() if v is not None and v != ""}


def _perm(session_id: str) -> dict:
    ctx = get_session_context(session_id)
    if not ctx.accessible_instance_names():
        return {"ok": False, "error": "Session không hợp lệ."}
    ma_set = set(ctx.get_ma_chuc_nang_list(INSTANCE_NAME))
    if not ma_set:
        return {"ok": False, "error": "Không có quyền xem đơn từ."}
    return {
        "ok":    True,
        "is_hr": bool(ma_set & HR_VIEW_NAMES),
    }


def _date_flt(from_date: str | None, to_date: str | None) -> dict:
    """Date filter trên field ngay_nop_don (ngày nộp đơn, lưu UTC)."""
    flt: dict = {}
    try:
        if from_date:
            dt = datetime.strptime(from_date, "%Y-%m-%d")
            flt.setdefault("ngay_nop_don", {})["$gte"] = dt - timedelta(hours=7)
        if to_date:
            dt = datetime.strptime(to_date, "%Y-%m-%d")
            flt.setdefault("ngay_nop_don", {})["$lte"] = dt + timedelta(hours=16, minutes=59)
    except ValueError:
        pass
    return flt


# ─────────────────────────────────────────────────────────────
# TOOLS — description ngắn, nêu rõ field names MongoDB
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def get_my_requests(
    session_id:   str,
    username:     str,
    loai_don:     Optional[str] = None,
    trang_thai:   Optional[str] = None,
    from_date:    Optional[str] = None,
    to_date:      Optional[str] = None,
    limit:        int           = 10,
    company_code: str           = "HITC",
) -> str:
    """
    Lấy đơn từ của 1 nhân viên.
    Filter MongoDB: nguoi_nop_don.value=username, loai_don, trang_thai_phe_duyet.value, ngay_nop_don.
    loai_don nhận: "Nghỉ phép"|"Nghỉ ốm"|"Đi muộn, về sớm"|"Làm việc từ xa"|"Đề nghị đi công tác"
                   hoặc viết tắt: "nghỉ phép","remote","công tác","đi muộn".
    trang_thai: "Đã duyệt"|"Chờ phê duyệt"|"Từ chối".
    from_date/to_date: YYYY-MM-DD (ngày nộp đơn).
    """
    p = _perm(session_id)
    if not p["ok"]:
        return json.dumps({"error": p["error"]}, ensure_ascii=False)

    db  = get_db()
    flt: dict = {
        "is_deleted":          {"$ne": True},
        "company_code":        company_code,
        "nguoi_nop_don.value": username,
    }
    ll = _norm(loai_don)
    if ll:
        flt["loai_don"] = ll[0] if len(ll) == 1 else {"$in": ll}
    if trang_thai:
        flt["trang_thai_phe_duyet.value"] = trang_thai
    flt.update(_date_flt(from_date, to_date))

    limit = max(1, min(limit, 50))
    docs  = list(db[COLLECTION].find(flt).sort("ngay_nop_don", -1).limit(limit))
    total = db[COLLECTION].count_documents(flt)

    return json.dumps({
        "username": username, "total": total, "count": len(docs),
        "requests": [_flatten(d) for d in docs],
    }, ensure_ascii=False, default=str)


@mcp.tool()
def list_requests(
    session_id:   str,
    username:     str,
    loai_don:     Optional[str] = None,
    trang_thai:   Optional[str] = None,
    don_vi_code:  Optional[str] = None,
    from_date:    Optional[str] = None,
    to_date:      Optional[str] = None,
    limit:        int           = 20,
    skip:         int           = 0,
    company_code: str           = "HITC",
) -> str:
    """
    Danh sách đơn từ. Permission tự động:
      HR (view quản lý): xem cả công ty, lọc được don_vi_code (field don_vi_cong_tac.value).
      NV thường: tự động filter nguoi_nop_don.value=username, bỏ qua don_vi_code.
    Các field filter: loai_don, trang_thai_phe_duyet.value, ngay_nop_don, don_vi_cong_tac.value.
    """
    p = _perm(session_id)
    if not p["ok"]:
        return json.dumps({"error": p["error"]}, ensure_ascii=False)

    db  = get_db()
    flt: dict = {"is_deleted": {"$ne": True}, "company_code": company_code}

    if not p["is_hr"]:
        flt["nguoi_nop_don.value"] = username
    elif don_vi_code:
        flt["don_vi_cong_tac.value"] = don_vi_code

    ll = _norm(loai_don)
    if ll:
        flt["loai_don"] = ll[0] if len(ll) == 1 else {"$in": ll}
    if trang_thai:
        flt["trang_thai_phe_duyet.value"] = trang_thai
    flt.update(_date_flt(from_date, to_date))

    limit = max(1, min(limit, 50))
    docs  = list(db[COLLECTION].find(flt).sort("ngay_nop_don", -1).skip(skip).limit(limit))
    total = db[COLLECTION].count_documents(flt)

    return json.dumps({
        "is_hr": p["is_hr"], "total": total, "count": len(docs),
        "requests": [_flatten(d) for d in docs],
    }, ensure_ascii=False, default=str)


@mcp.tool()
def get_requests_by_user(
    session_id:      str,
    target_username: str,
    loai_don:        Optional[str] = None,
    trang_thai:      Optional[str] = None,
    from_date:       Optional[str] = None,
    to_date:         Optional[str] = None,
    limit:           int           = 10,
    company_code:    str           = "HITC",
) -> str:
    """
    HR xem đơn của nhân viên khác. Filter: nguoi_nop_don.value=target_username.
    Trả lỗi nếu người dùng không có quyền HR.
    """
    p = _perm(session_id)
    if not p["ok"]:
        return json.dumps({"error": p["error"]}, ensure_ascii=False)
    if not p["is_hr"]:
        return json.dumps({"error": "Chỉ HR mới xem được đơn của nhân viên khác."}, ensure_ascii=False)

    db  = get_db()
    flt: dict = {
        "is_deleted": {"$ne": True}, "company_code": company_code,
        "nguoi_nop_don.value": target_username,
    }
    ll = _norm(loai_don)
    if ll:
        flt["loai_don"] = ll[0] if len(ll) == 1 else {"$in": ll}
    if trang_thai:
        flt["trang_thai_phe_duyet.value"] = trang_thai
    flt.update(_date_flt(from_date, to_date))

    docs  = list(db[COLLECTION].find(flt).sort("ngay_nop_don", -1).limit(min(limit, 50)))
    total = db[COLLECTION].count_documents(flt)
    return json.dumps({
        "target": target_username, "total": total, "count": len(docs),
        "requests": [_flatten(d) for d in docs],
    }, ensure_ascii=False, default=str)


@mcp.tool()
def get_pending_requests(
    session_id:   str,
    username:     str,
    loai_don:     Optional[str] = None,
    don_vi_code:  Optional[str] = None,
    company_code: str           = "HITC",
) -> str:
    """
    Đơn đang chờ phê duyệt (trang_thai_phe_duyet.value = "Chờ phê duyệt").
    HR: xem cả công ty. NV: chỉ của mình (nguoi_nop_don.value=username).
    """
    p = _perm(session_id)
    if not p["ok"]:
        return json.dumps({"error": p["error"]}, ensure_ascii=False)

    db  = get_db()
    flt: dict = {
        "is_deleted": {"$ne": True}, "company_code": company_code,
        "trang_thai_phe_duyet.value": "Chờ phê duyệt",
    }
    if not p["is_hr"]:
        flt["nguoi_nop_don.value"] = username
    elif don_vi_code:
        flt["don_vi_cong_tac.value"] = don_vi_code
    ll = _norm(loai_don)
    if ll:
        flt["loai_don"] = ll[0] if len(ll) == 1 else {"$in": ll}

    docs  = list(db[COLLECTION].find(flt).sort("ngay_nop_don", -1).limit(50))
    total = db[COLLECTION].count_documents(flt)
    return json.dumps({
        "trang_thai": "Chờ phê duyệt", "is_hr": p["is_hr"],
        "total": total, "count": len(docs),
        "requests": [_flatten(d) for d in docs],
    }, ensure_ascii=False, default=str)


@mcp.tool()
def get_request_stats(
    session_id:   str,
    username:     str,
    year:         Optional[int] = None,
    month:        Optional[int] = None,
    don_vi_code:  Optional[str] = None,
    company_code: str           = "HITC",
) -> str:
    """
    Thống kê đơn từ theo loại và trạng thái. Dùng field ngay_nop_don để lọc tháng/năm.
    HR: cả công ty. NV: chỉ của mình.
    Kết quả: số đơn theo loại + tổng số ngày nghỉ (so_ngay_nghi) / ngày công tác (so_ngay_di_cong_tac).
    """
    p = _perm(session_id)
    if not p["ok"]:
        return json.dumps({"error": p["error"]}, ensure_ascii=False)

    db   = get_db()
    yr   = year or datetime.now().year
    mo   = month

    if mo:
        dt_from = datetime(yr, mo, 1) - timedelta(hours=7)
        dt_to   = (datetime(yr, mo + 1, 1) if mo < 12 else datetime(yr + 1, 1, 1)) - timedelta(hours=7, seconds=1)
    else:
        dt_from = datetime(yr, 1, 1) - timedelta(hours=7)
        dt_to   = datetime(yr, 12, 31, 23, 59, 59)

    flt: dict = {
        "is_deleted": {"$ne": True}, "company_code": company_code,
        "ngay_nop_don": {"$gte": dt_from, "$lte": dt_to},
    }
    if not p["is_hr"]:
        flt["nguoi_nop_don.value"] = username
    elif don_vi_code:
        flt["don_vi_cong_tac.value"] = don_vi_code

    pipeline = [
        {"$match": flt},
        {"$group": {
            "_id": {"loai_don": "$loai_don", "trang_thai": "$trang_thai_phe_duyet.value"},
            "count":     {"$sum": 1},
            "tong_ngay": {"$sum": "$so_ngay_nghi"},
            "tong_ct":   {"$sum": "$so_ngay_di_cong_tac"},
        }},
        {"$sort": {"_id.loai_don": 1}},
    ]
    rows  = list(db[COLLECTION].aggregate(pipeline))
    stats: dict = {}
    total = 0
    for row in rows:
        loai = row["_id"].get("loai_don", "Khác")
        tt   = row["_id"].get("trang_thai", "Không rõ")
        cnt  = row["count"]
        total += cnt
        if loai not in stats:
            stats[loai] = {"tong": 0, "theo_trang_thai": {}}
        stats[loai]["tong"] += cnt
        stats[loai]["theo_trang_thai"][tt] = cnt
        if row.get("tong_ngay", 0):
            stats[loai]["tong_ngay_nghi"] = stats[loai].get("tong_ngay_nghi", 0) + row["tong_ngay"]
        if row.get("tong_ct", 0):
            stats[loai]["tong_ngay_cong_tac"] = stats[loai].get("tong_ngay_cong_tac", 0) + row["tong_ct"]

    period = f"Tháng {mo}/{yr}" if mo else f"Năm {yr}"
    return json.dumps({
        "period": period, "is_hr": p["is_hr"],
        "total_all": total, "by_type": stats,
    }, ensure_ascii=False, default=str)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8018)