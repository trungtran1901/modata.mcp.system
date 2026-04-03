"""
mcp_servers/mail_server.py
MCP Mail Server — gửi email qua SMTP.

Tools (prefix: mail_):
  send_email  send_email_to_team  lookup_email_by_name
"""
from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor

from fastmcp import FastMCP

from app.core.config import settings
from app.db.mongo import get_db

logger    = logging.getLogger(__name__)
mcp       = FastMCP("modata-mail")
_executor = ThreadPoolExecutor(max_workers=4)


# ─────────────────────────────────────────────────────────────
# SMTP HELPERS
# ─────────────────────────────────────────────────────────────

def _send_smtp_sync(
    to: list[str],
    subject: str,
    body: str,
    cc: list[str] = None,
    is_html: bool = False,
    sender_name: str = "MODATA AI System",
) -> dict:
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.utils import formataddr

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = formataddr((sender_name, settings.MAIL_FROM))
    msg["To"]      = ", ".join(to)
    if cc:
        msg["Cc"]  = ", ".join(cc)
    msg.attach(MIMEText(body, "html" if is_html else "plain", "utf-8"))

    recipients = to + (cc or [])
    smtp_cls   = smtplib.SMTP_SSL if settings.MAIL_PORT == 465 else smtplib.SMTP
    with smtp_cls(settings.MAIL_HOST, settings.MAIL_PORT, timeout=30) as smtp:
        if settings.MAIL_PORT != 465 and settings.MAIL_USE_TLS:
            smtp.starttls()
        if settings.MAIL_USERNAME and settings.MAIL_PASSWORD:
            smtp.login(settings.MAIL_USERNAME, settings.MAIL_PASSWORD)
        smtp.sendmail(settings.MAIL_FROM, recipients, msg.as_string())

    return {"status": "sent", "to": to, "cc": cc or [], "subject": subject}


async def _send_smtp(to, subject, body, cc=None, is_html=False, sender_name="MODATA AI System") -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        _executor,
        lambda: _send_smtp_sync(to, subject, body, cc, is_html, sender_name),
    )


def _lookup_email(username_or_email: str) -> str | None:
    if "@" in username_or_email:
        return username_or_email
    nv = get_db()["instance_data_thong_tin_nhan_vien"].find_one(
        {"ten_dang_nhap": username_or_email, "is_deleted": {"$ne": True}},
        {"email": 1},
    )
    return nv.get("email") if nv else None


# ─────────────────────────────────────────────────────────────
# TOOLS
# ─────────────────────────────────────────────────────────────

@mcp.tool()
async def send_email(
    to: list[str],
    subject: str,
    body: str,
    cc: list[str] = None,
    is_html: bool = False,
    sender_display_name: str = "MODATA AI System",
) -> str:
    """Gửi email. to: list email hoặc username nội bộ."""
    try:
        resolved_to = []
        for r in to:
            email = _lookup_email(r)
            if not email:
                return json.dumps({"status": "error", "message": f"Không tìm thấy email: {r}"})
            resolved_to.append(email)

        resolved_cc = [e for r in (cc or []) if (e := _lookup_email(r))]
        result = await _send_smtp(
            to=resolved_to, subject=subject, body=body,
            cc=resolved_cc or None, is_html=is_html,
            sender_name=sender_display_name,
        )
        logger.info("Email sent to %s — %s", resolved_to, subject)
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        logger.error("Send email error: %s", e)
        return json.dumps({"status": "error", "message": str(e)})


@mcp.tool()
async def send_email_to_team(
    don_vi_path: str,
    subject: str,
    body: str,
    is_html: bool = False,
    trang_thai: str = "Đang làm việc",
    company_code: str = "HITC",
) -> str:
    """Gửi email cho cả đơn vị. don_vi_path: /HTC/TTCNTT/ hoặc mã TTCNTT."""
    try:
        db = get_db()

        if "/" in don_vi_path:
            path_filter = {"$or": [
                {"path_don_vi_cong_tac": don_vi_path},
                {"path_don_vi_cong_tac": {"$regex": f"^{don_vi_path}", "$options": "i"}},
            ]}
        else:
            path_filter = {"$or": [
                {"phong_ban_phu_trach.value":   don_vi_path},
                {"don_vi_cong_tac.option.code": don_vi_path},
                {"phong_cap_2.value":            don_vi_path},
                {"phong_cap_1.value":            don_vi_path},
            ]}

        flt = {"is_deleted": {"$ne": True}, "company_code": company_code, **path_filter}
        if trang_thai:
            flt["trang_thai_lao_dong.value"] = trang_thai

        emails = [
            nv["email"]
            for nv in db["instance_data_thong_tin_nhan_vien"].find(flt, {"email": 1})
            if nv.get("email")
        ]

        if not emails:
            return json.dumps({
                "status":  "error",
                "message": f"Không có nhân viên tại: {don_vi_path}",
            }, ensure_ascii=False)

        sent = 0
        for i in range(0, len(emails), 50):
            await _send_smtp(to=emails[i:i+50], subject=subject, body=body, is_html=is_html)
            sent += len(emails[i:i+50])

        logger.info("Email sent to %d employees at %s", sent, don_vi_path)
        return json.dumps({
            "status":          "sent",
            "recipient_count": sent,
            "subject":         subject,
            "don_vi_path":     don_vi_path,
        }, ensure_ascii=False)
    except Exception as e:
        logger.error("Send team email error: %s", e, exc_info=True)
        return json.dumps({"status": "error", "message": str(e)}, ensure_ascii=False)


@mcp.tool()
def lookup_email_by_name(name: str, company_code: str = "HITC") -> str:
    """Tra cứu email của nhân viên theo tên hoặc username."""
    db  = get_db()
    flt = {
        "is_deleted":   {"$ne": True},
        "company_code": company_code,
        "$or": [
            {"ho_va_ten":        {"$regex": name, "$options": "i"}},
            {"ho_va_ten_co_dau": {"$regex": name, "$options": "i"}},
            {"ten_dang_nhap":    {"$regex": name, "$options": "i"}},
        ],
    }
    docs = list(db["instance_data_thong_tin_nhan_vien"].find(
        flt,
        {"ten_dang_nhap": 1, "ho_va_ten_co_dau": 1, "ho_va_ten": 1, "email": 1},
    ).limit(10))
    return json.dumps([
        {
            "username":  d.get("ten_dang_nhap"),
            "ho_va_ten": d.get("ho_va_ten_co_dau") or d.get("ho_va_ten"),
            "email":     d.get("email"),
        }
        for d in docs
    ], ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8015)