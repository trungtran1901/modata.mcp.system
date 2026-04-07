"""
mcp_servers/gateway.py  (v2)
MCP Gateway — gộp tất cả servers vào 1 endpoint SSE duy nhất.

Tools prefix:
  data_*     analytics_*     docs_*      tools_*
  mail_*     admin_*
  hrm_*      ← hrm_server.py           (nhân viên, ngày nghỉ lễ, quy định)
  hrm_req_*  ← hrm_request_server.py  (đơn từ: xin nghỉ, đi muộn, remote, công tác)
"""
from __future__ import annotations

import sys
import asyncio
import logging

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("mcp").setLevel(logging.DEBUG)
logging.getLogger("fastmcp").setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

from fastmcp import FastMCP

from mcp_servers.data_server        import mcp as data_mcp
from mcp_servers.analytics_server   import mcp as analytics_mcp
from mcp_servers.docs_server        import mcp as docs_mcp
from mcp_servers.tools_server       import mcp as tools_mcp
from mcp_servers.mail_server        import mcp as mail_mcp
from mcp_servers.admin_server       import mcp as admin_mcp
from mcp_servers.hrm_server         import mcp as hrm_mcp
from mcp_servers.hrm_request_server import mcp as hrm_req_mcp   # ← MỚI
from mcp_servers.hrm_attendance_server  import mcp as hrm_att_mcp

gateway = FastMCP("MODATA Gateway")

gateway.mount(data_mcp,      prefix="data")
gateway.mount(analytics_mcp, prefix="analytics")
gateway.mount(docs_mcp,      prefix="docs")
gateway.mount(tools_mcp,     prefix="tools")
gateway.mount(mail_mcp,      prefix="mail")
gateway.mount(admin_mcp,     prefix="admin")
gateway.mount(hrm_mcp,       prefix="hrm")         # hrm_*
gateway.mount(hrm_req_mcp,   prefix="hrm_req")     # hrm_req_* ← MỚI
gateway.mount(hrm_att_mcp,   prefix="hrm_att")     # hrm_att_* ← MỚI

if __name__ == "__main__":
    from app.core.config import settings
    logger.info("MCP Gateway starting → http://%s:%d/sse",
                settings.MCP_HOST, settings.MCP_PORT)
    gateway.run(transport="sse", host=settings.MCP_HOST, port=settings.MCP_PORT)