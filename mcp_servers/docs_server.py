"""
mcp_servers/docs_server.py
MCP Docs Server — RAG semantic search (Qdrant).

Tools (prefix: docs_):
  search_docs
"""
from __future__ import annotations

import json
import logging

from fastmcp import FastMCP

from utils.session import get_session_context
from utils.knowledge import search_knowledge
from utils.permission import UserPermissionContext

logger = logging.getLogger(__name__)
mcp    = FastMCP("modata-docs")


@mcp.tool()
def search_docs(session_id: str, query: str, top_k: int = 5) -> str:
    """Tìm kiếm tài liệu nội bộ, quy trình, quy định, hướng dẫn."""
    ctx = get_session_context(session_id)

    user = UserPermissionContext(
        user_id=session_id,
        username="",
        email="",
        roles=[],
        accessible_ma_chuc_nang=set(),
        accessible_instance_names=set(ctx.accessible_instance_names()),
    )

    chunks = search_knowledge(query=query, user=user, top_k=top_k)
    if not chunks:
        return "Không tìm thấy tài liệu liên quan."

    return json.dumps([
        {"score": round(c["score"], 3), "content": c["text"]}
        for c in chunks
    ], ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8013)