"""
utils/knowledge.py

Tìm kiếm Qdrant với permission filter.
Dùng bởi docs_server để RAG tài liệu nội bộ.
"""
from __future__ import annotations

import logging
from typing import Optional

import httpx
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchAny, MatchValue

from app.core.config import settings
from utils.permission import UserPermissionContext

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# EMBEDDER (lightweight — chỉ cần embed_query cho search)
# ─────────────────────────────────────────────────────────────

class _Embedder:
    def __init__(self):
        headers = {"Content-Type": "application/json"}
        if settings.EMBED_API_KEY:
            headers["Authorization"] = f"Bearer {settings.EMBED_API_KEY}"
        self._client = httpx.Client(
            base_url=settings.EMBED_BASE_URL,
            headers=headers,
            timeout=settings.EMBED_TIMEOUT,
        )

    def embed_query(self, query: str) -> list[float]:
        text = f"{settings.EMBED_QUERY_INSTRUCTION}{query}"
        r = self._client.post("/embeddings", json={
            "model": settings.EMBED_MODEL,
            "input": [text],
        })
        r.raise_for_status()
        return r.json()["data"][0]["embedding"]


_embedder: _Embedder | None = None


def _get_embedder() -> _Embedder:
    global _embedder
    if _embedder is None:
        _embedder = _Embedder()
    return _embedder


# ─────────────────────────────────────────────────────────────
# PERMISSION FILTER
# ─────────────────────────────────────────────────────────────

def _build_permission_filter(
    user: UserPermissionContext,
    app_module: Optional[str] = None,
) -> Filter:
    should = []

    if user.username:
        should.append(FieldCondition(
            key="allowed_users", match=MatchAny(any=[user.username])
        ))
    if user.nhan_vien_vai_tro:
        should.append(FieldCondition(
            key="allowed_vai_tro", match=MatchAny(any=user.nhan_vien_vai_tro)
        ))
    if user.don_vi_code:
        should.append(FieldCondition(
            key="allowed_don_vi", match=MatchAny(any=[user.don_vi_code])
        ))
    if user.don_vi_path:
        parts     = [p for p in user.don_vi_path.strip("/").split("/") if p]
        ancestors = ["/" + "/".join(parts[:i+1]) + "/" for i in range(len(parts))]
        should.append(FieldCondition(
            key="allowed_paths", match=MatchAny(any=ancestors)
        ))

    must = [
        FieldCondition(key="company_code", match=MatchValue(value=user.company_code))
    ]
    if app_module:
        must.append(FieldCondition(
            key="app_module", match=MatchValue(value=app_module)
        ))
    if user.accessible_ma_chuc_nang:
        must.append(FieldCondition(
            key="ma_chuc_nang",
            match=MatchAny(any=list(user.accessible_ma_chuc_nang)),
        ))

    return Filter(must=must, should=should or None)


# ─────────────────────────────────────────────────────────────
# SEARCH
# ─────────────────────────────────────────────────────────────

def search_knowledge(
    query: str,
    user: UserPermissionContext,
    app_module: Optional[str] = None,
    top_k: Optional[int] = None,
) -> list[dict]:
    client = QdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY or None)
    k      = top_k or settings.RAG_TOP_K

    vector      = _get_embedder().embed_query(query)
    perm_filter = _build_permission_filter(user, app_module)

    hits = client.query_points(
        collection_name=settings.QDRANT_COLLECTION,
        query=vector,
        query_filter=perm_filter,
        limit=k,
        score_threshold=settings.RAG_SCORE_THRESHOLD,
        with_payload=True,
    ).points

    return [
        {
            "score":         h.score,
            "text":          h.payload.get("text", ""),
            "doc_id":        h.payload.get("doc_id", ""),
            "instance_name": h.payload.get("instance_name", ""),
            "group_name":    h.payload.get("group_name", ""),
            "ma_chuc_nang":  h.payload.get("ma_chuc_nang", ""),
        }
        for h in hits
    ]
