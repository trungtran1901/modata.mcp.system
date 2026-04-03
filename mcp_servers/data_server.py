"""
mcp_servers/data_server.py
MCP Data Server — query MongoDB với view-based field permission.

Tối ưu result:
  - _flatten_doc(): convert raw MongoDB doc → {display_name: human_value}
    dùng schema field list — AI không thấy raw field kỹ thuật
  - search_records(): searchable fields 100% load từ schema (type String/Text/Email)
    không còn priority list hardcode
  - Tất cả result đều kèm schema_fields để AI hiểu ngữ nghĩa

Tools (prefix: data_):
  list_accessible_collections
  get_schema
  query_collection
  find_one
  search_records
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastmcp import FastMCP

from app.db.mongo import get_db
from utils.session import get_session_context
from utils.schema_cache import SchemaInfo, FieldInfo, get_schema_info

logger = logging.getLogger(__name__)
mcp    = FastMCP("modata-data")


# ─────────────────────────────────────────────────────────────
# VALUE EXTRACTOR
# ─────────────────────────────────────────────────────────────

def _extract_value(v: Any, field_type: str = "String") -> Any:
    """
    Chuyển giá trị MongoDB sang dạng human-readable.

    Các kiểu phổ biến trong hệ thống:
      - Lookup / Select  : {"label": "...", "value": "..."}  → trả label
      - MultiSelect/List : [{"label": "...", "value": "..."}, ...]  → list labels
      - ObjectId/datetime: str(v)
      - Primitive        : giữ nguyên
    """
    if v is None:
        return None

    if isinstance(v, dict):
        # Lookup field: ưu tiên label, fallback value
        label = v.get("label") or v.get("value")
        if label is not None:
            return label
        # Dict khác (vd address object): serialize gọn
        return {k: _extract_value(sub) for k, sub in v.items()
                if not k.startswith("_") and k not in ("data_source", "view_to_open_link", "option")}

    if isinstance(v, list):
        items = [_extract_value(i) for i in v]
        # Lọc None, flatten nếu chỉ có 1 phần tử
        items = [i for i in items if i is not None]
        return items if len(items) != 1 else items[0]

    if type(v).__name__ in ("ObjectId", "datetime"):
        return str(v)

    return v


def _flatten_doc(doc: dict, schema: SchemaInfo) -> dict:
    """
    Convert raw MongoDB document → ordered dict {display_name: human_value}.

    - Chỉ giữ fields trong schema (đã qua view permission filter)
    - Key = display name (tiếng Việt, dễ đọc cho AI)
    - Value = human-readable (label thay vì {label, value, data_source...})
    - Giữ thứ tự theo schema index
    - Bỏ qua field có giá trị None/empty
    """
    result = {}
    for field in schema.fields:           # đã sort theo index
        raw = doc.get(field.name)
        val = _extract_value(raw, field.type)
        if val is None or val == "" or val == []:
            continue
        result[field.display] = val
    return result


def _flatten_docs(docs: list[dict], schema: SchemaInfo) -> list[dict]:
    return [_flatten_doc(d, schema) for d in docs]


# Giới hạn JSON output trả về LLM — tránh vượt context window
# ~4,000 chars ≈ 1,200 tokens — đủ thông tin, không overflow
_MAX_RESULT_CHARS = 2_000

def _trim_result(data: dict) -> str:
    """Serialize + truncate nếu vượt ngưỡng."""
    s = json.dumps(data, ensure_ascii=False, default=str)
    if len(s) <= _MAX_RESULT_CHARS:
        return s
    # Cắt bớt records từ cuối, giữ metadata
    records = data.get("records", [])
    while records and len(s) > _MAX_RESULT_CHARS:
        records = records[:-1]
        data = {**data, "records": records,
                "truncated": True,
                "note": "Kết quả bị cắt bớt do giới hạn context. Dùng filter cụ thể hơn hoặc giảm limit."}
        s = json.dumps(data, ensure_ascii=False, default=str)
    return s


# ─────────────────────────────────────────────────────────────
# SESSION → SCHEMA
# ─────────────────────────────────────────────────────────────

def _get_schema_for_session(session_id: str, instance_name: str) -> SchemaInfo | None:
    """
    Lấy SchemaInfo merged từ tất cả ma_chuc_nang user có trên instance_name.
    Union fields nếu user có nhiều quyền — dùng Redis cache.
    """
    ctx     = get_session_context(session_id)
    ma_list = ctx.get_ma_chuc_nang_list(instance_name)
    if not ma_list:
        return None

    if len(ma_list) == 1:
        return get_schema_info(instance_name, ma_list[0])

    # Nhiều ma_chuc_nang → merge
    seen: dict[str, FieldInfo] = {}
    display_name = instance_name
    for ma in ma_list:
        info = get_schema_info(instance_name, ma)
        display_name = info.display_name
        for f in info.fields:
            if f.name not in seen or f.index < seen[f.name].index:
                seen[f.name] = f

    return SchemaInfo(instance_name, display_name,
                      sorted(seen.values(), key=lambda x: x.index))


# ─────────────────────────────────────────────────────────────
# TOOLS
# ─────────────────────────────────────────────────────────────

@mcp.tool()
def list_accessible_collections(session_id: str) -> str:
    """Danh sách collections có quyền truy cập."""
    ctx    = get_session_context(session_id)
    names  = ctx.accessible_instance_names()
    db     = get_db()
    result = []

    # Chỉ trả instance_name — LLM không cần display_name để gọi tool tiếp
    # display_name chỉ cần khi hiển thị cho user, không phải cho AI routing
    return json.dumps(names, ensure_ascii=False)


@mcp.tool()
def get_schema(session_id: str, instance_name: str) -> str:
    """Fields user được phép xem. Dùng trước khi query."""
    ctx = get_session_context(session_id)
    if not ctx.can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})

    schema = _get_schema_for_session(session_id, instance_name)
    if not schema:
        return json.dumps({"error": "Không tìm thấy schema hoặc không có quyền."})

    return json.dumps(schema.to_dict(), ensure_ascii=False)


@mcp.tool()
def query_collection(
    session_id:    str,
    instance_name: str,
    filter:        dict      = None,
    fields:        list[str] = None,
    limit:         int       = 5,
    skip:          int       = 0,
) -> str:
    """Truy vấn MongoDB. filter dùng field_name kỹ thuật, result flatten theo display_name."""
    ctx = get_session_context(session_id)
    if not ctx.can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})

    schema = _get_schema_for_session(session_id, instance_name)
    if not schema or not schema.fields:
        return json.dumps({"error": "Không có field nào trong quyền truy cập."})

    db         = get_db()
    flt        = dict(filter) if filter else {}
    flt["is_deleted"] = {"$ne": True}
    projection = schema.allowed_projection(fields)
    limit      = max(1, min(limit, 50))

    col   = db[f"instance_data_{instance_name}"]
    docs  = list(col.find(flt, projection).skip(skip).limit(limit))
    total = col.count_documents(flt)

    return _trim_result({
        "collection": schema.display_name,
        "total":      total,
        "count":      len(docs),
        "records":    _flatten_docs(docs, schema),
    })


@mcp.tool()
def find_one(
    session_id:    str,
    instance_name: str,
    filter:        dict      = None,
    fields:        list[str] = None,
) -> str:
    """
    Lấy 1 record theo điều kiện. Result được flatten theo display_name.
    """
    ctx = get_session_context(session_id)
    if not ctx.can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})

    schema = _get_schema_for_session(session_id, instance_name)
    if not schema or not schema.fields:
        return json.dumps({"error": "Không có field nào trong quyền truy cập."})

    db         = get_db()
    flt        = dict(filter) if filter else {}
    flt["is_deleted"] = {"$ne": True}
    projection = schema.allowed_projection(fields)

    doc = db[f"instance_data_{instance_name}"].find_one(flt, projection)
    if not doc:
        return json.dumps({"error": "Không tìm thấy dữ liệu."})

    return json.dumps(_flatten_doc(doc, schema), ensure_ascii=False, default=str)


@mcp.tool()
def search_records(
    session_id:    str,
    instance_name: str,
    keyword:       str,
    fields:        list[str] = None,
    limit:         int       = 10,
) -> str:
    """Tìm kiếm full-text trên mọi collection. Tự chọn searchable fields từ schema."""
    ctx = get_session_context(session_id)
    if not ctx.can_access(instance_name):
        return json.dumps({"error": "Bạn không có quyền truy cập collection này."})

    schema = _get_schema_for_session(session_id, instance_name)
    if not schema or not schema.fields:
        return json.dumps({"error": "Không có field nào trong quyền truy cập."})

    # ── Searchable fields: load 100% từ schema, không hardcode ──
    # Chỉ search trên String/Text/Email — không search Date, Number, Boolean
    _SEARCHABLE_TYPES = {"String", "Text", "Email", "Phone"}

    searchable = [
        f.name for f in schema.fields
        if f.type in _SEARCHABLE_TYPES
    ]

    # Giới hạn 10 fields để tránh query MongoDB nặng
    searchable = searchable[:10]

    if not searchable:
        return json.dumps({
            "error":      "Không có searchable field (String/Text/Email) trong schema này.",
            "suggestion": "Dùng query_collection với filter cụ thể.",
            "available_fields": [f.to_dict() for f in schema.fields],
        })

    # ── MongoDB $or regex ─────────────────────────────────────
    # Với Lookup field (type String nhưng value lưu trong .value):
    # search cả field thẳng và field.value để cover cả 2 trường hợp
    or_conditions = []
    for fname in searchable:
        or_conditions.append({fname: {"$regex": keyword, "$options": "i"}})
        # Thêm .value nếu là field dạng lookup (tên field không có dấu chấm)
        if "." not in fname:
            or_conditions.append({f"{fname}.value": {"$regex": keyword, "$options": "i"}})

    flt: dict = {
        "is_deleted": {"$ne": True},
        "$or":        or_conditions,
    }

    projection = schema.allowed_projection(fields)
    db         = get_db()
    col        = db[f"instance_data_{instance_name}"]
    limit      = max(1, min(limit, 50))

    docs  = list(col.find(flt, projection).limit(limit))
    total = col.count_documents(flt)

    return _trim_result({
        "collection": schema.display_name,
        "keyword":    keyword,
        "total":      total,
        "count":      len(docs),
        "records":    _flatten_docs(docs, schema),
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(mcp.http_app(), host="0.0.0.0", port=8011)