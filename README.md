# MODATA.MCP — MCP Gateway

MCP (Model Context Protocol) Gateway độc lập — cung cấp tools cho AI Agent truy cập
dữ liệu nội bộ qua MongoDB, PostgreSQL, Qdrant và SMTP.

Project này **không** bao gồm API, Agent hay embedding pipeline.
Dùng kết hợp với project `modata-rag` hoặc bất kỳ MCP client nào hỗ trợ SSE transport.

## Stack

| Thành phần | Vai trò |
|---|---|
| FastMCP | MCP server framework |
| MongoDB | Nguồn dữ liệu nghiệp vụ |
| PostgreSQL | Session store (kiểm tra quyền) |
| Qdrant | Vector search cho RAG |
| Remote Embed server | Embed query cho docs_server |
| SMTP | Gửi email |

## Cấu trúc

```
modata-mcp/
├── app/
│   ├── core/config.py          # Toàn bộ config đọc từ .env
│   └── db/mongo.py             # Singleton MongoDB client
├── mcp_servers/
│   ├── gateway.py              # Entry point — mount tất cả servers, SSE port 8001
│   ├── data_server.py          # prefix: data_*
│   ├── analytics_server.py     # prefix: analytics_*
│   ├── docs_server.py          # prefix: docs_*
│   ├── tools_server.py         # prefix: tools_*
│   └── mail_server.py          # prefix: mail_*
├── utils/
│   ├── permission.py           # UserPermissionContext dataclass
│   ├── session.py              # get_accessible() — đọc quyền từ PostgreSQL
│   └── knowledge.py           # Qdrant search + permission filter
├── Dockerfile
├── docker-compose.yml
├── docker-compose.dev.yml
├── .env.example
├── run.py
└── requirements.txt
```

## Tools

| Prefix | Tool | Mô tả |
|---|---|---|
| `data_` | `list_accessible_collections` | Danh sách collections user có quyền |
| `data_` | `get_schema` | Schema fields của collection |
| `data_` | `query_collection` | Query MongoDB với filter |
| `data_` | `find_one` | Lấy 1 record |
| `analytics_` | `count` | Đếm records theo điều kiện |
| `analytics_` | `aggregate` | Chạy aggregation pipeline |
| `analytics_` | `group_by_field` | Thống kê phân bổ theo field |
| `analytics_` | `compare_periods` | So sánh 2 khoảng thời gian |
| `docs_` | `search_docs` | Semantic search tài liệu nội bộ (RAG) |
| `tools_` | `search_employee_by_name` | Tìm nhân viên theo tên/mã |
| `tools_` | `get_current_time` | Thời gian hiện tại |
| `tools_` | `calculate_working_days` | Số ngày làm việc giữa 2 ngày |
| `tools_` | `calculate_service_time` | Thời gian công tác từ ngày bắt đầu |
| `tools_` | `lookup_danhmuc` | Tra cứu danh mục hệ thống |
| `tools_` | `get_org_tree` | Cây tổ chức theo path |
| `mail_` | `send_email` | Gửi email cá nhân |
| `mail_` | `send_email_to_team` | Gửi email cho cả đơn vị |
| `mail_` | `lookup_email_by_name` | Tra cứu email theo tên |

## Khởi động nhanh

### 1. Chuẩn bị .env

```bash
cp .env.example .env
# Chỉnh sửa .env với thông tin thực tế
```

### 2. Docker (khuyến nghị)

```bash
# Production
docker compose up -d --build

# Development (hot reload)
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build

# Xem logs
docker compose logs -f

# Dừng
docker compose down
```

### 3. Local

```bash
pip install -r requirements.txt
python run.py
```

Gateway chạy tại: `http://localhost:8001/sse`

## Kết nối từ AI Agent

```python
from agno.tools.mcp import MCPTools

mcp_tools = MCPTools(
    transport="sse",
    url="http://localhost:8001/sse",
)
await mcp_tools.connect()
```

## Cơ chế phân quyền

Mỗi tool nhận `session_id` và tra PostgreSQL (`rag_sessions.accessible_instance_names`)
để kiểm tra quyền truy cập collection — không inject danh sách collections vào prompt.

```
Request → Tool(session_id) → PostgreSQL.rag_sessions → accessible_instance_names → allow/deny
```
