"""
utils/permission.py

UserPermissionContext — dataclass mô tả quyền của người dùng.
Được dùng bởi docs_server để truyền context vào search_knowledge.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class UserPermissionContext:
    # Từ Keycloak JWT (truyền qua MCP tool params)
    user_id:  str
    username: str
    email:    str
    roles:    list[str]

    # Từ collection nhan_vien
    company_code:      str           = "HITC"
    don_vi_code:       str           = ""
    don_vi_path:       str           = ""
    vi_tri_cong_viec:  Optional[str] = None
    nhan_vien_vai_tro: list[str]     = field(default_factory=list)

    # Kết quả phân quyền
    accessible_ma_chuc_nang:   set[str] = field(default_factory=set)
    accessible_instance_names: set[str] = field(default_factory=set)
