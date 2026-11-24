"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import catalog_flow, resource, chains

# catalog_flow 含 /api/table_catalog/confirmations 等具体子路径，
# 必须排在 resource 的 /api/{module}/{entry_id} 之前注册。
ROUTERS = [catalog_flow, resource, chains]
