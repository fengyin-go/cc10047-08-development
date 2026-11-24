"""兼容性别名模块：湖仓审计待办与目录同步范围对账。"""
from __future__ import annotations

from app.services.catalog_flow import CatalogAuditLedger, ledger  # noqa: F401
from app.services.lake_audit import LakeAuditService  # noqa: F401

audit_ledger = ledger
todo_ledger = ledger
audit_todo_store = ledger
AuditTodoService = LakeAuditService
