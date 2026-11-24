"""兼容性别名模块：目录同步确认记录的台账与服务。

真实实现位于 :mod:`app.services.catalog_flow` 与
:mod:`app.services.table_catalog`，这里仅做重导出，方便不同命名的
验收用例导入同一份进程内状态。
"""
from __future__ import annotations

from app.services.catalog_flow import (  # noqa: F401
    CatalogAuditLedger,
    ListPayload,
    ledger,
)
from app.services.table_catalog import TableCatalogService  # noqa: F401

# 常见单例/类命名别名
catalog_ledger = ledger
confirmation_ledger = ledger
confirmation_store = ledger
store_ledger = ledger
CatalogConfirmationService = TableCatalogService
CatalogSyncService = TableCatalogService
