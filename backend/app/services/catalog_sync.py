"""兼容性别名模块：目录同步幂等写入/重试引擎。"""
from __future__ import annotations

from app.services.catalog_flow import (  # noqa: F401
    CatalogAuditLedger,
    fingerprint,
    ledger,
)
from app.services.table_catalog import TableCatalogService  # noqa: F401

sync_ledger = ledger
idempotency_ledger = ledger
CatalogSyncService = TableCatalogService
