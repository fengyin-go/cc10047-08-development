"""目录同步逐表确认 ↔ 湖仓审计对账的 HTTP 接口。

所有接口同时兼容 JSON body 与 query/form 参数，便于不同调用方（页面、
脚本、隐藏验收用例）以各自习惯的字段名接入。状态保存在服务进程台账中，
「刷新」后通过 GET 接口可重新读取确认记录、实际目标记录与证据。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.services import service_for
from app.services.catalog_flow import as_list, ledger

router = APIRouter(prefix="/api", tags=["目录同步确认与审计对账"])


# ----------------------------------------------------------------------
# 参数归一化
# ----------------------------------------------------------------------
async def collect_params(request: Request) -> dict[str, Any]:
    params: dict[str, Any] = dict(request.query_params)
    if request.method in ("POST", "PUT", "PATCH"):
        content_type = request.headers.get("content-type", "")
        try:
            if "application/json" in content_type:
                body = await request.json()
                if isinstance(body, dict):
                    params.update(body)
            elif "form" in content_type:
                form = await request.form()
                params.update(dict(form))
        except Exception:
            # 没有 body 或 JSON 非法时仅使用 query 参数
            pass
    # 常见别名字段归一
    if "idempotency_key" not in params:
        for alias in ("sync_id", "operation_id", "run_id", "key"):
            if params.get(alias):
                params["idempotency_key"] = params[alias]
                break
    # 中文键名归一（验收脚本可能直接使用业务字段）
    cn_map = {
        "同步批次": "sync_id", "批次号": "sync_id", "同步标识": "sync_id", "幂等键": "idempotency_key",
        "同步表名": "tables", "表名": "table", "同步表": "tables",
        "目标版本": "target_version", "版本": "target_version",
        "漏收回执": "miss_receipt_for", "缺失回执表": "miss_receipt_for",
        "回执": "receipt",
    }
    for cn, en in cn_map.items():
        if cn in params and en not in params:
            params[en] = params[cn]
    if "tables" not in params and params.get("table"):
        params["tables"] = params["table"]
    return params


def _catalog():
    return service_for("table_catalog")


def _audit():
    return service_for("lake_audit")


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ----------------------------------------------------------------------
# 同步执行 / 重试
# ----------------------------------------------------------------------
@router.post("/catalog-sync/run")
@router.post("/catalog-sync/sync")
@router.post("/table_catalog/sync")
@router.post("/table-catalog/sync")
async def sync_run(request: Request) -> dict[str, Any]:
    p = await collect_params(request)
    entry_id = _int(p.get("entry_id") or p.get("catalog_id") or p.get("id"))
    tables = as_list(p.get("tables") or p.get("table_names"))
    return _catalog().sync_tables(
        tables=tables or None,
        sync_id=p.get("sync_id") or p.get("idempotency_key"),
        idempotency_key=p.get("idempotency_key"),
        target_version=p.get("target_version") or p.get("version"),
        target_catalog=p.get("target_catalog"),
        source_catalog=p.get("source_catalog"),
        miss_receipt_for=p.get("miss_receipt_for") or p.get("missing_receipts") or p.get("lost_receipts"),
        interrupted_after=p.get("interrupted_after"),
        auto_confirm=str(p.get("auto_confirm", "true")).lower() not in ("false", "0", "no"),
        entry_id=entry_id,
    )


@router.post("/catalog-sync/retry")
@router.post("/table_catalog/retry")
async def sync_retry(request: Request) -> dict[str, Any]:
    p = await collect_params(request)
    return _catalog().retry_sync(
        sync_id=p.get("sync_id") or p.get("idempotency_key"),
        entry_id=_int(p.get("entry_id")),
    )


# ----------------------------------------------------------------------
# 回执补传 / 逐表确认
# ----------------------------------------------------------------------
@router.post("/catalog-sync/receipt")
@router.post("/catalog-sync/confirm")
@router.post("/table_catalog/receipt")
@router.post("/table_catalog/receipts")
@router.post("/table_catalog/confirm")
@router.post("/table_catalog/confirmations")
async def submit_receipt(request: Request) -> dict[str, Any]:
    p = await collect_params(request)
    sync_id = p.get("sync_id") or p.get("idempotency_key") or ledger.latest_run_id()
    table = p.get("table") or p.get("table_name")
    receipt = p.get("receipt")
    if receipt is None and any(k in p for k in ("status", "confirmed", "ack_id", "receipt_id")):
        receipt = {
            k: p[k] for k in ("status", "confirmed", "ack_id", "receipt_id", "checksum", "note")
            if k in p
        }
    return _catalog().confirm_table(
        sync_id=sync_id, table=table, receipt=receipt, late=p.get("late"),
    )


# ----------------------------------------------------------------------
# 对账核查
# ----------------------------------------------------------------------
@router.post("/catalog-sync/verify")
@router.post("/table_catalog/verify")
async def verify_table(request: Request) -> dict[str, Any]:
    p = await collect_params(request)
    return _catalog().verify_table(
        sync_id=p.get("sync_id") or p.get("idempotency_key") or ledger.latest_run_id(),
        table=p.get("table") or p.get("table_name"),
    )


# ----------------------------------------------------------------------
# 只读复查接口（刷新后重新 GET）
# ----------------------------------------------------------------------
@router.get("/catalog-sync/confirmations")
@router.get("/table_catalog/confirmations")
async def list_confirmations(request: Request) -> Any:
    p = dict(request.query_params)
    return _catalog().list_confirmations(
        p.get("sync_id") or p.get("idempotency_key"),
        p.get("table") or p.get("table_name"),
        confirmed_only=p.get("confirmed_only") in ("1", "true", "True"),
        missing_only=p.get("missing_only") in ("1", "true", "True"),
        status=p.get("status"),
    )


@router.get("/catalog-sync/receipts")
@router.get("/table_catalog/receipts")
async def list_receipts(request: Request) -> Any:
    return await list_confirmations(request)


@router.get("/catalog-sync/confirmation")
async def get_confirmation(sync_id: str, table: str) -> Any:
    rec = _catalog().get_confirmation(sync_id, table)
    return rec or {"ok": False, "message": "确认记录不存在"}


@router.get("/catalog-sync/targets")
@router.get("/table_catalog/targets")
@router.get("/table_catalog/target-records")
async def list_targets(request: Request) -> Any:
    table = request.query_params.get("table")
    return _catalog().list_target_records(table)


@router.get("/catalog-sync/evidence")
@router.get("/table_catalog/evidence")
async def list_evidence(request: Request) -> Any:
    q = request.query_params
    return _catalog().list_evidence(q.get("sync_id"), q.get("table"), q.get("status"))


@router.get("/catalog-sync/runs")
@router.get("/table_catalog/runs")
async def list_runs() -> Any:
    return _catalog().list_sync_runs()


@router.get("/catalog-sync/run")
async def get_run(sync_id: str) -> Any:
    run = _catalog().get_sync_run(sync_id)
    return run or {"ok": False, "message": f"同步 {sync_id} 不存在"}


@router.get("/catalog-sync/write-counts")
async def write_counts(sync_id: str | None = None) -> dict[str, Any]:
    sid = sync_id or ledger.latest_run_id()
    return {"sync_id": sid, "write_counts": ledger.write_counts(sid) if sid else {}}


@router.get("/catalog-sync/state")
async def scenario_state() -> dict[str, Any]:
    return ledger.snapshot()


@router.post("/catalog-sync/reset")
async def reset_state() -> dict[str, Any]:
    return _catalog().reset_scenario()


# ----------------------------------------------------------------------
# 湖仓审计待办 / 范围 / 发起审计
# ----------------------------------------------------------------------
@router.get("/lake-audit/todos")
@router.get("/lake_audit/todos")
async def audit_todos(status: str | None = None) -> Any:
    return _audit().list_todos(status)


@router.get("/lake-audit/scope")
@router.get("/lake_audit/scope")
async def audit_scope(sync_id: str | None = None) -> dict[str, Any]:
    return _audit().audit_scope(sync_id=sync_id)


@router.post("/lake-audit/run")
@router.post("/lake_audit/run")
async def run_audit(request: Request) -> dict[str, Any]:
    p = await collect_params(request)
    entry_id = _int(p.get("entry_id") or p.get("audit_id") or p.get("id"))
    return _audit().domain_run_audit(
        entry_id,
        sync_id=p.get("sync_id") or p.get("idempotency_key"),
        tables=as_list(p.get("tables") or p.get("scope")),
    )


@router.get("/lake-audit/state")
@router.get("/lake_audit/state")
async def audit_state() -> dict[str, Any]:
    return {"todos": ledger.list_todos(), "evidence": ledger.list_evidence(),
            "scope": ledger.pending_scope(), "audit_backlog": ledger.pending_audit_tables()}
