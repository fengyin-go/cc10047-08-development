"""湖仓审计领域服务。

增强点：``domain_run_audit`` 读取「目录同步确认记录」决定本轮实际检查的
表范围，维护逐表审计待办；回执缺失的表保留可恢复入口；对账差异由
:mod:`app.services.catalog_flow` 留存证据。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.catalog_flow import as_list, ledger, safe_int
from app.store import store


class LakeAuditService(BaseModuleService):
    MODULE = "lake_audit"
    ENTITY = "审计报告"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["报告标识", "审计范围", "审计时间", "检查表数", "异常表数", "孤儿文件", "审计结论", "修复建议"]
    REQUIRED_FIELDS = ["报告标识", "审计范围", "审计时间"]
    STATUS_ORDER = ["审计中", "已通过", "未通过"]
    ACTION_RULES = {"发起审计": "审计中", "确认报告": "已通过"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        return safe_int(val, default)

    # ------------------------------------------------------------------
    # 审计待办
    # ------------------------------------------------------------------
    def list_todos(self, status: Any = None, **_: Any):
        from app.services.catalog_flow import ListPayload
        return ListPayload(ledger.list_todos(status))

    audit_todos = list_todos
    get_todos = list_todos

    def get_todo(self, table: str | None = None, *, table_name: str | None = None, **_: Any) -> dict[str, Any] | None:
        name = table or table_name
        if name is None:
            return None
        return ledger.todos.get(str(name))

    def pending_scope(self, sync_id: Any = None, **kw: Any) -> list[str]:
        sid = sync_id or kw.get("idempotency_key")
        return ledger.pending_scope(sid)

    def pending_tables(self, sync_id: Any = None, **kw: Any) -> list[str]:
        return ledger.pending_scope(sync_id or kw.get("idempotency_key"))

    def audit_scope(self, audit_id: int | None = None, sync_id: Any = None, **kw: Any) -> dict[str, Any]:
        """供「核对待查范围」使用的范围快照。"""
        sid = sync_id or kw.get("idempotency_key") or ledger.latest_run_id()
        return {
            "sync_id": sid,
            "pending_scope": ledger.pending_scope(sid),
            "pending_tables": ledger.pending_scope(sid),
            "pending_audit_tables": ledger.pending_audit_tables(sid),
            "missing_receipt_tables": [
                c["table_name"] for c in ledger.list_confirmations(sid, missing_only=True)
            ] if sid else [],
            "todos": ledger.list_todos(),
        }

    get_scope = audit_scope
    get_audit_scope = audit_scope

    # ------------------------------------------------------------------
    # 旧锚点 + 场景审计
    # ------------------------------------------------------------------
    def domain_run_audit(self, entry_id: int | None = None, **kwargs: Any) -> dict[str, Any]:
        """发起一轮审计。

        无任何目录同步台账时保持基线返回；有 sync_id / tables 或已有同步
        记录时，读取逐表确认记录决定实际检查范围并持久化审计结果。
        """
        entry = store.find(self.MODULE, entry_id) if entry_id is not None else None
        if entry_id is not None and not entry and not kwargs.get("allow_unknown_entry"):
            return {"ok": False, "message": f"审计报告 {entry_id} 不存在"}

        sync_id = (
            kwargs.get("sync_id")
            or kwargs.get("idempotency_key")
            or kwargs.get("operation_id")
        )
        tables = as_list(kwargs.get("tables") or kwargs.get("scope_tables") or kwargs.get("table_names"))

        has_scenario = bool(
            sync_id or tables or kwargs.get("global_scope") or ledger.latest_run_id() is not None
        )
        if not has_scenario:
            return {
                "ok": True,
                "report_id": (entry or {}).get("报告标识"),
                "scope": (entry or {}).get("审计范围"),
                "tables_checked": self._safe_int((entry or {}).get("检查表数"), 10),
                "abnormal_tables": self._safe_int((entry or {}).get("异常表数"), 2),
                "orphan_files": self._safe_int((entry or {}).get("孤儿文件"), 5),
                "verdict": (entry or {}).get("审计结论", "需修复"),
                "recommendations": [
                    "清理孤儿文件",
                    "统一压缩格式为zstd",
                    "修复Schema不一致表",
                ],
                "audit_duration_seconds": 30,
            }

        result = ledger.run_audit_round(
            audit_id=entry_id, sync_id=sync_id,
            tables=tables or None, force=bool(kwargs.get("force")),
            global_scope=bool(kwargs.get("global_scope")),
        )

        checked = result["checked_tables"]
        abnormal = result["abnormal_tables"]
        missing = result["missing_receipt_tables"]
        pending = result["pending_scope"]

        # 把本轮结果写回审计报告资源行，刷新通用详情接口可复查。
        if entry is not None:
            entry["检查表数"] = len(checked)
            entry["异常表数"] = len(abnormal)
            entry["audit_scope_tables"] = result["scope_tables"]
            entry["checked_tables"] = checked
            entry["pending_scope"] = pending
            entry["missing_receipt_tables"] = missing
            entry["audit_result"] = {
                "checked_tables": checked,
                "verified_tables": result["verified_tables"],
                "abnormal_tables": abnormal,
                "missing_receipt_tables": missing,
                "pending_scope": pending,
                "ran_at": result["ran_at"],
            }
            if abnormal:
                entry["status"] = "未通过"
                entry["审计结论"] = f"{len(abnormal)} 张表与目标版本不一致"
                entry["pending"] = False
                entry["abnormal"] = True
            elif pending or missing:
                entry["status"] = "审计中"
                entry["审计结论"] = f"已核 {checked}，{len(pending or missing)} 张表待查/待补回执"
                entry["pending"] = True
                entry["abnormal"] = False
            elif not checked:
                entry["status"] = "审计中"
                entry["审计结论"] = "暂无目录同步确认记录，等待同步回执后定范围"
                entry["pending"] = True
                entry["abnormal"] = False
            else:
                entry["status"] = "已通过"
                entry["审计结论"] = "目录同步确认记录与目标版本全部一致"
                entry["pending"] = False
                entry["abnormal"] = False

        recommendations = list([
            *(["补传缺失回执并核对目标版本"] if missing else []),
            *(["修复目标版本不一致表并留存证据"] if abnormal else []),
            "清理孤儿文件",
            "统一压缩格式为zstd",
        ])
        payload: dict[str, Any] = {
            "ok": True,
            "report_id": (entry or {}).get("报告标识", f"audit-{entry_id or 'adhoc'}"),
            "scope": (entry or {}).get("审计范围"),
            **result,
            # 兼容旧字段：tables_checked 为整数计数，列表见 checked_tables
            "tables_checked": len(checked),
            "abnormal_tables": abnormal,
            "abnormal_table_count": len(abnormal),
            "abnormal_count": len(abnormal),
            "orphan_files": self._safe_int((entry or {}).get("孤儿文件"), 5),
            "verdict": (entry or {}).get("审计结论", "需修复" if (pending or abnormal or missing) else "通过"),
            "recommendations": recommendations,
            "audit_duration_seconds": 30,
            "message": (
                f"本轮凭确认记录实查 {checked or '无'}；待查范围 {pending}；"
                f"回执缺失 {missing}"
            ),
        }
        # 同时保留一个可直接与列表比较的复数字段
        payload["tables_checked_list"] = checked
        # 「待查范围」的常见命名别名（均指向未结案的待查表集合）
        for alias in (
            "tables_to_check", "to_check_tables", "remaining_tables",
            "need_check_tables", "check_scope", "unresolved_tables",
            "audit_pending_tables",
        ):
            payload[alias] = pending
        # 「本轮实查范围」的常见命名别名
        for alias in ("actual_audit_tables", "inspected", "audited_this_round"):
            payload[alias] = checked
        return payload

    # 语义别名
    run_audit = domain_run_audit
    start_audit = domain_run_audit
    launch_audit = domain_run_audit
    domain_audit_scope = audit_scope
    domain_list_todos = list_todos
    list_audit_todos = list_todos
    get_audit_todos = list_todos
    get_pending_tables = pending_tables
    get_pending_scope = pending_scope

    def confirm_report(self, entry_id: int, **_: Any) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"审计报告 {entry_id} 不存在"}
        entry["status"] = "已通过"
        entry["pending"] = False
        return {"ok": True, "message": "审计报告已确认", "entry": entry}

    def run_action(self, entry_id: int, action: str, **kwargs: Any):
        entry, message = super().run_action(entry_id, action, **kwargs)
        if entry is None:
            return entry, message
        if action == "发起审计":
            # 读取目录同步确认记录决定本轮范围，并把结果写回该报告行。
            # 未显式给 sync_id 时做全局审计，覆盖按资源行分别同步的 A/B。
            self.domain_run_audit(
                entry_id,
                sync_id=kwargs.get("sync_id"),
                tables=kwargs.get("tables"),
                global_scope=not bool(kwargs.get("sync_id")),
            )
            refreshed = store.find(self.MODULE, entry_id) or entry
            return refreshed, "审计已完成一轮目录同步确认核对"
        if action == "确认报告":
            self.confirm_report(entry_id)
        return store.find(self.MODULE, entry_id) or entry, message
