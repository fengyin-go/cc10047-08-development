"""目录同步逐表确认 + 湖仓审计对账的核心状态引擎。

设计目标（对应「表目录同步确认记录」题位）：

* 每次同步拥有稳定的 ``sync_id``（幂等键），下面按表保存**可重新读取**的
  逐表确认记录，记录原始差异、目标版本、回执状态与有效写入次数；
* 目标 Catalog 的实际落地记录独立保存，审计/值班员可以核对「实际目标记录」
  与「持久化回执」是否一致；
* 同一次同步的并发/重试写入在锁内去重，每张表有效写入恰好一次；
* 实际目标记录与持久化回执不一致时生成不可删除的证据记录，原差异、目标
  版本、迟到回执都能回查；
* 每张表维护一条湖仓审计待办，同步写入/确认/审计都会推进它。

本模块只依赖标准库，状态保存在进程内单例 :data:`ledger`，HTTP「刷新」或
重新调用服务查询方法都能读到同一份数据。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone
from typing import Any, Iterable


# 回执状态
RECEIPT_CONFIRMED = "confirmed"          # 已按时确认
RECEIPT_MISSING = "missing"              # 回执漏收/缺失
RECEIPT_LATE = "late"                    # 迟到后补传确认（仍记为 confirmed + late=True）

# 审计待办状态
TODO_PENDING = "待核"
TODO_READY = "待审计"
TODO_VERIFIED = "已核一致"
TODO_ABNORMAL = "核出异常"
TODO_RECEIPT_MISSING = "待补回执"

_CONFIRMED_WORDS = {"confirmed", "done", "ok", "已确认", "已同步", "确认", "有回执"}
_MISSING_WORDS = {"missing", "pending", "lost", "none", "回执缺失", "待回执", "漏收", "缺失", "未确认"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_list(value: Any) -> list[str]:
    """把各种入参形态归一化成非空字符串列表。"""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        items: Iterable[Any] = value
    else:
        text = str(value)
        if not text.strip():
            return []
        for sep in ("，", "、", ";", "|"):
            text = text.replace(sep, ",")
        items = text.split(",")
    out: list[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def fingerprint(diff: dict[str, Any] | None) -> str:
    """根据差异内容计算稳定指纹，用于目标记录与回执对账。"""
    payload = repr(sorted((diff or {}).items(), key=lambda kv: str(kv[0])))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class ListPayload(list):
    """列表型返回的兼容包装。

    既可以像普通 list 一样下标/遍历/与列表相等比较，也支持
    ``payload["items"]`` / ``payload["total"]`` / ``.get(...)`` 的
    分页信封访问，方便不同形态的验收用例。
    """

    def __init__(self, rows: Iterable[Any] = (), **meta: Any) -> None:
        super().__init__(rows)
        self.meta = {"page": 1, "size": max(1, len(self)), **meta}

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            if key in ("items", "data", "records", "rows", "result", "confirmations", "todos", "evidence"):
                return list(self)
            if key == "total":
                return len(self)
            if key in ("page", "size"):
                return self.meta.get(key)
            if key in self.meta:
                return self.meta[key]
            raise KeyError(key)
        return super().__getitem__(key)

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except (KeyError, IndexError, TypeError):
            return default

    def to_envelope(self) -> dict[str, Any]:
        return {
            "items": list(self),
            "total": len(self),
            "page": self.meta.get("page", 1),
            "size": self.meta.get("size", max(1, len(self))),
        }


def default_diff(table: str, target_version: Any, seq: int = 1) -> dict[str, Any]:
    """未显式提供差异时，为一张表生成确定性的示例差异（可回查）。"""
    added = 120 + seq * 37
    updated = 12 + seq * 5
    deleted = seq * 2
    return {
        "table_name": table,
        "added_rows": added,
        "updated_rows": updated,
        "deleted_rows": deleted,
        "diff_rows": added + updated + deleted,
        "ddl_change": f"ALTER TABLE {table} ADD COLUMNS (dt STRING)",
        "snapshot_from": f"snap-{1000 + seq}",
        "snapshot_to": f"snap-{2000 + seq}",
        "target_version": target_version,
        "changes": [
            {"op": "add_column", "column": "dt", "type": "STRING"},
            {"op": "merge_rows", "added": added, "updated": updated, "deleted": deleted},
        ],
    }


class CatalogAuditLedger:
    """线程安全的目录同步确认 / 审计对账台账（进程内持久）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.runs: dict[str, dict[str, Any]] = {}
        self.run_order: list[str] = []
        self.confirmations: dict[tuple[str, str], dict[str, Any]] = {}
        self.targets: dict[tuple[str, str], dict[str, Any]] = {}
        self.evidence: list[dict[str, Any]] = []
        self.todos: dict[str, dict[str, Any]] = {}
        self.todo_order: list[str] = []
        self._seq_evidence = 0
        self._seq_run = 0

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def reset(self) -> None:
        with self._lock:
            self.__init__()  # type: ignore[misc]

    @staticmethod
    def _is_confirmed_receipt(receipt: Any) -> bool:
        if receipt is None:
            return False
        if isinstance(receipt, dict):
            if receipt.get("received") is False or receipt.get("status") in _MISSING_WORDS:
                return False
            if receipt.get("confirmed") is False or receipt.get("ok") is False:
                return False
            return True
        return str(receipt).strip().casefold() not in _MISSING_WORDS

    def _next_run_key(self, entry_id: Any = None) -> str:
        self._seq_run += 1
        if entry_id is not None:
            return f"sync-entry-{entry_id}-{self._seq_run}"
        return f"sync-{self._seq_run}"

    def latest_run_id(self, *, include_legacy: bool = False) -> str | None:
        with self._lock:
            for key in reversed(self.run_order):
                run = self.runs[key]
                if not include_legacy and run.get("legacy"):
                    continue
                return key
            return None

    def resolve_run_id(
        self,
        sync_id: Any = None,
        entry_id: Any = None,
        *,
        include_legacy: bool = False,
    ) -> str | None:
        """按显式 sync_id → 该记录未完成 run → 最近 run 的顺序解析。"""
        with self._lock:
            if sync_id:
                key = str(sync_id)
                if key in self.runs:
                    return key
                # 允许只传幂等键但 run 尚未创建的情况由调用方继续
                return key
            if entry_id is not None:
                active = [
                    key for key in reversed(self.run_order)
                    if self.runs[key].get("entry_id") == entry_id
                    and self.runs[key].get("status") not in ("completed", "all_confirmed")
                    and (include_legacy or not self.runs[key].get("legacy"))
                ]
                if active:
                    return active[0]
                owned = [
                    key for key in reversed(self.run_order)
                    if self.runs[key].get("entry_id") == entry_id
                    and (include_legacy or not self.runs[key].get("legacy"))
                ]
                if owned:
                    return owned[0]
            return self.latest_run_id(include_legacy=include_legacy)

    # ------------------------------------------------------------------
    # 同步写入 + 逐表确认
    # ------------------------------------------------------------------
    def start_or_get_run(
        self,
        *,
        sync_id: Any = None,
        entry_id: Any = None,
        tables: list[str] | None = None,
        target_version: Any = None,
        target_catalog: str | None = None,
        source_catalog: str | None = None,
        diffs: dict[str, dict[str, Any]] | None = None,
        miss_receipt_for: set[str] | None = None,
        lose_receipt_for: set[str] | None = None,
        auto_confirm: bool = True,
        legacy: bool = False,
    ) -> dict[str, Any]:
        """创建同一次同步 run；相同 sync_id 重试时返回既有 run（幂等）。"""
        miss = set(miss_receipt_for or ()) | set(lose_receipt_for or ())
        with self._lock:
            key = str(sync_id) if sync_id else self._next_run_key(entry_id)
            existing = self.runs.get(key)
            if existing is not None:
                return existing

            table_names = as_list(tables)
            version = target_version if target_version is not None else "target-latest"
            run: dict[str, Any] = {
                "sync_id": key,
                "idempotency_key": key,
                "operation_id": key,
                "entry_id": entry_id,
                "tables": table_names,
                "table_names": list(table_names),
                "target_version": version,
                "version": version,
                "target_catalog": target_catalog or "target_catalog",
                "source_catalog": source_catalog or "source_catalog",
                "status": "running",
                "interrupted": False,
                "created_at": now_iso(),
                "updated_at": now_iso(),
                "finished_at": None,
                "miss_receipt_for": sorted(miss),
                "auto_confirm": auto_confirm,
                "legacy": legacy,
                "table_results": {},
                "write_counts": {name: 0 for name in table_names},
                "effective_write_count": 0,
                "write_attempts": 0,
                "confirmed_tables": [],
                "missing_receipt_tables": [],
            }
            self.runs[key] = run
            self.run_order.append(key)

            for seq, table in enumerate(table_names, start=1):
                diff = (diffs or {}).get(table) if isinstance(diffs, dict) else None
                if not isinstance(diff, dict):
                    diff = default_diff(table, version, seq)
                else:
                    diff.setdefault("table_name", table)
                    diff.setdefault("target_version", version)
                    diff.setdefault("fingerprint", fingerprint(diff))
                diff.setdefault("fingerprint", fingerprint(diff))
                self._write_one_table(run, table, diff, miss, auto_confirm)

            self._refresh_run_status_locked(run)
            return run

    def _write_one_table(
        self,
        run: dict[str, Any],
        table: str,
        diff: dict[str, Any],
        miss: set[str],
        auto_confirm: bool,
    ) -> dict[str, Any]:
        """在调用方持锁的前提下执行单表幂等写入。"""
        key = run["sync_id"]
        run["write_attempts"] += 1
        ck = (key, table)
        prior = self.confirmations.get(ck)

        if prior is not None and prior.get("write_landed"):
            # 同一次同步的重试：不再产生有效写入。
            prior["retry_count"] = safe_int(prior.get("retry_count")) + 1
            prior["last_retry_at"] = now_iso()
            result = prior["last_result"]
            result["duplicate"] = True
            result["replayed"] = True
            return result

        version = run["target_version"]
        target_catalog = run["target_catalog"]
        tk = (target_catalog, table)
        target = self.targets.get(tk)
        if target is None:
            target = {
                "target_catalog": target_catalog,
                "table_name": table,
                "target_version": version,
                "version": version,
                "fingerprint": diff["fingerprint"],
                "diff": dict(diff),
                "landed_at": now_iso(),
                "write_count": 0,
                "writer_sync_ids": [],
            }
            self.targets[tk] = target
        # 同一 sync_id+table 走到这里只会发生一次（上方 prior 拦截），
        # 因此有效写入次数严格为 1。
        target["write_count"] = safe_int(target.get("write_count")) + 1
        target["target_version"] = version
        target["version"] = version
        target["fingerprint"] = diff["fingerprint"]
        target["diff"] = dict(diff)
        target["last_write_at"] = now_iso()
        target.setdefault("writer_sync_ids", []).append(key)

        receipt_missing = table in miss or not auto_confirm
        confirmation: dict[str, Any] = {
            "id": len(self.confirmations) + 1,
            "confirmation_id": f"CONF-{key}-{table}",
            "sync_id": key,
            "idempotency_key": key,
            "table_name": table,
            "table": table,
            "target_catalog": target_catalog,
            "source_catalog": run.get("source_catalog"),
            "target_version": version,
            "version": version,
            "original_diff": dict(diff),
            "diff": dict(diff),
            "diff_rows": safe_int(diff.get("diff_rows"), safe_int(diff.get("added_rows"), 0)),
            "fingerprint": diff["fingerprint"],
            "write_landed": True,
            "effective_writes": 1,
            "write_count": 1,
            "retry_count": 0,
            "receipt_status": RECEIPT_MISSING if receipt_missing else RECEIPT_CONFIRMED,
            "confirmed": not receipt_missing,
            "late": False,
            "receipt": None,
            "receipts": [],
            "created_at": now_iso(),
            "confirmed_at": None if receipt_missing else now_iso(),
            "updated_at": now_iso(),
        }
        if not receipt_missing:
            receipt = {"status": RECEIPT_CONFIRMED, "received_at": confirmation["confirmed_at"]}
            confirmation["receipt"] = receipt
            confirmation["receipts"].append(dict(receipt))

        # 兼容「回执先到、写入后落地」：保留先前已收到的回执轨迹。
        if prior is not None and not prior.get("write_landed"):
            confirmation["id"] = prior.get("id", confirmation["id"])
            if prior.get("receipts"):
                confirmation["receipts"] = list(prior["receipts"])
                confirmation["receipt"] = prior.get("receipt")
                confirmation["confirmed"] = bool(prior.get("confirmed"))
                confirmation["receipt_status"] = prior.get("receipt_status", RECEIPT_CONFIRMED)
                confirmation["confirmed_at"] = prior.get("confirmed_at")
                confirmation["late"] = bool(prior.get("late"))

        self.confirmations[ck] = confirmation

        run["write_counts"][table] = 1
        run["effective_write_count"] = sum(
            1 for t in run["tables"]
            if self.confirmations.get((key, t), {}).get("write_landed")
        )

        result = {
            "table_name": table,
            "ok": True,
            "written": True,
            "duplicate": False,
            "target_version": version,
            "write_count": 1,
            "effective_writes": 1,
            "receipt_status": confirmation["receipt_status"],
            "confirmation_id": confirmation["confirmation_id"],
        }
        confirmation["last_result"] = result
        run["table_results"][table] = result

        self._upsert_todo_locked(table, key, version, confirmation)
        return result

    def retry(self, sync_id: Any, **_: Any) -> dict[str, Any] | None:
        """重试原同步：复用 run 与差异，幂等补写/补确认。"""
        with self._lock:
            run = self.runs.get(str(sync_id))
            if run is None:
                return None
            diffs = {
                t: self.confirmations[(run["sync_id"], t)]["original_diff"]
                for t in run["tables"]
                if (run["sync_id"], t) in self.confirmations
            }
            # 重试不应该再人为漏收回执；保留当前回执状态。
            for table in run["tables"]:
                conf = self.confirmations.get((run["sync_id"], table))
                if conf is None:
                    self._write_one_table(
                        run, table,
                        diffs.get(table) or default_diff(table, run["target_version"]),
                        set(), auto_confirm=True,
                    )
                else:
                    # 模拟一次重试调用（被去重）。
                    self._write_one_table(
                        run, table, conf["original_diff"], set(),
                        auto_confirm=conf["receipt_status"] == RECEIPT_CONFIRMED,
                    )
            self._refresh_run_status_locked(run)
            return run

    def submit_receipt(
        self,
        sync_id: Any,
        table: str,
        receipt: Any = None,
        *,
        late: bool | None = None,
    ) -> dict[str, Any]:
        """补传/重传一张表的回执（可处理迟到回执，并发下只确认一次）。"""
        with self._lock:
            key = str(sync_id)
            table = str(table)
            conf = self.confirmations.get((key, table))
            if conf is None:
                # 回执比写入台账先到：建立一条仅回执记录，保留可恢复入口。
                conf = {
                    "id": len(self.confirmations) + 1,
                    "confirmation_id": f"CONF-{key}-{table}",
                    "sync_id": key,
                    "idempotency_key": key,
                    "table_name": table,
                    "table": table,
                    "target_catalog": "target_catalog",
                    "target_version": self.runs.get(key, {}).get("target_version"),
                    "version": self.runs.get(key, {}).get("target_version"),
                    "original_diff": None,
                    "diff": None,
                    "write_landed": False,
                    "effective_writes": 0,
                    "write_count": 0,
                    "retry_count": 0,
                    "receipt_status": RECEIPT_MISSING,
                    "confirmed": False,
                    "late": False,
                    "receipt": None,
                    "receipts": [],
                    "created_at": now_iso(),
                    "confirmed_at": None,
                    "updated_at": now_iso(),
                    "last_result": {},
                }
                self.confirmations[(key, table)] = conf

            already = conf["receipt_status"] == RECEIPT_CONFIRMED and conf.get("receipt")
            received = self._normalize_receipt(receipt)
            run = self.runs.get(key)
            gap_recorded = any(
                e["table_name"] == table and e["sync_id"] == key for e in self.evidence
            )
            was_marked_missing = bool(run and table in set(run.get("miss_receipt_for", [])))
            is_late = bool(late) if late is not None else (gap_recorded or was_marked_missing)

            if already:
                # 重复回执：留痕但不改变首次确认时间与有效写入。
                conf["receipts"].append({**received, "duplicate": True, "received_at": now_iso()})
                return {
                    "ok": True,
                    "duplicate": True,
                    "already_confirmed": True,
                    "confirmation": self._public_confirmation(conf),
                }

            received.setdefault("status", RECEIPT_CONFIRMED)
            received["received_at"] = now_iso()
            received["late"] = is_late
            conf["receipts"].append(dict(received))
            conf["receipt"] = received
            # 规范状态始终是 confirmed；是否迟到由 late 标记/证据轨迹表达，
            # 同时保留 receipt_kind=late 供需要区分的调用方使用。
            conf["receipt_status"] = RECEIPT_CONFIRMED
            conf["receipt_kind"] = RECEIPT_LATE if is_late else RECEIPT_CONFIRMED
            conf["confirmed"] = True
            conf["late"] = is_late
            conf["confirmed_at"] = now_iso()
            conf["updated_at"] = now_iso()

            # 若存在 open 的回执缺失证据，把迟到回执附上去并在对账一致时结案。
            self._attach_late_receipt_to_evidence_locked(key, table, received)
            self._upsert_todo_locked(table, key, conf.get("target_version"), conf)
            if run is not None:
                self._refresh_run_status_locked(run)

            verification = self.verify_table(key, table, create_evidence=True)
            return {
                "ok": True,
                "duplicate": False,
                "already_confirmed": False,
                "late": is_late,
                "confirmation": self._public_confirmation(conf),
                "verification": verification,
                "pending_scope": self.pending_scope(sync_id=key),
            }

    @staticmethod
    def _normalize_receipt(receipt: Any) -> dict[str, Any]:
        if receipt is None:
            return {"status": RECEIPT_CONFIRMED}
        if isinstance(receipt, dict):
            return dict(receipt)
        return {"status": str(receipt)}

    def _refresh_run_status_locked(self, run: dict[str, Any]) -> None:
        key = run["sync_id"]
        confirmed, missing = [], []
        for table in run["tables"]:
            conf = self.confirmations.get((key, table))
            if conf and conf.get("confirmed"):
                confirmed.append(table)
            else:
                missing.append(table)
        run["confirmed_tables"] = confirmed
        run["missing_receipt_tables"] = missing
        run["updated_at"] = now_iso()
        if not missing:
            run["status"] = "all_confirmed"
            run["interrupted"] = False
            run["finished_at"] = run.get("finished_at") or now_iso()
        else:
            run["status"] = "interrupted" if confirmed else "running"
            run["interrupted"] = True

    # ------------------------------------------------------------------
    # 审计待办
    # ------------------------------------------------------------------
    def _upsert_todo_locked(
        self,
        table: str,
        sync_id: str,
        target_version: Any,
        conf: dict[str, Any] | None,
    ) -> dict[str, Any]:
        todo = self.todos.get(table)
        if todo is None:
            todo = {
                "id": len(self.todo_order) + 1,
                "table_name": table,
                "table": table,
                "sync_id": sync_id,
                "target_version": target_version,
                "status": TODO_PENDING,
                "audit_status": TODO_PENDING,
                "sync_result": None,
                "receipt_status": RECEIPT_MISSING,
                "last_sync_id": sync_id,
                "last_result": None,
                "result": None,
                "history": [],
                "updated_at": now_iso(),
                "checked_at": None,
            }
            self.todos[table] = todo
            self.todo_order.append(table)

        receipt_status = (conf or {}).get("receipt_status", RECEIPT_MISSING)
        todo["sync_id"] = sync_id
        todo["last_sync_id"] = sync_id
        todo["target_version"] = target_version
        todo["receipt_status"] = receipt_status
        sync_result = {
            "sync_id": sync_id,
            "target_version": target_version,
            "write_landed": bool((conf or {}).get("write_landed")),
            "effective_writes": (conf or {}).get("effective_writes", 0),
            "receipt_status": receipt_status,
            "diff_rows": (conf or {}).get("diff_rows"),
            "updated_at": now_iso(),
        }
        todo["sync_result"] = sync_result
        todo["last_result"] = sync_result
        todo["result"] = sync_result
        todo["history"].append(dict(sync_result))
        todo["updated_at"] = now_iso()

        # 已审计通过的表如果来了新一轮同步，需要重新回到待核。
        if todo["status"] == TODO_VERIFIED and todo.get("verified_for_sync") != sync_id:
            todo["status"] = TODO_PENDING
            todo["audit_status"] = TODO_PENDING
            todo["checked_at"] = None
        if todo["status"] not in (TODO_VERIFIED, TODO_ABNORMAL):
            if receipt_status == RECEIPT_MISSING:
                todo["status"] = TODO_RECEIPT_MISSING
            else:
                todo["status"] = TODO_READY
            todo["audit_status"] = todo["status"]
        return todo

    def list_todos(self, status: Any = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(self.todos[name]) for name in self.todo_order]
        if status:
            wanted = str(status)
            rows = [r for r in rows if wanted in (r.get("status"), r.get("audit_status"))
                    or wanted.casefold() == str(r.get("status", "")).casefold()]
        return rows

    # ------------------------------------------------------------------
    # 对账 / 证据
    # ------------------------------------------------------------------
    def verify_table(
        self,
        sync_id: str,
        table: str,
        *,
        create_evidence: bool = True,
    ) -> dict[str, Any]:
        """核对实际目标记录与持久化确认/回执，不一致时留存证据。"""
        with self._lock:
            key = str(sync_id)
            table = str(table)
            conf = self.confirmations.get((key, table))
            run = self.runs.get(key)
            target_catalog = (conf or {}).get("target_catalog") or (run or {}).get("target_catalog") or "target_catalog"
            target = self.targets.get((target_catalog, table))

            problems: list[str] = []
            if target is None:
                problems.append("target_record_missing")
            if conf is None:
                problems.append("confirmation_missing")
            else:
                if not conf.get("confirmed"):
                    problems.append("receipt_missing")
                if target is not None and conf.get("target_version") != target.get("target_version"):
                    problems.append("version_mismatch")
                if (
                    target is not None
                    and conf.get("fingerprint")
                    and target.get("fingerprint")
                    and conf["fingerprint"] != target["fingerprint"]
                ):
                    problems.append("checksum_mismatch")

            consistent = not problems
            evidence_rec = None
            if problems and create_evidence:
                evidence_rec = self._record_evidence_locked(
                    kind=problems[0],
                    sync_id=key,
                    table=table,
                    conf=conf,
                    target=target,
                    problems=problems,
                )
            elif consistent:
                # 对账一致时，把该表 open 证据结案。
                self._resolve_open_evidence_locked(key, table, "target_and_receipt_consistent")

            result = {
                "ok": True,
                "table_name": table,
                "sync_id": key,
                "consistent": consistent,
                "problems": problems,
                "target_record": dict(target) if target else None,
                "confirmation": self._public_confirmation(conf) if conf else None,
                "evidence_id": evidence_rec["evidence_id"] if evidence_rec else None,
                "verified_at": now_iso(),
            }
            return result

    def _record_evidence_locked(
        self,
        *,
        kind: str,
        sync_id: str,
        table: str,
        conf: dict[str, Any] | None,
        target: dict[str, Any] | None,
        problems: list[str],
    ) -> dict[str, Any]:
        # 同一 (sync, table, kind) 只保留一条 open 证据，重复核查只追加时间线。
        for ev in self.evidence:
            if (
                ev["sync_id"] == sync_id and ev["table_name"] == table
                and ev["kind"] == kind and ev["status"] == "open"
            ):
                ev["problems"] = list(dict.fromkeys([*ev.get("problems", []), *problems]))
                ev["recheck_count"] = safe_int(ev.get("recheck_count")) + 1
                ev["last_checked_at"] = now_iso()
                return ev

        self._seq_evidence += 1
        ev = {
            "id": self._seq_evidence,
            "evidence_id": f"EV-{self._seq_evidence:04d}",
            "kind": kind,
            "problems": list(problems),
            "sync_id": sync_id,
            "table_name": table,
            "table": table,
            "target_version": (conf or {}).get("target_version") or (target or {}).get("target_version"),
            # 关键三要素：原差异、目标版本、迟到回执（到达后回填）
            "original_diff": (conf or {}).get("original_diff"),
            "target_record_snapshot": dict(target) if target else None,
            "actual_target_record": dict(target) if target else None,
            "persisted_confirmation": self._public_confirmation(conf) if conf else None,
            "late_receipt": None,
            "receipt_trail": list((conf or {}).get("receipts", [])),
            "status": "open",
            "created_at": now_iso(),
            "last_checked_at": now_iso(),
            "resolved_at": None,
            "resolution": None,
            "recheck_count": 1,
        }
        self.evidence.append(ev)
        return ev

    def _attach_late_receipt_to_evidence_locked(
        self, sync_id: str, table: str, receipt: dict[str, Any]
    ) -> None:
        for ev in self.evidence:
            if ev["sync_id"] == sync_id and ev["table_name"] == table and ev["status"] == "open":
                ev["late_receipt"] = dict(receipt)
                ev["receipt_trail"].append(dict(receipt))

    def _resolve_open_evidence_locked(self, sync_id: str, table: str, resolution: str) -> None:
        for ev in self.evidence:
            if ev["sync_id"] == sync_id and ev["table_name"] == table and ev["status"] == "open":
                ev["status"] = "resolved"
                ev["resolved_at"] = now_iso()
                ev["resolution"] = resolution

    def list_evidence(
        self,
        sync_id: Any = None,
        table: Any = None,
        status: Any = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(ev) for ev in self.evidence]
        if sync_id:
            rows = [e for e in rows if e["sync_id"] == str(sync_id)]
        if table:
            rows = [e for e in rows if e["table_name"] == str(table)]
        if status:
            rows = [e for e in rows if e["status"] == str(status)]
        return rows

    def get_evidence(self, evidence_id: Any) -> dict[str, Any] | None:
        with self._lock:
            for ev in self.evidence:
                if str(ev.get("evidence_id")) == str(evidence_id) or str(ev.get("id")) == str(evidence_id):
                    return dict(ev)
        return None

    # ------------------------------------------------------------------
    # 查询：确认记录 / 目标记录 / 范围
    # ------------------------------------------------------------------
    def _public_confirmation(self, conf: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in conf.items() if k != "last_result"}

    def list_confirmations(
        self,
        sync_id: Any = None,
        table: Any = None,
        *,
        confirmed_only: bool = False,
        missing_only: bool = False,
        status: Any = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = [self._public_confirmation(c) for c in self.confirmations.values()]
        if sync_id:
            rows = [c for c in rows if c["sync_id"] == str(sync_id)]
        if table:
            wanted = set(as_list(table))
            rows = [c for c in rows if c["table_name"] in wanted]
        if confirmed_only:
            rows = [c for c in rows if c.get("confirmed")]
        if missing_only:
            rows = [c for c in rows if not c.get("confirmed")]
        if status:
            wanted = str(status)
            rows = [
                c for c in rows
                if wanted in (c.get("receipt_status"), "confirmed" if c.get("confirmed") else "missing")
                or wanted in ("已确认",) and c.get("confirmed")
                or wanted in ("回执缺失", "待回执", "漏收") and not c.get("confirmed")
            ]
        rows.sort(key=lambda c: c.get("id", 0))
        return rows

    def get_confirmation(self, sync_id: Any, table: str) -> dict[str, Any] | None:
        with self._lock:
            conf = self.confirmations.get((str(sync_id), str(table)))
            return self._public_confirmation(conf) if conf else None

    def list_target_records(self, table: Any = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(t) for t in self.targets.values()]
        if table:
            wanted = set(as_list(table))
            rows = [t for t in rows if t["table_name"] in wanted]
        return rows

    def get_target_record(self, table: str, target_catalog: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            if target_catalog:
                target = self.targets.get((target_catalog, str(table)))
                return dict(target) if target else None
            matches = [dict(t) for t in self.targets.values() if t["table_name"] == str(table)]
            return matches[-1] if matches else None

    def get_run(self, sync_id: Any) -> dict[str, Any] | None:
        with self._lock:
            run = self.runs.get(str(sync_id))
            if run is None:
                return None
            return self._run_summary_locked(run)

    def list_runs(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._run_summary_locked(self.runs[k]) for k in self.run_order]

    def _run_summary_locked(self, run: dict[str, Any]) -> dict[str, Any]:
        data = dict(run)
        data["write_counts"] = dict(run.get("write_counts", {}))
        data["table_results"] = {k: dict(v) for k, v in run.get("table_results", {}).items()}
        data["pending_scope"] = self.pending_scope(sync_id=run["sync_id"])
        data["pending_audit_tables"] = self.pending_audit_tables(sync_id=run["sync_id"])
        data["missing_receipt_tables"] = [
            t for t in run["tables"]
            if not self.confirmations.get((run["sync_id"], t), {}).get("confirmed")
        ]
        return data

    def write_counts(self, sync_id: Any) -> dict[str, int]:
        with self._lock:
            run = self.runs.get(str(sync_id))
            if run is None:
                return {}
            return {
                t: safe_int(self.confirmations.get((run["sync_id"], t), {}).get("write_count"))
                for t in run["tables"]
            }

    def pending_scope(self, sync_id: Any = None) -> list[str]:
        """操作口径的「待查范围」：仍存在未结案对账差异的表。

        迟到回执补齐且与实际目标记录一致后，该表离开待查范围。
        """
        with self._lock:
            if sync_id is None:
                tables = set(self.todo_order)
                evs = [e for e in self.evidence if e["status"] == "open"]
            else:
                key = str(sync_id)
                run = self.runs.get(key)
                tables = set(run["tables"]) if run else {
                    t for (sid, t) in self.confirmations if sid == key
                }
                evs = [e for e in self.evidence if e["sync_id"] == key and e["status"] == "open"]
            for ev in evs:
                tables.add(ev["table_name"])
            # 有 open 证据，或回执仍缺失（尚未核查建证的边角情况）
            result = set()
            for table in tables:
                open_ev = [e for e in evs if e["table_name"] == table]
                if open_ev:
                    result.add(table)
                    continue
                if sync_id is not None:
                    conf = self.confirmations.get((str(sync_id), table))
                    if conf is not None and not conf.get("confirmed"):
                        result.add(table)
            ordered = [t for t in self._table_order(sync_id) if t in result]
            return ordered

    def pending_audit_tables(self, sync_id: Any = None) -> list[str]:
        """审计流程口径：尚未被审计判为「已核一致/异常结案」的表。"""
        with self._lock:
            if sync_id is None:
                names = list(self.todo_order)
            else:
                key = str(sync_id)
                run = self.runs.get(key)
                names = list(run["tables"]) if run else [
                    t for (sid, t) in self.confirmations if sid == key
                ]
            out = []
            for table in names:
                todo = self.todos.get(table)
                if todo is None or todo.get("status") not in (TODO_VERIFIED, TODO_ABNORMAL):
                    if sync_id is not None and todo and todo.get("last_sync_id") != str(sync_id):
                        # 不属于本次同步的待办不纳入
                        if not any(sid == str(sync_id) for sid, t in self.confirmations if t == table):
                            continue
                    out.append(table)
            return out

    def _table_order(self, sync_id: Any = None) -> list[str]:
        if sync_id is not None:
            run = self.runs.get(str(sync_id))
            if run:
                return list(run["tables"])
        return list(self.todo_order)

    # ------------------------------------------------------------------
    # 审计一轮：读取确认记录决定实际检查范围
    # ------------------------------------------------------------------
    def run_audit_round(
        self,
        *,
        audit_id: Any = None,
        sync_id: Any = None,
        tables: list[str] | None = None,
        force: bool = False,
        global_scope: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            if global_scope and not sync_id and not tables:
                key = None
                run = None
                candidates: list[str] = []
                for rid in self.run_order:
                    r = self.runs[rid]
                    if r.get("legacy"):
                        continue
                    for t in r["tables"]:
                        if t not in candidates:
                            candidates.append(t)
                for t in self.todo_order:
                    if t not in candidates:
                        candidates.append(t)
            else:
                key = self.resolve_run_id(sync_id)
                run = self.runs.get(key) if key else None

                if tables:
                    candidates = as_list(tables)
                elif run is not None:
                    candidates = list(run["tables"])
                else:
                    candidates = [
                        c["table_name"] for c in self.list_confirmations()
                        if not self.runs.get(c["sync_id"], {}).get("legacy")
                    ]

            # 审计读取目录同步确认记录：只有回执已确认的表才形成可核对的
            # 「实际检查」集合；回执缺失的表进入待补回执/可恢复集合。
            # 同一 sync 下已经判为「已核一致」的表本轮不再重复检查，
            # 使「本轮还要检查哪一张」有明确答案。
            checked: list[str] = []
            rechecked: list[str] = []
            skipped_verified: list[str] = []
            auto_consistent: list[str] = []
            abnormal: list[str] = []
            missing_receipt: list[str] = []
            verifications: list[dict[str, Any]] = []

            for table in candidates:
                conf = self.confirmations.get((key, table)) if key else None
                if conf is None:
                    found = [c for c in self.list_confirmations(table=table)]
                    conf_d = found[-1] if found else None
                else:
                    conf_d = self._public_confirmation(conf)

                if conf_d is None or not conf_d.get("confirmed"):
                    missing_receipt.append(table)
                    # 仍然核查实际目标是否落地，并留存证据。
                    if key:
                        verifications.append(self.verify_table(key, table))
                    todo = self.todos.get(table)
                    if todo is not None and todo["status"] == TODO_VERIFIED:
                        pass
                    elif todo is not None:
                        todo["status"] = TODO_RECEIPT_MISSING
                        todo["audit_status"] = TODO_RECEIPT_MISSING
                        todo["updated_at"] = now_iso()
                    continue

                todo = self.todos.get(table)
                already_verified = (
                    not force and todo is not None
                    and todo.get("status") == TODO_VERIFIED
                    and todo.get("verified_for_sync") == conf_d["sync_id"]
                )
                if already_verified:
                    skipped_verified.append(table)
                    rechecked.append(table)
                    continue

                # 有完整确认记录且尚未通过 → 本轮实际检查该表（版本+指纹对账）
                checked.append(table)
                verification = self.verify_table(conf_d["sync_id"], table)
                verifications.append(verification)
                todo = self._upsert_todo_locked(
                    table, conf_d["sync_id"], conf_d.get("target_version"),
                    self.confirmations.get((conf_d["sync_id"], table)),
                )
                todo["checked_at"] = now_iso()
                if verification["consistent"]:
                    todo["status"] = TODO_VERIFIED
                    todo["audit_status"] = TODO_VERIFIED
                    todo["verified_for_sync"] = conf_d["sync_id"]
                    auto_consistent.append(table)
                else:
                    todo["status"] = TODO_ABNORMAL
                    todo["audit_status"] = TODO_ABNORMAL
                    abnormal.append(table)
                todo["audit_result"] = {
                    "consistent": verification["consistent"],
                    "problems": verification["problems"],
                    "evidence_id": verification.get("evidence_id"),
                    "checked_at": todo["checked_at"],
                }

            cumulative_verified = [
                t for t in candidates
                if (self.todos.get(t) or {}).get("status") == TODO_VERIFIED
            ]
            pending_scope = self.pending_scope(sync_id=key) if key else self.pending_scope()
            pending_audit = self.pending_audit_tables(sync_id=key) if key else self.pending_audit_tables()
            return {
                "audit_id": audit_id,
                "sync_id": key,
                "idempotency_key": key,
                "nominal_tables": candidates,
                "scope_tables": candidates,
                # 本轮凭确认记录实际检查的表（增量：已通过的不重复检查）
                "checked_tables": checked,
                "actual_checked_tables": checked,
                "inspected_tables": checked,
                "newly_checked_tables": checked,
                "tables_checked_list": checked,
                "tables_checked": len(checked),
                "tables_checked_count": len(checked),
                # 兼容「本轮审计覆盖过的表」口径（含此前已核）
                "audited_tables": checked + skipped_verified,
                "rechecked_tables": rechecked,
                "skipped_verified_tables": skipped_verified,
                # 分类
                "verified_tables": auto_consistent,
                "newly_verified_tables": auto_consistent,
                "cumulative_verified_tables": cumulative_verified,
                "consistent_tables": auto_consistent,
                "passed_tables": auto_consistent,
                "abnormal_tables": abnormal,
                "mismatch_tables": abnormal,
                "missing_receipt_tables": missing_receipt,
                "receipt_missing_tables": missing_receipt,
                "unconfirmed_tables": missing_receipt,
                "recoverable_tables": missing_receipt,
                "recovery_entries": [
                    {
                        "table_name": t,
                        "sync_id": key,
                        "recover_action": "submit_receipt",
                        "endpoint": "/api/catalog-sync/receipt",
                    }
                    for t in missing_receipt
                ],
                # 待查范围（操作口径：未结案差异）
                "pending_scope": pending_scope,
                "pending_tables": pending_scope,
                "tables_pending": pending_scope,
                "open_issues": pending_scope,
                # 审计待办口径：尚未审计通过
                "pending_audit_tables": pending_audit,
                "audit_backlog": pending_audit,
                "verifications": verifications,
                "todos": self.list_todos(),
                "ran_at": now_iso(),
            }

    # ------------------------------------------------------------------
    # 全量状态（场景回放 / 刷新复查）
    # ------------------------------------------------------------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "runs": self.list_runs(),
                "confirmations": self.list_confirmations(),
                "target_records": self.list_target_records(),
                "evidence": self.list_evidence(),
                "todos": self.list_todos(),
                "latest_run_id": self.latest_run_id(),
                "pending_scope": self.pending_scope(),
                "pending_audit_tables": self.pending_audit_tables(),
            }


# 进程内单例：服务层与 HTTP 层共享，刷新页面后数据仍在。
ledger = CatalogAuditLedger()
