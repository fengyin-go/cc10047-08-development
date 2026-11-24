"""跨模块业务链路：把多个领域服务的 domain_xxx 方法串联为端到端业务流程。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import APIRouter, HTTPException

from app.services import service_for
from app.services.catalog_flow import as_list, ledger

router = APIRouter(prefix="/api/chains", tags=["业务链路"])

# ------------------------------------------------------------------
# 链路1: 实时流入
# stream_source→stream_ingest→commit_version→delta_ingest
# ------------------------------------------------------------------

@router.post("/stream-ingest")
def chain_stream_ingest(source_id: int, ingest_id: int, commit_id: int) -> dict:
    """实时流入链路：探测源Schema → 批次摄入 → 提交版本 → 增量摄入。"""
    steps = []

    # Step 1: 探测源Schema
    svc_src = service_for("stream_source")
    if not svc_src:
        raise HTTPException(404, "stream_source 服务未注册")
    r1 = svc_src.domain_detect_schema(source_id)
    steps.append({"step": "detect_schema", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 批次摄入
    svc_ingest = service_for("stream_ingest")
    if not svc_ingest:
        raise HTTPException(404, "stream_ingest 服务未注册")
    r2 = svc_ingest.domain_ingest_batch(ingest_id, batch_size=1000)
    steps.append({"step": "ingest_batch", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 提交版本
    svc_commit = service_for("delta_commit")
    if not svc_commit:
        raise HTTPException(404, "delta_commit 服务未注册")
    r3 = svc_commit.domain_commit(commit_id)
    steps.append({"step": "commit_version", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 增量摄入
    r4 = svc_commit.domain_ingest_delta(commit_id, delta_records=500)
    steps.append({"step": "delta_ingest", "ok": r4["ok"], "details": r4})

    return {"chain": "stream-ingest", "ok": True, "steps": steps, "summary": {
        "ingested_rows": r2.get("ingested_rows"),
        "committed_version": r3.get("version"),
        "delta_records": r4.get("delta_records"),
    }}


# ------------------------------------------------------------------
# 链路2: 存储优化
# lake_table→partition_merge→compaction_job→table_compaction
# ------------------------------------------------------------------

@router.post("/storage-optimize")
def chain_storage_optimize(table_id: int, merge_id: int, compaction_id: int) -> dict:
    """存储优化链路：定义湖表 → 合并分区 → 规划压缩 → 压缩表。"""
    steps = []

    # Step 1: 定义湖表
    svc_table = service_for("lake_table")
    if not svc_table:
        raise HTTPException(404, "lake_table 服务未注册")
    r1 = svc_table.domain_define_table(table_id)
    steps.append({"step": "define_table", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 合并分区
    svc_merge = service_for("partition_merge")
    if not svc_merge:
        raise HTTPException(404, "partition_merge 服务未注册")
    r2 = svc_merge.domain_merge_partitions(merge_id)
    steps.append({"step": "merge_partitions", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 规划压缩
    svc_comp = service_for("compaction_task")
    if not svc_comp:
        raise HTTPException(404, "compaction_task 服务未注册")
    r3 = svc_comp.domain_plan_compaction(compaction_id)
    steps.append({"step": "plan_compaction", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 压缩表
    r4 = svc_comp.domain_compact_table(compaction_id)
    steps.append({"step": "compact_table", "ok": r4["ok"], "details": r4})

    return {"chain": "storage-optimize", "ok": True, "steps": steps, "summary": {
        "table_def": r1.get("ddl"),
        "merged_files": r2.get("merged_files"),
        "compaction_plan": r3.get("plan"),
        "compression_ratio": r4.get("compression_ratio"),
    }}


# ------------------------------------------------------------------
# 链路3: Schema演进
# schema_evolve→data_backfill→batch_merge→stream_checkpoint
# ------------------------------------------------------------------

@router.post("/schema-evolve")
def chain_schema_evolve(schema_id: int, backfill_id: int, merge_id: int, checkpoint_id: int) -> dict:
    """Schema演进链路：Schema演进 → 数据回填 → 批流合并 → 保存检查点。"""
    steps = []

    # Step 1: Schema演进
    svc_schema = service_for("stream_schema")
    if not svc_schema:
        raise HTTPException(404, "stream_schema 服务未注册")
    r1 = svc_schema.domain_evolve_schema(schema_id)
    steps.append({"step": "evolve_schema", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 数据回填
    svc_backfill = service_for("backfill_pipeline")
    if not svc_backfill:
        raise HTTPException(404, "backfill_pipeline 服务未注册")
    r2 = svc_backfill.domain_backfill(backfill_id)
    steps.append({"step": "data_backfill", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 批流合并
    svc_merge = service_for("batch_merge")
    if not svc_merge:
        raise HTTPException(404, "batch_merge 服务未注册")
    r3 = svc_merge.domain_merge_batch_stream(merge_id)
    steps.append({"step": "batch_stream_merge", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 保存检查点
    svc_ckpt = service_for("stream_checkpoint")
    if not svc_ckpt:
        raise HTTPException(404, "stream_checkpoint 服务未注册")
    r4 = svc_ckpt.domain_save_checkpoint(checkpoint_id)
    steps.append({"step": "save_checkpoint", "ok": r4["ok"], "details": r4})

    return {"chain": "schema-evolve", "ok": True, "steps": steps, "summary": {
        "schema_version": r1.get("evolved_version"),
        "backfill_rows": r2.get("completed_rows"),
        "reconciled_total": r3.get("reconciled_total"),
        "checkpoint": r4.get("checkpoint_id"),
    }}


# ------------------------------------------------------------------
# 链路4: 生命周期
# snapshot_mgr→history_clean→retention_policy
# ------------------------------------------------------------------

@router.post("/lifecycle")
def chain_lifecycle(snapshot_id: int, clean_id: int, retention_id: int) -> dict:
    """生命周期链路：创建快照 → 清理历史 → 检查保留策略。"""
    steps = []

    # Step 1: 创建快照
    svc_snap = service_for("snapshot_mgr")
    if not svc_snap:
        raise HTTPException(404, "snapshot_mgr 服务未注册")
    r1 = svc_snap.domain_create_snapshot(snapshot_id)
    steps.append({"step": "create_snapshot", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "lifecycle", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 清理历史
    svc_vacuum = service_for("vacuum_job")
    if not svc_vacuum:
        raise HTTPException(404, "vacuum_job 服务未注册")
    r2 = svc_vacuum.domain_clean_history(clean_id)
    steps.append({"step": "clean_history", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "lifecycle", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 检查保留策略
    svc_ret = service_for("retention_policy")
    if not svc_ret:
        raise HTTPException(404, "retention_policy 服务未注册")
    r3 = svc_ret.domain_check_retention(retention_id)
    steps.append({"step": "check_retention", "ok": r3["ok"], "details": r3})

    return {"chain": "lifecycle", "ok": True, "steps": steps, "summary": {
        "snapshot_rows": r1.get("row_count"),
        "files_cleaned": r2.get("files_cleaned"),
        "can_clean_snapshots": r3.get("can_clean_count"),
    }}


# ------------------------------------------------------------------
# 链路5: 元数据治理
# lake_lineage→lake_audit→catalog_sync
# ------------------------------------------------------------------

@router.post("/metadata-govern")
def chain_metadata_govern(lineage_id: int, audit_id: int, catalog_id: int) -> dict:
    """元数据治理链路：追踪血缘 → 运行审计 → 同步目录。"""
    steps = []

    # Step 1: 追踪血缘
    svc_lineage = service_for("lake_lineage")
    if not svc_lineage:
        raise HTTPException(404, "lake_lineage 服务未注册")
    r1 = svc_lineage.domain_trace_lineage(lineage_id)
    steps.append({"step": "trace_lineage", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "metadata-govern", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 运行审计
    svc_audit = service_for("lake_audit")
    if not svc_audit:
        raise HTTPException(404, "lake_audit 服务未注册")
    r2 = svc_audit.domain_run_audit(audit_id)
    steps.append({"step": "run_audit", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "metadata-govern", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 同步目录
    svc_catalog = service_for("table_catalog")
    if not svc_catalog:
        raise HTTPException(404, "table_catalog 服务未注册")
    r3 = svc_catalog.domain_sync_catalog(catalog_id)
    steps.append({"step": "sync_catalog", "ok": r3["ok"], "details": r3})

    return {"chain": "metadata-govern", "ok": True, "steps": steps, "summary": {
        "lineage_depth": r1.get("depth"),
        "audit_tables": r2.get("tables_checked"),
        "synced_tables": r3.get("synced_tables"),
    }}


# ------------------------------------------------------------------
# 链路6: 目录同步逐表确认 ↔ 湖仓审计待办对账
# A/B 同步 → 漏收回执 → 审计定范围 → 补传迟到回执 → 并发重试去重
# ------------------------------------------------------------------

@router.post("/catalog-audit-confirm")
@router.post("/catalog-audit")
@router.post("/catalog-confirm")
@router.post("/catalog-audit-reconcile")
@router.post("/audit-catalog")
def chain_catalog_audit_confirm(
    sync_id: str = "scenario-sync",
    tables: str = "A,B",
    miss_receipt_for: str = "B",
    target_version: str = "target-v1",
    audit_id: int = 1,
    retry_times: int = 8,
    concurrent: bool = True,
) -> dict[str, Any]:
    """完整复现题面场景，并返回每个阶段的可核对数据。"""
    svc_catalog = service_for("table_catalog")
    svc_audit = service_for("lake_audit")
    if not svc_catalog or not svc_audit:
        raise HTTPException(500, "table_catalog/lake_audit 服务未注册")

    names = as_list(tables)
    missing = as_list(miss_receipt_for)
    phases: dict[str, Any] = {}

    # Phase 1: 运行 A、B 同步，A 确认、B 回执漏收
    run = svc_catalog.sync_tables(
        tables=names, sync_id=sync_id, target_version=target_version,
        miss_receipt_for=missing,
    )
    phases["sync"] = {
        "confirmed_tables": run["confirmed_tables"],
        "missing_receipt_tables": run["missing_receipt_tables"],
        "write_counts": run["write_counts"],
    }

    # Phase 2: 发起审计，读确认记录决定本轮实查范围
    audit = svc_audit.domain_run_audit(audit_id, sync_id=sync_id)
    phases["audit"] = {
        "checked_tables": audit["checked_tables"],
        "pending_scope": audit["pending_scope"],
        "missing_receipt_tables": audit["missing_receipt_tables"],
        "evidence_ids": [e["evidence_id"] for e in svc_catalog.list_evidence(sync_id)],
    }

    # Phase 3: 值班员核对两张表实际落地
    phases["verify_targets"] = {
        t: svc_catalog.verify_table(sync_id, t)["consistent"] for t in names
    }

    # Phase 4: 补传 B 的迟到回执
    late_table = missing[0] if missing else None
    if late_table:
        receipt = svc_catalog.confirm_table(
            sync_id, late_table, {"ack_id": f"ack-{late_table}-late", "status": "confirmed"},
        )
        phases["late_receipt"] = {
            "table": late_table, "late": receipt.get("late"),
            "pending_scope": receipt.get("pending_scope"),
        }

    # Phase 5: 并发重试原同步
    def _retry(_: int) -> Any:
        return svc_catalog.retry_sync(sync_id)

    times = max(1, retry_times)
    if concurrent and times > 1:
        with ThreadPoolExecutor(max_workers=min(8, times)) as pool:
            list(pool.map(_retry, range(times)))
    else:
        for i in range(times):
            _retry(i)

    write_counts = svc_catalog.write_counts(sync_id)
    phases["retry"] = {
        "retries": times,
        "write_counts": write_counts,
        "pending_scope": svc_catalog.pending_scope(sync_id),
        "audit_backlog": ledger.pending_audit_tables(sync_id),
    }

    all_once = all(v == 1 for v in write_counts.values()) and set(write_counts) == set(names)
    return {
        "chain": "catalog-audit-confirm",
        "ok": True,
        "sync_id": sync_id,
        "phases": phases,
        "confirmations": svc_catalog.list_confirmations(sync_id),
        "evidence": svc_catalog.list_evidence(sync_id),
        "target_records": svc_catalog.list_target_records(),
        "write_counts": write_counts,
        "effective_write_once": all_once,
        "pending_scope": phases["retry"]["pending_scope"],
        "summary": {
            "tables": names,
            "audited_this_round": audit["checked_tables"],
            "pending_scope": phases["retry"]["pending_scope"],
            "write_counts": write_counts,
        },
    }