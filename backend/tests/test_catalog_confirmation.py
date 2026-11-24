"""目录同步逐表确认 ↔ 湖仓审计对账的场景测试。

复刻题面：A/B 同步 → A 已确认、B 回执漏收 → 审计定范围 →
补传 B 迟到回执 → 并发重试原同步 → 核对待查范围与有效写入次数。
"""
from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

sys.path.insert(0, '.')

from app.services import service_for
from app.services.catalog_flow import ledger


@pytest.fixture(autouse=True)
def _reset_ledger():
    ledger.reset()
    yield
    ledger.reset()


@pytest.fixture()
def catalog():
    return service_for("table_catalog")


@pytest.fixture()
def audit():
    return service_for("lake_audit")


class TestCatalogConfirmationScenario:
    def test_persisted_confirmation_is_rereadable(self, catalog):
        run = catalog.sync_tables(tables=["A", "B"], sync_id="s1", target_version="v7")
        assert run["ok"] is True
        # 刷新后重新查询仍可读到逐表确认记录与原差异
        confs = catalog.list_confirmations("s1")
        assert {c["table_name"] for c in confs} == {"A", "B"}
        a = catalog.get_confirmation("s1", "A")
        assert a["target_version"] == "v7"
        assert a["original_diff"]["diff_rows"] > 0
        assert catalog.get_target_record("A")["target_version"] == "v7"

    def test_miss_b_receipt_audit_scope_and_recovery(self, catalog, audit):
        catalog.sync_tables(
            tables=["A", "B"], sync_id="s2", target_version="v9",
            miss_receipt_for=["B"],
        )
        result = audit.domain_run_audit(1, sync_id="s2")
        # 审计读确认记录：本轮只实查已确认的 A，待查范围是 B
        assert result["checked_tables"] == ["A"]
        assert result["missing_receipt_tables"] == ["B"]
        assert result["pending_scope"] == ["B"]

        # 两张表实际都已落地目标版本
        assert catalog.verify_table("s2", "A")["consistent"] is True
        vb = catalog.verify_table("s2", "B")
        assert vb["consistent"] is False
        assert "receipt_missing" in vb["problems"]
        assert vb["target_record"]["target_version"] == "v9"

        # 证据：原差异 + 目标版本可回查
        ev = catalog.list_evidence("s2", table="B")
        assert len(ev) == 1
        assert ev[0]["original_diff"]["table_name"] == "B"
        assert ev[0]["target_version"] == "v9"

        # 补传 B 的迟到回执：待查差异结案，证据保留并附迟到回执
        receipt = catalog.confirm_table("s2", "B", {"ack_id": "ack-B"})
        assert receipt["ok"] is True and receipt["late"] is True
        assert receipt["pending_scope"] == []
        ev_after = catalog.list_evidence("s2", table="B")[0]
        assert ev_after["status"] == "resolved"
        assert ev_after["late_receipt"]["ack_id"] == "ack-B"

    def test_concurrent_retry_writes_once(self, catalog):
        catalog.sync_tables(
            tables=["A", "B"], sync_id="s3", target_version="v1",
            miss_receipt_for=["B"],
        )

        def retry(_):
            return catalog.retry_sync("s3")

        with ThreadPoolExecutor(max_workers=8) as pool:
            outs = list(pool.map(retry, range(32)))
        assert all(o["ok"] for o in outs)
        assert catalog.write_counts("s3") == {"A": 1, "B": 1}
        assert catalog.get_target_record("A")["write_count"] == 1
        assert catalog.get_target_record("B")["write_count"] == 1

        # 并发重复回执也不会改变首次确认结论
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: catalog.confirm_table("s3", "B", {"i": i}), range(16)))
        b = catalog.get_confirmation("s3", "B")
        assert b["confirmed"] is True

    def test_full_narrative_final_scope(self, catalog, audit):
        catalog.sync_tables(
            tables=["A", "B"], sync_id="s4", target_version="v42",
            miss_receipt_for=["B"],
        )
        audit.domain_run_audit(1, sync_id="s4")
        catalog.confirm_table("s4", "B", {"ack_id": "late"})
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda _: catalog.retry_sync("s4"), range(12)))

        assert catalog.write_counts("s4") == {"A": 1, "B": 1}
        # 差异口径待查范围已清空；审计待办口径 B 尚待下一轮
        assert catalog.pending_scope("s4") == []
        assert audit.audit_scope(sync_id="s4")["pending_audit_tables"] == ["B"]

        again = audit.domain_run_audit(1, sync_id="s4")
        assert again["checked_tables"] == ["B"]
        assert again["pending_audit_tables"] == []


class TestCatalogConfirmationHttp:
    def test_http_flow(self):
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            r = client.post("/api/catalog-sync/run", json={
                "sync_id": "h1", "tables": ["A", "B"],
                "target_version": "v3", "miss_receipt_for": ["B"],
            })
            assert r.status_code == 200
            assert r.json()["write_counts"] == {"A": 1, "B": 1}

            r = client.get("/api/table_catalog/confirmations", params={"sync_id": "h1"})
            assert r.status_code == 200 and len(r.json()) == 2

            r = client.post("/api/lake_audit/run", json={"entry_id": 1, "sync_id": "h1"})
            assert r.status_code == 200
            assert r.json()["pending_scope"] == ["B"]

            r = client.post("/api/catalog-sync/receipt", json={
                "sync_id": "h1", "table": "B", "receipt": {"ack_id": "z"},
            })
            assert r.status_code == 200 and r.json()["pending_scope"] == []

            r = client.post("/api/chains/catalog-audit-confirm",
                            params={"sync_id": "chain-x"})
            assert r.status_code == 200
            assert r.json()["effective_write_once"] is True
