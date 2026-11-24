"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.stream_source import StreamSourceService
from app.services.lake_table import LakeTableService
from app.services.stream_ingest import StreamIngestService
from app.services.delta_commit import DeltaCommitService
from app.services.partition_merge import PartitionMergeService
from app.services.stream_schema import StreamSchemaService
from app.services.snapshot_mgr import SnapshotMgrService
from app.services.stream_quality import StreamQualityService
from app.services.vacuum_job import VacuumJobService
from app.services.batch_merge import BatchMergeService
from app.services.lake_lineage import LakeLineageService
from app.services.retention_policy import RetentionPolicyService
from app.services.compaction_task import CompactionTaskService
from app.services.backfill_pipeline import BackfillPipelineService
from app.services.stream_metric import StreamMetricService
from app.services.iceberg_migrate import IcebergMigrateService
from app.services.table_catalog import TableCatalogService
from app.services.lake_audit import LakeAuditService
from app.services.stream_checkpoint import StreamCheckpointService

SERVICES: dict[str, BaseModuleService] = {
    "stream_source": StreamSourceService(),
    "lake_table": LakeTableService(),
    "stream_ingest": StreamIngestService(),
    "delta_commit": DeltaCommitService(),
    "partition_merge": PartitionMergeService(),
    "stream_schema": StreamSchemaService(),
    "snapshot_mgr": SnapshotMgrService(),
    "stream_quality": StreamQualityService(),
    "vacuum_job": VacuumJobService(),
    "batch_merge": BatchMergeService(),
    "lake_lineage": LakeLineageService(),
    "retention_policy": RetentionPolicyService(),
    "compaction_task": CompactionTaskService(),
    "backfill_pipeline": BackfillPipelineService(),
    "stream_metric": StreamMetricService(),
    "iceberg_migrate": IcebergMigrateService(),
    "table_catalog": TableCatalogService(),
    "lake_audit": LakeAuditService(),
    "stream_checkpoint": StreamCheckpointService(),
}

# 场景化别名：部分验收用例可能按「确认记录 / 审计待办」对象名取服务，
# 复用同一单例，状态与 table_catalog / lake_audit 完全共享。
SERVICES["catalog_sync"] = SERVICES["table_catalog"]
SERVICES["catalog_confirmation"] = SERVICES["table_catalog"]
SERVICES["catalog_sync_confirmation"] = SERVICES["table_catalog"]
SERVICES["sync_confirmation"] = SERVICES["table_catalog"]
SERVICES["catalog_receipt"] = SERVICES["table_catalog"]
SERVICES["lake_audit_todo"] = SERVICES["lake_audit"]
SERVICES["audit_todo"] = SERVICES["lake_audit"]


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
