"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    # 允许验收/前端透传任意场景参数（table、late、checksum 等），
    # 未声明字段会进入 model_dump 并由路由透传给领域服务。
    model_config = ConfigDict(extra="allow")

    action: str
    remark: str | None = None
    # 目录同步逐表确认场景的可选透传参数（其他模块忽略）
    idempotency_key: str | None = None
    sync_id: str | None = None
    tables: list[str] | str | None = None
    table: str | None = None
    table_name: str | None = None
    target_version: str | int | None = None
    miss_receipt_for: list[str] | str | None = None
    receipt: dict[str, Any] | None = None
