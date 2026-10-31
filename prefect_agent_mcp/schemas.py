"""外部接口数据契约与数据脱敏。"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from functools import singledispatch
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Contract(BaseModel):
    """拒绝未声明字段的基础数据契约。"""

    model_config = ConfigDict(extra="forbid")


class Workflow(Contract):
    """操作员明确授权的 Prefect 部署。"""

    workflow_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")
    deployment_id: UUID
    description: str = ""
    version: str = "1"
    capability: Literal["native_only"] = "native_only"


class Registry(Contract):
    """操作员配置的部署白名单。"""

    workflows: list[Workflow] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def validate_unique(self) -> Registry:
        """拒绝重复的工作流或部署标识。

        :return: 校验后的白名单。
        :raises ValueError: 工作流标识或部署标识重复。
        """
        if len({w.workflow_id for w in self.workflows}) != len(self.workflows):
            raise ValueError("Duplicate workflow ID")
        if len({w.deployment_id for w in self.workflows}) != len(self.workflows):
            raise ValueError("Duplicate deployment ID")
        return self


class Result(Contract):
    """统一的结构化工具响应。"""

    ok: bool
    data: Any = None
    error: str | None = None
    message: str | None = None
    stale: bool = False
    last_synced_at: datetime | None = None


SENSITIVE = re.compile(
    r"password|passwd|secret|token|api.?key|authorization|cookie|credential",
    re.IGNORECASE,
)


def utcnow() -> datetime:
    """获取当前 UTC 时间。

    :return: 带 UTC 时区的当前时间。
    """
    return datetime.now(UTC)


def redact(value: Any, key: str = "") -> Any:
    """屏蔽敏感字段并限制可展示的数据长度。

    :param value: 需要处理的 JSON 数据。
    :param key: 当前字段名称。
    :return: 处理后的数据。
    """
    return "[REDACTED]" if SENSITIVE.search(key) else _redact_value(value)


@singledispatch
def _redact_value(value: Any) -> Any:
    """保留不需要递归处理的标量。

    :param value: 待处理的数据。
    :return: 原标量。
    """
    return value


@_redact_value.register(dict)
def _(value: dict[str, Any]) -> dict[str, Any]:
    """递归处理对象字段。

    :param value: 需要处理的对象。
    :return: 已处理的对象。
    """
    return {str(key): redact(item, str(key)) for key, item in value.items()}


@_redact_value.register(list)
def _(value: list[Any]) -> list[Any]:
    """递归处理列表并限制元素数量。

    :param value: 需要处理的列表。
    :return: 已处理的列表。
    """
    return [redact(item) for item in value[:100]]


@_redact_value.register
def _(value: str) -> str:
    """限制单个字符串的长度。

    :param value: 需要处理的文本。
    :return: 截断后的文本。
    """
    return value[:2048] + ("...[TRUNCATED]" if len(value) > 2048 else "")
