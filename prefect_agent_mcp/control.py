"""工作流控制命令、权限检查和幂等处理。"""

from __future__ import annotations

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, ClassVar
from uuid import UUID

import httpx
import jsonschema
from prefect.client.schemas.objects import State, StateType
from prefect.exceptions import PrefectException
from sqlalchemy.exc import SQLAlchemyError

from .runtime import Runtime
from .schemas import Result


class FlowCommand(ABC):
    """定义工作流命令的校验与执行过程。"""

    # 只有外部系统支持相同幂等键重试时，子类才应开启此选项。
    retry_pending = False

    def __init__(
        self, runtime: Runtime, identity: str, payload: dict[str, Any]
    ) -> None:
        """保存执行命令所需的上下文。

        :param runtime: 工作流查询与存储服务。
        :param identity: 工作流或运行标识。
        :param payload: 命令参数。
        """
        self.runtime = runtime
        self.identity = identity
        self.payload = payload

    @abstractmethod
    async def validate(self) -> Result | None:
        """校验命令是否可以执行。

        :return: 不可执行时返回错误，否则返回空值。
        """

    @abstractmethod
    async def perform(self, key: str) -> Result:
        """向 Prefect 提交命令。

        :param key: 本次请求的幂等键。
        :return: 命令执行结果。
        """


class StartFlowCommand(FlowCommand):
    """校验参数并启动已注册的工作流。"""

    retry_pending = True
    deployment_id: UUID

    async def validate(self) -> Result | None:
        """校验部署白名单和输入 Schema。

        :return: 参数无效时返回错误，否则返回空值。
        """
        workflow = self.runtime.workflow(self.identity)
        self.deployment_id = workflow.deployment_id
        async with self.runtime.client() as client:
            deployment = await client.read_deployment(self.deployment_id)
        if len(json.dumps(self.payload, ensure_ascii=False).encode("utf-8")) > 65536:
            return Result(
                ok=False, error="INVALID_INPUT", message="Parameters exceed 64 KiB"
            )
        schema = deployment.parameter_openapi_schema
        if not schema:
            return Result(
                ok=False,
                error="INVALID_INPUT",
                message="Deployment has no parameter schema",
            )
        if set(self.payload) - set(schema.get("properties", {})):
            return Result(ok=False, error="INVALID_INPUT", message="Unknown parameter")
        jsonschema.validate(instance=self.payload, schema=schema)
        return None

    async def perform(self, key: str) -> Result:
        """使用 Prefect 幂等键创建运行。

        :param key: 本次请求的幂等键。
        :return: 新建或已存在的运行标识。
        """
        async with self.runtime.client() as client:
            run = await client.create_flow_run_from_deployment(
                self.deployment_id, parameters=self.payload, idempotency_key=key
            )
        return Result(ok=True, data={"flow_run_id": str(run.id)})


class CancelFlowCommand(FlowCommand):
    """请求取消已授权的工作流运行。"""

    flow_id: UUID

    async def validate(self) -> Result | None:
        """确认运行属于白名单且当前状态可读取。

        :return: 运行不可访问时返回错误，否则返回空值。
        """
        self.flow_id = UUID(self.identity)
        current = await self.runtime.flow(self.flow_id)
        if not current.ok or current.stale:
            return Result(
                ok=False,
                error="NOT_AVAILABLE",
                message="Authorized live flow run is required",
            )
        return None

    async def perform(self, key: str) -> Result:
        """向 Prefect 申请取消，实际停止由 Prefect 处理。

        :param key: 已登记的幂等键。
        :return: Prefect 对取消请求的处理结果。
        """
        async with self.runtime.client() as client:
            response = await client.set_flow_run_state(
                self.flow_id, state=State(type=StateType.CANCELLING, name="Cancelling")
            )
        accepted = response.status.value == "ACCEPT"
        return Result(
            ok=accepted,
            data={"orchestration_status": response.status.value},
            error=None if accepted else "STATE_REJECTED",
        )


class CommandService:
    """以统一方式执行不同的工作流命令。"""

    commands: ClassVar[dict[str, type[FlowCommand]]] = {
        "start": StartFlowCommand,
        "cancel": CancelFlowCommand,
    }

    def __init__(self, runtime: Runtime) -> None:
        """保存命令共享的运行时依赖。

        :param runtime: 工作流查询与存储服务。
        """
        self.runtime = runtime

    async def execute(
        self, operation: str, identity: str, payload: dict[str, Any], key: str
    ) -> Result:
        """检查权限及幂等记录，调用具体命令并保存结果。

        :param operation: 命令名称。
        :param identity: 工作流或运行标识。
        :param payload: 命令参数。
        :param key: 幂等键。
        :return: 结构化执行结果。
        """
        if not self.runtime.writable:
            return Result(
                ok=False,
                error="FORBIDDEN",
                message="Writes are disabled for this local server",
            )
        if not key or len(key) > 200:
            return Result(
                ok=False,
                error="INVALID_ARGUMENT",
                message="A nonempty idempotency key is required",
            )
        command_class = self.commands.get(operation)
        if command_class is None:
            return Result(
                ok=False, error="UNSUPPORTED", message="Operation is not implemented"
            )
        command = command_class(self.runtime, identity, payload)
        try:
            invalid = await command.validate()
            if invalid is not None:
                return invalid
        except (ValueError, jsonschema.ValidationError, jsonschema.SchemaError):
            return Result(
                ok=False,
                error="INVALID_INPUT",
                message="Invalid identifier or parameters",
            )
        except (httpx.HTTPError, PrefectException, OSError, TimeoutError):
            return Result(
                ok=False,
                error="PREFECT_UNAVAILABLE",
                message="Unable to validate operation",
            )

        digest = hashlib.sha256(
            json.dumps(
                [operation, identity, payload], sort_keys=True, default=str
            ).encode()
        ).hexdigest()
        try:
            (
                request_hash,
                saved_result,
                inserted,
            ) = await self.runtime.store.reserve_command(key, operation, digest)
        except SQLAlchemyError:
            logging.getLogger(__name__).error("工作流命令登记失败：%s", operation)
            return Result(
                ok=False,
                error="STORAGE_UNAVAILABLE",
                message="Command could not be recorded",
            )
        if request_hash != digest:
            return Result(
                ok=False,
                error="IDEMPOTENCY_CONFLICT",
                message="Key was used with different arguments",
            )
        if saved_result:
            return Result.model_validate_json(saved_result)
        if not inserted and not command.retry_pending:
            return Result(
                ok=False,
                error="OUTCOME_UNKNOWN",
                message="Cancellation is pending or its outcome is unknown; inspect the run",
            )
        try:
            result = await command.perform(key)
            await self.runtime.store.complete_command(key, result.model_dump_json())
            return result
        except Exception:  # noqa: BLE001 - 提交后任何故障都可能导致执行结果未知
            logging.getLogger(__name__).error("工作流命令执行结果未知：%s", operation)
            return Result(
                ok=False,
                error="OUTCOME_UNKNOWN",
                message="Operation may have reached Prefect; inspect the run before retrying",
            )
