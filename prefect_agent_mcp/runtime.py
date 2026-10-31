"""通过部署白名单查询 Prefect 运行并记录观测数据。"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterId,
)
from prefect.client.schemas.sorting import FlowRunSort
from prefect.exceptions import PrefectException

from .schemas import Registry, Result, Workflow, redact, utcnow
from .store import Store


class Runtime:
    """管理授权工作流的查询和状态快照。"""

    def __init__(
        self,
        store: Store,
        registry: Registry,
        writable: bool = False,
        notify: Callable[[str], Awaitable[None]] | None = None,
        client_factory: Callable[[], PrefectClient] | None = None,
    ) -> None:
        """配置运行时依赖。

        :param store: 监控数据库。
        :param registry: 操作员配置的白名单。
        :param writable: 是否允许本地进程执行写操作。
        :param notify: 快照提交后使用的资源变更回调。
        :param client_factory: 创建 Prefect 客户端的工厂。
        """
        self.store = store
        self.registry = registry
        self.writable = writable
        self.notify = notify
        self.client_factory = client_factory

    @classmethod
    def from_environment(cls) -> Runtime:
        """从环境变量加载运行时配置。

        :return: 已配置的运行时。
        """
        registry_path = Path(os.getenv("PREFECT_MCP_REGISTRY", "workflows.json"))
        registry = (
            Registry.model_validate_json(registry_path.read_text(encoding="utf-8"))
            if registry_path.exists()
            else Registry()
        )
        return cls(
            Store(os.getenv("PREFECT_MCP_DB", "prefect-monitor.db")),
            registry,
            os.getenv("PREFECT_MCP_ALLOW_WRITE") == "1",
        )

    def client(self) -> PrefectClient:
        """创建访问当前 Prefect API 的客户端。

        :return: Prefect 客户端。
        """
        if self.client_factory is not None:
            return self.client_factory()
        return get_client()

    def workflow(self, workflow_id: str) -> Workflow:
        """查找白名单中的工作流。

        :param workflow_id: 已注册的工作流标识。
        :return: 工作流定义。
        :raises ValueError: 工作流未注册。
        """
        for workflow in self.registry.workflows:
            if workflow.workflow_id == workflow_id:
                return workflow
        raise ValueError("Workflow is not registered")

    def authorized(self, deployment_id: Any) -> bool:
        """检查部署是否在白名单中。

        :param deployment_id: Prefect 部署标识。
        :return: 部署是否已授权。
        """
        return any(
            str(item.deployment_id) == str(deployment_id)
            for item in self.registry.workflows
        )

    async def describe(self, workflow_id: str) -> Result:
        """读取已注册部署的描述和参数 Schema。

        :param workflow_id: 已注册的工作流标识。
        :return: 部署描述或错误信息。
        """
        try:
            workflow = self.workflow(workflow_id)
            async with self.client() as client:
                deployment = await client.read_deployment(workflow.deployment_id)
            return Result(
                ok=True,
                data={
                    **workflow.model_dump(mode="json"),
                    "flow_input_schema": redact(deployment.parameter_openapi_schema),
                    "graph": None,
                    "capabilities": {
                        "start": self.writable,
                        "cancel": self.writable,
                        "pause": False,
                        "resume": False,
                        "task_retry": False,
                    },
                },
            )
        except ValueError:
            return Result(
                ok=False, error="NOT_REGISTERED", message="Workflow is not registered"
            )
        except (httpx.HTTPError, PrefectException, OSError, TimeoutError):
            return Result(
                ok=False,
                error="PREFECT_UNAVAILABLE",
                message="Could not read deployment",
            )

    async def flow(self, flow_run_id: UUID) -> Result:
        """读取运行状态，无法连接时使用最近快照。

        :param flow_run_id: 工作流运行标识。
        :return: 实时或带过期标记的运行状态。
        """
        try:
            async with self.client() as client:
                run = await client.read_flow_run(flow_run_id)
            if not self.authorized(run.deployment_id):
                return Result(
                    ok=False,
                    error="NOT_REGISTERED",
                    message="Run does not belong to a registered deployment",
                )
            data = {
                "id": str(run.id),
                "deployment_id": str(run.deployment_id),
                "name": run.name,
                "state": str(run.state_type.value) if run.state_type else None,
                "run_count": run.run_count,
                "start_time": str(run.start_time) if run.start_time else None,
                "end_time": str(run.end_time) if run.end_time else None,
                "inputs": {"available": False},
                "outputs": {"available": False},
                "progress": None,
            }
            changed = await self.store.save("flow", str(run.id), str(run.id), data)
            if changed and self.notify:
                await self.notify(f"prefect-monitor://flows/{run.id}")
                await self.notify(f"prefect-monitor://flows/{run.id}/timeline")
            return Result(ok=True, data=data, last_synced_at=utcnow())
        except (httpx.HTTPError, PrefectException, OSError, TimeoutError):
            cached = await self.store.get("flow", str(flow_run_id))
            if cached and self.authorized(cached[0].get("deployment_id")):
                return Result(
                    ok=True,
                    data=cached[0],
                    stale=True,
                    last_synced_at=datetime.fromisoformat(cached[1]),
                )
            return Result(
                ok=False,
                error="PREFECT_UNAVAILABLE_OR_NOT_FOUND",
                message="Run could not be read and no authorized snapshot is available",
            )

    async def tasks(self, flow_run_id: UUID, limit: int, offset: int) -> Result:
        """分页读取已授权运行的任务。

        :param flow_run_id: 工作流运行标识。
        :param limit: 每页最多返回的记录数。
        :param offset: 分页偏移量。
        :return: 任务列表或错误信息。
        """
        parent = await self.flow(flow_run_id)
        if not parent.ok:
            return parent
        if parent.stale:
            return Result(
                ok=False,
                error="PREFECT_UNAVAILABLE",
                message="Live task list unavailable",
                stale=True,
                last_synced_at=parent.last_synced_at,
            )
        try:
            async with self.client() as client:
                runs = await client.read_task_runs(
                    flow_run_filter=FlowRunFilter(
                        id=FlowRunFilterId(any_=[flow_run_id])
                    ),
                    limit=limit,
                    offset=offset,
                )
            result = []
            for run in runs:
                item = {
                    "id": str(run.id),
                    "flow_run_id": str(flow_run_id),
                    "name": run.name,
                    "state": run.state_type.value if run.state_type else None,
                    "run_count": run.run_count,
                    "start_time": str(run.start_time) if run.start_time else None,
                    "end_time": str(run.end_time) if run.end_time else None,
                    "inputs": {"available": False},
                    "outputs": {"available": False},
                    "progress": None,
                }
                changed = await self.store.save(
                    "task", str(run.id), str(flow_run_id), item
                )
                if changed and self.notify:
                    await self.notify(f"prefect-monitor://flows/{flow_run_id}/tasks")
                    await self.notify(f"prefect-monitor://flows/{flow_run_id}/timeline")
                result.append(item)
            return Result(
                ok=True,
                data={"items": result, "offset": offset, "limit": limit},
                last_synced_at=utcnow(),
            )
        except (httpx.HTTPError, PrefectException, OSError, TimeoutError):
            return Result(
                ok=False,
                error="PREFECT_UNAVAILABLE",
                message="Task runs could not be read",
            )

    async def flows(self, workflow_id: str, limit: int, offset: int) -> Result:
        """分页读取指定工作流的近期运行。

        :param workflow_id: 已注册的工作流标识。
        :param limit: 每页最多返回的记录数。
        :param offset: 分页偏移量。
        :return: 分页后的运行列表。
        """
        try:
            workflow = self.workflow(workflow_id)
            async with self.client() as client:
                runs = await client.read_flow_runs(
                    flow_run_filter=FlowRunFilter(
                        deployment_id=FlowRunFilterDeploymentId(
                            any_=[workflow.deployment_id]
                        )
                    ),
                    sort=FlowRunSort.EXPECTED_START_TIME_DESC,
                    limit=limit,
                    offset=offset,
                )
            return Result(
                ok=True,
                data={
                    "items": [
                        {
                            "id": str(run.id),
                            "state": run.state_type.value if run.state_type else None,
                        }
                        for run in runs
                    ],
                    "limit": limit,
                    "offset": offset,
                },
            )
        except ValueError:
            return Result(
                ok=False, error="NOT_REGISTERED", message="Workflow is not registered"
            )
        except (httpx.HTTPError, PrefectException, OSError, TimeoutError):
            return Result(
                ok=False,
                error="PREFECT_UNAVAILABLE",
                message="Flow runs could not be read",
            )
