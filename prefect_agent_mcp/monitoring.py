"""定期采集工作流状态。"""

import asyncio
import logging
from uuid import UUID

from .runtime import Runtime


class Monitor:
    """定时采集已注册工作流的近期运行。"""

    def __init__(self, runtime: Runtime) -> None:
        """保存采集使用的运行时。

        :param runtime: 工作流查询服务。
        """
        self.runtime = runtime

    async def run(self) -> None:
        """持续采样；单个工作流或任务出错不会中断后续采样。"""
        while True:
            for workflow in self.runtime.registry.workflows:
                try:
                    recent = await self.runtime.flows(workflow.workflow_id, 20, 0)
                    if recent.ok:
                        for item in recent.data.get("items", []):
                            try:
                                await self.runtime.tasks(UUID(item["id"]), 100, 0)
                            except Exception:
                                logging.getLogger(__name__).warning(
                                    "Flow %s 任务状态采集失败",
                                    item.get("id"),
                                    exc_info=True,
                                )
                except Exception:
                    logging.getLogger(__name__).warning(
                        "工作流 %s 状态采集失败",
                        workflow.workflow_id,
                        exc_info=True,
                    )
            await asyncio.sleep(30)
