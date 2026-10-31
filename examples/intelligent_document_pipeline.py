"""包含五个 Prefect 任务和 LangGraph 检查点的文档处理示例。"""

from __future__ import annotations

import os
import re
from typing import Any

import aiosqlite
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import END, START, StateGraph
from prefect import flow, task
from prefect.runtime import flow_run, task_run
from pydantic import BaseModel, ConfigDict, Field


class Model(BaseModel):
    """示例使用的基础数据契约。"""

    model_config = ConfigDict(extra="forbid")


class ValidateInput(Model):
    """文档校验步骤的输入。"""

    document: str = Field(min_length=1, max_length=5000)


class ValidateOutput(Model):
    """已校验的文档。"""

    document: str


class PreprocessInput(Model):
    """文档预处理步骤的输入。"""

    document: str


class PreprocessOutput(Model):
    """预处理后的文档。"""

    text: str


class AnalyzeInput(Model):
    """分析参数与预处理后的文档。"""

    text: str
    max_words: int = Field(default=20, ge=1, le=100)


class AnalyzeOutput(Model):
    """内部状态图的分析结果。"""

    summary: str
    word_count: int = Field(ge=0)


class VerifyInput(AnalyzeOutput):
    """结果校验步骤的输入。"""


class VerifyOutput(AnalyzeOutput):
    """已校验的分析结果。"""


class PersistInput(VerifyOutput):
    """结果保存步骤的输入。"""


class PersistOutput(VerifyOutput):
    """由 Prefect 持久化的工作流结果。"""


class GraphState(Model):
    """内部固定状态图的数据状态。"""

    text: str
    max_words: int
    summary: str = ""
    word_count: int = 0


def analyze_node(state: GraphState) -> dict[str, Any]:
    """生成确定性摘要并统计词数。

    :param state: 已校验的状态图数据。
    :return: 状态图更新内容。
    """
    words = state.text.split()
    result = AnalyzeOutput(
        summary=" ".join(words[: state.max_words]), word_count=len(words)
    )
    return result.model_dump()


@task
async def validate_input(value: dict[str, Any]) -> dict[str, Any]:
    """校验输入文档。

    :param value: 当前步骤的输入数据。
    :return: 已校验的文档数据。
    """
    return ValidateOutput(
        **ValidateInput.model_validate(value).model_dump()
    ).model_dump()


@task
async def preprocess_document(value: dict[str, Any]) -> dict[str, Any]:
    """规范化文档空白字符。

    :param value: 当前步骤的输入数据。
    :return: 预处理后的文档数据。
    """
    parsed = PreprocessInput.model_validate(value)
    return PreprocessOutput(
        text=re.sub(r"\s+", " ", parsed.document).strip()
    ).model_dump()


@task(retries=1, retry_delay_seconds=1)
async def agent_analyze(value: dict[str, Any]) -> dict[str, Any]:
    """使用带检查点的固定状态图分析文档。

    :param value: 当前步骤的输入数据。
    :return: 已校验的分析结果。
    """
    parsed = AnalyzeInput.model_validate(value)
    if not flow_run.id or not task_run.id:
        raise RuntimeError("分析步骤缺少 Prefect 运行上下文")
    thread_id = f"{flow_run.id}:{task_run.id}:{task_run.run_count}"
    path = os.getenv("PREFECT_MCP_LANGGRAPH_DB", "langgraph-checkpoints.db")
    async with aiosqlite.connect(path) as connection:
        serializer = JsonPlusSerializer(
            pickle_fallback=False, allowed_json_modules=[], allowed_msgpack_modules=[]
        )
        saver = AsyncSqliteSaver(connection, serde=serializer)
        builder = StateGraph(GraphState)
        builder.add_node("analyze", analyze_node)
        builder.add_edge(START, "analyze")
        builder.add_edge("analyze", END)
        graph = builder.compile(checkpointer=saver)
        result = await graph.ainvoke(
            GraphState(**parsed.model_dump()),
            {"configurable": {"thread_id": thread_id}},
        )
    return AnalyzeOutput.model_validate(
        {"summary": result["summary"], "word_count": result["word_count"]}
    ).model_dump()


@task
async def validate_result(value: dict[str, Any]) -> dict[str, Any]:
    """校验分析结果。

    :param value: 当前步骤的输入数据。
    :return: 已校验的结果。
    """
    return VerifyOutput(**VerifyInput.model_validate(value).model_dump()).model_dump()


@task(persist_result=True)
async def persist_result(value: dict[str, Any]) -> dict[str, Any]:
    """通过 Prefect 结果存储持久化结果。

    :param value: 当前步骤的输入数据。
    :return: 已持久化的结果。
    """
    return PersistOutput(**PersistInput.model_validate(value).model_dump()).model_dump()


@flow(name="intelligent-document-pipeline", persist_result=True, log_prints=False)
async def intelligent_document_pipeline(
    document: str, max_words: int = 20
) -> dict[str, Any]:
    """执行固定的五步骤文档处理流程。

    :param document: 待处理的文档。
    :param max_words: 摘要最多包含的词数。
    :return: 工作流处理结果。
    """
    validated = await validate_input({"document": document})
    prepared = await preprocess_document(validated)
    analysis = await agent_analyze({**prepared, "max_words": max_words})
    verified = await validate_result(analysis)
    return await persist_result(verified)


if __name__ == "__main__":
    intelligent_document_pipeline.serve(name="local-demo")
