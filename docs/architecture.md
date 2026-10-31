# 技术核实和架构记录

基准日期：2026-10-31。当前锁文件使用 Python 3.13.5 验证：Prefect 3.9.0、MCP Python SDK 2.3.0、Pydantic 2.14.2、LangGraph 1.2.19、langgraph-checkpoint-sqlite 3.1.3、SQLAlchemy 2.1.6。实际解析结果以 `uv.lock` 为准。查询时 PyPI 还可见 Prefect 3.9.1；这里保留需求指定的 3.9.0，以免在未验证升级影响时变更基线。

## 核实来源

- [MCP Python SDK 2.3.0](https://pypi.org/project/mcp/2.3.0/)：`from mcp.server import MCPServer`；MCP 2026 订阅从 `subscriptions/listen` 接收，不能依靠旧 session 通知方法。
- [Prefect API client](https://docs.prefect.io/v3/advanced/api-client)：`get_client` 和 Flow/Task 查询。`create_flow_run_from_deployment` 支持幂等键。取消请求使用 `CANCELLING`，不是直接强设 `CANCELLED`。
- [Prefect pause workflow](https://docs.prefect.io/v3/how-to-guides/workflows/pause-workflows)：暂停需要运行中的 Flow 主动到达暂停点，不能把远程任意暂停当成普遍能力。
- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence)：图 Checkpoint 和 Prefect Result 的职责不同；SQLite Checkpointer 适合本地实验。
- [Pydantic PyPI](https://pypi.org/project/pydantic/2.14.2/)、[LangGraph PyPI](https://pypi.org/project/langgraph/1.2.19/)、[Prefect PyPI](https://pypi.org/project/prefect/3.9.0/)。依赖已通过 uv 解析安装，真实示例也已运行，但这不代表全部功能和环境组合均已验证。

## 当前边界

外部 MCP 客户端 -> Prefect FastAPI（同一端口的 `/api/mcp/`）-> 进程内 Prefect 客户端 -> Prefect API。示例 `serve` 进程仅负责执行工作流。SQLite 只存采样后的元信息及命令键，不修改 Prefect 数据库。后台轮询提交快照之后向本进程订阅总线发布提示。LangGraph 的 Checkpoint 在执行流程的进程中写入独立本地 SQLite。

## 离最终需求仍缺少

完整基于 Prefect Events 的可重放采集、同步游标与对账、远端 Worker 埋点、数据血缘和输出引用、Schema 注册及迁移、带身份的权限与审批、可编辑检查点和受控重放、全面审计、任务错误和进度、断线恢复、跨实例订阅、真实端到端异常和恢复测试。现有采样事件无法补全未观察到的状态，不能作为这些能力的替代。实际部署必须先完成这些能力、安全评审和容量评估。

## 代码职责与扩展

- `Runtime` 负责白名单和运行查询；注入的 Prefect 客户端直接访问本进程的 Prefect API，回调只负责通知资源变化。
- `FlowCommand` 定义控制命令的校验和执行接口；`StartFlowCommand`、`CancelFlowCommand` 分别实现具体语义。`CommandService` 统一处理权限、幂等和执行结果，新增命令应继承基类并登记到命令映射，同时明确未完成请求能否安全重试。
- `Store` 负责快照、观察记录和命令记录的数据库事务；`Monitor` 负责定期采集。
- MCP 工具和资源边界转换内部错误，工具结果先经过统一契约及 JSON 序列化验证；参数校验仍由 Pydantic 和部署 Schema 执行。Prefect 任务失败仍由 Prefect 记录，避免将失败误报为成功。
