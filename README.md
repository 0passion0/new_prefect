# Prefect Agent MCP Runtime（开发中）

本项目是一个**尚未完成验收的本地原型**。它使用 Prefect 作为唯一编排引擎，提供 MCP stdio 接口、Deployment 白名单、原生启动与取消请求、有限状态采样和本地快照。不要将它部署为多用户服务，也不要在其中处理秘密或敏感文档。

## 环境和启动

需要 Python 3.12–3.14、uv，以及可访问的 Prefect 3 Server。服务端和执行进程都必须连接同一个 Prefect Server；必须启用 Prefect 的后台服务，否则 Task Run 数据可能不可用。以下命令在项目根目录执行：

```powershell
uv sync --locked
uv run prefect server start --host 127.0.0.1 --port 4200
```

在另一个终端运行示例：

```powershell
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'
uv run python examples/intelligent_document_pipeline.py
```

从输出的 Deployment 地址复制 UUID，建立 `workflows.json`：

```json
{"workflows": [{"workflow_id": "document", "deployment_id": "00000000-0000-0000-0000-000000000000", "description": "文档示例", "version": "1"}]}
```

在 MCP 宿主进程的环境中设置 `PREFECT_API_URL` 和 `PREFECT_MCP_REGISTRY`（指向该 JSON 文件），命令为 `uv run --directory K:\new_prefect prefect-agent-mcp`，或使用安装后的 `prefect-agent-mcp`。默认禁止写操作；仅当宿主和所有本机用户均可信时，可由操作者设置 `PREFECT_MCP_ALLOW_WRITE=1`。这是进程级开关，不是用户认证或细粒度授权。勿暴露为远程服务。数据库默认保存在当前目录的 `prefect-monitor.db`；可用 `PREFECT_MCP_DB` 指定路径。示例 LangGraph 的独立数据库由 `PREFECT_MCP_LANGGRAPH_DB` 指定。

初始化或升级数据库：

```powershell
uv run alembic upgrade head
```

现有服务启动时也会创建当前版本的表；未来迁移必须在启动前执行。SQLite 启用 WAL、外键和 busy timeout，数据库及 LangGraph 检查点应由操作系统权限保护。MCP 日志仅输出到 stderr。

## 支持情况

| 能力 | 当前实现 |
| --- | --- |
| 工作流发现 | 仅操作员 JSON 白名单；读取已登记 Deployment 的参数 Schema。无自动注册、静态 DAG 或 Task Schema 发现 |
| Flow 操作 | 经 Schema 检查的启动、Prefect 幂等键及请求取消；无暂停、恢复、重新运行或审批 |
| Task 控制 | 明确拒绝任意 Task 独立重试；无参数覆盖、受控重放或副作用策略 |
| 运行观察 | 当前 Flow 状态和分页 Task 列表，SQLite 存储已观察到的变化；捕获不到的输入、输出和进度标记为不可用 |
| 后台采样 | 每 30 秒对每个工作流最多采样最近 20 个 Flow、每个 Flow 最多 100 个 Task；快速状态变化、更多运行、离线期间变化可能丢失，不能视为完整事件或审计日志 |
| 订阅 | 单进程 MCP 2026 订阅总线；快照提交后发送相关 Resource 变更通知，断线需重读；没有跨进程通知或消息重放 |
| 示例 | 五个固定 Prefect Task；分析步骤为确定性 LangGraph 演示，不调用大模型。图状态存入本地 SQLite，禁用 Pickle，并限制反序列化类型。未实现人工控制点、统一埋点、完整血缘或远端 Worker 数据同步 |
| 安全 | stdio 本地信任边界、默认禁止写入、限制参数大小并检查已知参数；常见字段名脱敏只是辅助，不保证发现文本中的敏感信息。缺少身份、角色、审批、审计及提示注入防护，不可用于生产 |
| 数据库 | 目前仅有 snapshots、events 和 commands 三张表；不满足完整事件模型、保留策略、重建或对账要求 |

当前 Tools：`list_workflows`、`describe_workflow`、`list_flow_runs`、`get_flow_run`、`list_task_runs`、`get_run_timeline`、`start_flow`、`cancel_flow`、`retry_task`。后者总是返回 `UNSUPPORTED`。操作结果使用 `ok`、`data`、`error`、`message`、`stale` 和 `last_synced_at`；连接失败时只有曾保存的授权 Flow 快照支持降级。Timeline 是**采样观察**，而非 Prefect 完整状态历史。

当前 Resources：`prefect-monitor://workflows`、`prefect-monitor://workflows/{workflow_id}`、`prefect-monitor://flows/{flow_run_id}`、`prefect-monitor://flows/{flow_run_id}/tasks`、`prefect-monitor://flows/{flow_run_id}/timeline`。订阅只是变化提示，查询才是读取入口。

控制请求的幂等键在本地数据库中唯一，启动还使用 Prefect 的原生幂等键。取消请求发生通信或数据库错误时可能处于未知状态；先检查实际 Flow 状态。幂等机制没有跨数据库或多实例协调。

## 测试

```powershell
uv run pytest -q
```

真实集成测试默认跳过。启动上述 Server 和示例后：

```powershell
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'
$env:PREFECT_MCP_TEST_DEPLOYMENT_ID='<示例 Deployment UUID>'
uv run pytest -q
```

测试会创建真实运行。尚未验证远端 Worker、服务断线重连和完整需求列出的所有验收场景。设计、版本依据和缺口见 [技术核实](docs/architecture.md)。
