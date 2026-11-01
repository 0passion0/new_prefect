# Prefect Agent MCP Runtime（开发中）

本项目将 MCP 自动挂载到 Prefect FastAPI，不创建额外 Web 服务。MCP 地址是 Prefect API 下的 `/api/mcp/`，复用同一应用、进程、端口和 Prefect API 认证。

## 启动

在项目目录首次安装依赖，然后激活环境：

```powershell
cd K:\new_prefect
uv sync --locked
.\.venv\Scripts\Activate.ps1
```

激活后，在项目目录中运行原生命令（打开新终端时需重新激活环境）：

```powershell
prefect server start
```

Prefect 界面为 `http://127.0.0.1:4200`，MCP 地址为 `http://127.0.0.1:4200/api/mcp/`。安装必须使用项目的 Python 环境；项目目录中的 Prefect 设置启用插件系统，也会启用环境中其他已安装的 Prefect 插件。若显式关闭插件、开启安全模式或使用插件允许名单，请确保允许 `prefect-agent-mcp`，否则不会自动挂载。Prefect 遇到插件加载错误时可能继续启动；如无法连接 MCP，请检查启动日志和插件配置。

Prefect 没有公开的路由注册钩子，本项目通过插件在运行时包装 Prefect 的应用工厂，不修改 Prefect 源码。该适配已针对锁定的 Prefect 3.9.0 和默认前台单进程启动验证；升级 Prefect 或使用后台、多进程启动时需重新验证。

## 配置

MCP 默认只读。开启启动或取消操作前，Prefect Server 必须配置 API 认证，并在**现有 Prefect 进程**启动前设置：

```powershell
$env:PREFECT_SERVER_API_AUTH_STRING='<用户名:密码>'
$env:PREFECT_MCP_ALLOW_WRITE='1'
```

通过认证的 MCP 客户端共享同一写权限；这不是细粒度的用户或角色授权。对外部署仍需要 TLS、可信网关和权限设计。MCP SDK 默认只接受本机 Host，远程 Host 需要配置合适的传输安全策略。

用已知 Deployment UUID 创建 `workflows.json`：

```json
{"workflows": [{"workflow_id": "document", "deployment_id": "00000000-0000-0000-0000-000000000000", "description": "文档示例", "version": "1"}]}
```

可通过 `PREFECT_MCP_REGISTRY` 指定白名单路径，变更后重启 Prefect 进程。监控数据库默认是当前目录下的 `prefect-monitor.db`，可通过 `PREFECT_MCP_DB` 指定。初始化或升级监控数据库：

```powershell
uv run alembic upgrade head
```

适配器启动时也会创建当前版本的数据表；未来迁移必须在启动前执行。SQLite 启用 WAL、外键和 busy timeout。监控数据库和 LangGraph 检查点应由操作系统权限保护。

## 示例工作流与 Worker

示例使用 Process Worker 连接现有 Prefect Server。首次使用时，在项目目录激活环境，创建工作池并部署示例：

```powershell
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'
$env:PREFECT_API_AUTH_STRING='<用户名:密码>' # 服务启用认证时配置
prefect work-pool create mcp-local --type process
prefect deploy 'examples/intelligent_document_pipeline.py:intelligent_document_pipeline' --name mcp-local --pool mcp-local --no-prompt
```

从部署输出复制 UUID 填入白名单并重启 Prefect Server。之后在另一个已配置相同 API 地址和认证信息的终端运行：

```powershell
$env:PREFECT_CLIENT_METRICS_ENABLED='false'
$env:PREFECT_RUNNER_SERVER_ENABLE='false'
prefect worker start --pool mcp-local --install-policy never
```

Worker 负责执行工作流，不启动第二个 Web 服务。示例 LangGraph 检查点使用独立 SQLite，可通过 `PREFECT_MCP_LANGGRAPH_DB` 指定路径。

## 可恢复的 Prefect Task 示例

可恢复示例使用 LangGraph 固定图 `A → B → C`，每个节点执行真正的 Prefect Task。MCP 投递一个独立的恢复执行 Task，由 Task Worker 执行；该 Worker 不提供 Web 服务。确保 API 地址和认证已配置，在另一个终端启动：

```powershell
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'
$env:PREFECT_API_AUTH_STRING='<用户名:密码>' # 服务启用认证时配置
$env:PREFECT_CLIENT_METRICS_ENABLED='false'
prefect task serve --module prefect_agent_mcp.recovery_worker
```

通过 MCP 调用 `start_recoverable_workflow`，例如参数 `{"value":9,"divisor":0,"idempotency_key":"start-1"}`。B 会因除数为零失败。用返回的业务 `run_id` 调用 `get_recoverable_run`，取得 B 的 `task_run_id`；然后调用 `retry_task`：

```json
{"run_id":"业务运行 UUID","task_run_id":"B 的 Task Run UUID","parameters":{"divisor":2},"idempotency_key":"retry-1"}
```

B 使用新输入执行，成功后执行 C；A 的 Task Run 不变。已完成的 A、B 或 C 也可从对应检查点重放，已执行的下游节点会再次执行。`get_recoverable_history` 返回各分支中保存的 Task Run 和输入输出。每次启动和重试都需要唯一幂等键；重复提交相同键和参数返回同一执行任务。

业务 `run_id` 是 LangGraph 的稳定运行标识，不是原 Prefect Flow Run ID。每次重试产生新的 Prefect Task Run，原记录不变。此功能目前只支持内置示例及其无副作用的节点，原有任意 Prefect Flow 不会自动获得重放能力。检查点保存在 `prefect-recovery.db`，可用 `PREFECT_MCP_RECOVERY_DB` 指定；Server 和 Task Worker 必须使用同一主机上的共享数据库路径。示例未实现进程异常退出后的任务自动对账，不能用于生产。

## 支持情况

| 能力 | 当前实现 |
| --- | --- |
| 工作流发现 | 操作员 JSON 白名单及部署参数 Schema；无自动注册、静态 DAG 或 Task Schema 发现 |
| Flow 操作 | 参数校验、幂等启动和请求取消；无暂停、恢复、重新运行或审批 |
| Task 控制 | 内置 A/B/C 示例支持修改输入和检查点重放；不支持任意现有 Flow Task，缺少副作用策略 |
| 运行观察 | 当前 Flow 状态和分页 Task 列表，SQLite 保存已观察到的变化；未知的输入、输出和进度标记为不可用 |
| 后台采样 | 每 30 秒采样每个工作流最近 20 个 Flow、每个 Flow 最多 100 个 Task；快速变化、更多运行和离线期间变化可能丢失 |
| 订阅 | 单进程 MCP 订阅总线；提交快照后发送资源变更通知；无跨进程通知或消息重放 |
| 示例 | 五个固定 Prefect Task；分析步骤是确定性 LangGraph 演示，不调用大模型；未实现人工控制点、统一埋点或完整血缘 |
| 安全 | 复用 Prefect API 的认证；脱敏无法保证发现文本中的所有敏感信息。缺少细粒度身份、审批、审计和提示注入防护，不可用于生产 |
| 数据库 | 监控、命令与业务运行使用 SQLite，检查点单独存储；不满足完整事件模型、保留策略、重建或对账要求 |

`retry_task` 仅适用于上述可恢复示例。Timeline 是采样观察，不是 Prefect 完整状态历史；取消结果未知时应先检查实际运行状态。

## 测试

```powershell
uv sync --locked
uv run pytest -q
```

真实集成测试默认跳过。要运行它们，需先启动 Prefect Server 和恢复 Task Worker，准备带认证的示例 Deployment 和白名单，并设置 `PREFECT_API_URL`、`PREFECT_API_AUTH_STRING`、`PREFECT_MCP_TEST_DEPLOYMENT_ID`、`PREFECT_MCP_TEST_URL`、`PREFECT_MCP_TEST_USER`、`PREFECT_MCP_TEST_PASSWORD` 以及与示例执行进程一致的 `PREFECT_MCP_LANGGRAPH_DB`。测试会创建真实运行。设计依据和缺口见 [技术核实](docs/architecture.md)。
