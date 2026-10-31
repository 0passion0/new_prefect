# Prefect Agent MCP Runtime（开发中）

本项目将 MCP 接口挂载在 Prefect 自身的 FastAPI 中：**一个进程、一个 HTTP 端口**，Prefect API、界面和 MCP 分别位于 `/api`、`/` 和 `/api/mcp/`。它仍是本地原型，完整能力和安全限制见下文。

## 启动

需要 Python 3.12–3.14、uv 和现有 Prefect Deployment。在项目目录运行：

```powershell
uv sync --locked
$env:PREFECT_SERVER_API_HOST='127.0.0.1'
$env:PREFECT_SERVER_API_PORT='4200'
uv run prefect-agent-mcp
```

此命令启动的就是带 MCP 路由的 Prefect Server，保留 Prefect 原有 API、界面和后台服务。**不要再启动第二个 `prefect server start`**。如果 Prefect Server 已在运行，需停止它，再使用上述命令启动；不能将路由注入已运行的进程。

启动后，Prefect API 是 `http://127.0.0.1:4200/api`，MCP 地址是 `http://127.0.0.1:4200/api/mcp/`。MCP 使用 Prefect API 原有的认证中间件。默认只读；要启用启动或取消操作，必须在启动前同时配置 Prefect 认证和写权限：

```powershell
$env:PREFECT_SERVER_API_AUTH_STRING='<用户名:密码>'
$env:PREFECT_MCP_ALLOW_WRITE='1'
uv run prefect-agent-mcp
```

MCP 客户端使用相同的 HTTP Basic 凭据。该开关赋予通过认证的客户端相同写权限，**不是细粒度的用户或角色授权**。默认绑定本机；对外部署仍需 TLS、可信网关及权限设计。MCP SDK 默认只接受本机 Host，远程 Host 必须先配置合适的传输安全策略。

在服务启动前，用已知 Deployment UUID 创建 `workflows.json` 白名单：

```json
{"workflows": [{"workflow_id": "document", "deployment_id": "00000000-0000-0000-0000-000000000000", "description": "文档示例", "version": "1"}]}
```

可通过 `PREFECT_MCP_REGISTRY` 指定白名单路径，变更后重启服务。监控数据库默认是当前目录下的 `prefect-monitor.db`，可通过 `PREFECT_MCP_DB` 指定。初始化或升级监控数据库：

```powershell
uv run alembic upgrade head
```

服务启动时也会创建当前版本的表；未来迁移必须在启动前执行。SQLite 启用 WAL、外键和 busy timeout。监控数据库和 LangGraph 检查点应由操作系统权限保护。

## 示例工作流

在另一个终端设置 Prefect 地址，并按需设置服务相同的认证信息，运行示例执行进程：

```powershell
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'
$env:PREFECT_API_AUTH_STRING='<用户名:密码>' # 服务启用认证时配置
uv run python examples/intelligent_document_pipeline.py
```

示例使用 Prefect 的 `serve` 监听待运行的 Deployment，它是工作流执行进程，不启动第二个 Web 服务。首次运行后，从输出的 Deployment 地址复制 UUID 填入 `workflows.json`，重启集成服务使白名单生效。示例 LangGraph 检查点使用独立 SQLite，可通过 `PREFECT_MCP_LANGGRAPH_DB` 指定路径。

## 支持情况

| 能力 | 当前实现 |
| --- | --- |
| 工作流发现 | 仅操作员 JSON 白名单；读取已登记 Deployment 的参数 Schema。无自动注册、静态 DAG 或 Task Schema 发现 |
| Flow 操作 | 经 Schema 检查的启动、Prefect 幂等键及请求取消；无暂停、恢复、重新运行或审批 |
| Task 控制 | 明确拒绝任意 Task 独立重试；无参数覆盖、受控重放或副作用策略 |
| 运行观察 | 当前 Flow 状态和分页 Task 列表，SQLite 存储已观察到的变化；捕获不到的输入、输出和进度标记为不可用 |
| 后台采样 | 每 30 秒对每个工作流最多采样最近 20 个 Flow、每个 Flow 最多 100 个 Task；快速变化、更多运行、离线期间变化可能丢失，不能视为完整事件或审计日志 |
| 订阅 | 单进程 MCP 2026 订阅总线；快照提交后发送资源变更通知，断线需重读；没有跨进程通知或消息重放 |
| 示例 | 五个固定 Prefect Task；分析步骤为确定性 LangGraph 演示，不调用大模型。未实现人工控制点、统一埋点、完整血缘或远端 Worker 数据同步 |
| 安全 | 使用 Prefect API 的认证；常见字段名脱敏只是辅助，不保证发现文本中的敏感信息。缺少细粒度身份、角色、审批、审计及提示注入防护，不可用于生产 |
| 数据库 | 目前仅有 snapshots、events 和 commands 三张表；不满足完整事件模型、保留策略、重建或对账要求 |

当前 Tools：`list_workflows`、`describe_workflow`、`list_flow_runs`、`get_flow_run`、`list_task_runs`、`get_run_timeline`、`start_flow`、`cancel_flow`、`retry_task`。后者返回 `UNSUPPORTED`。Timeline 是采样观察，而非 Prefect 完整状态历史。控制请求的幂等键在本地数据库中唯一，启动还使用 Prefect 原生幂等键；取消结果未知时需先检查实际运行状态。

## 测试

```powershell
uv run pytest -q
```

真实集成测试默认跳过。先启动上述带认证的集成服务和示例执行进程，配置示例 Deployment 白名单并重启服务，然后运行：

```powershell
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'
$env:PREFECT_API_AUTH_STRING='<用户名:密码>'
$env:PREFECT_MCP_TEST_DEPLOYMENT_ID='<示例 Deployment UUID>'
$env:PREFECT_MCP_TEST_URL='http://127.0.0.1:4200'
$env:PREFECT_MCP_TEST_USER='<用户名>'
$env:PREFECT_MCP_TEST_PASSWORD='<密码>'
uv run pytest -q
```

测试会创建真实运行。尚未验证远端 Worker、多进程服务和完整需求列出的所有验收场景。设计、版本依据及缺口见 [技术核实](docs/architecture.md)。
