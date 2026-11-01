# Prefect Agent MCP Runtime（开发中）

本项目提供 Prefect FastAPI 的 MCP 挂载适配器，不创建或启动 Web 服务，也不代理到另一个服务。挂载后，MCP 路径为 Prefect API 下的 `/api/mcp/`，复用现有应用、进程、端口和 Prefect API 认证。

## 接入现有后端

在创建 Prefect FastAPI 应用的现有代码中，应用创建完成、Web 服务启动之前调用：

```python
from prefect_agent_mcp.app import install_prefect_mcp

# app 是现有 Prefect Server 使用的 FastAPI 应用
install_prefect_mcp(app)
```

适配器使用传入的 Prefect 应用，不会创建第二个 Prefect Server；Prefect 与 MCP 的生命周期一起启动和关闭。必须在 Prefect Server 的 Python 环境安装本项目，修改接入代码后重启该服务。

**原生 `prefect server start` 当前不能自动加载这个适配器。** 已核查的 Prefect 3.9 公开插件钩子不提供 Server 路由注册；仅安装本项目并继续运行原生命令，不会出现 MCP 端点。要在不修改 Prefect 源码的情况下接入，需要调整启动入口，让它调用上面的挂载函数。若必须原样保留该命令，还需要 Prefect 官方提供挂载钩子，或修改其启动实现；本项目没有通过私有猴子补丁绕过这一限制。

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

## 示例工作流

示例执行进程连接到现有 Prefect Server：

```powershell
$env:PREFECT_API_URL='http://127.0.0.1:4200/api'
$env:PREFECT_API_AUTH_STRING='<用户名:密码>' # 服务启用认证时配置
uv run python examples/intelligent_document_pipeline.py
```

示例使用 Prefect 的 `serve` 监听待运行的 Deployment，不启动第二个 Web 服务。首次运行后，从输出的 Deployment 地址复制 UUID 填入白名单。示例 LangGraph 检查点使用独立 SQLite，可通过 `PREFECT_MCP_LANGGRAPH_DB` 指定路径。

## 支持情况

| 能力 | 当前实现 |
| --- | --- |
| 工作流发现 | 操作员 JSON 白名单及部署参数 Schema；无自动注册、静态 DAG 或 Task Schema 发现 |
| Flow 操作 | 参数校验、幂等启动和请求取消；无暂停、恢复、重新运行或审批 |
| Task 控制 | 明确拒绝任意 Task 独立重试；无参数覆盖、受控重放或副作用策略 |
| 运行观察 | 当前 Flow 状态和分页 Task 列表，SQLite 保存已观察到的变化；未知的输入、输出和进度标记为不可用 |
| 后台采样 | 每 30 秒采样每个工作流最近 20 个 Flow、每个 Flow 最多 100 个 Task；快速变化、更多运行和离线期间变化可能丢失 |
| 订阅 | 单进程 MCP 订阅总线；提交快照后发送资源变更通知；无跨进程通知或消息重放 |
| 示例 | 五个固定 Prefect Task；分析步骤是确定性 LangGraph 演示，不调用大模型；未实现人工控制点、统一埋点或完整血缘 |
| 安全 | 复用 Prefect API 的认证；脱敏无法保证发现文本中的所有敏感信息。缺少细粒度身份、审批、审计和提示注入防护，不可用于生产 |
| 数据库 | 仅有 snapshots、events 和 commands 三张表；不满足完整事件模型、保留策略、重建或对账要求 |

`retry_task` 当前返回 `UNSUPPORTED`。Timeline 是采样观察，不是 Prefect 完整状态历史；取消结果未知时应先检查实际运行状态。

## 测试

```powershell
uv sync --locked
uv run pytest -q
```

真实集成测试默认跳过。要运行它们，需先准备一个**已调用挂载函数的现有 Prefect Server**、带认证的示例 Deployment 和白名单，并设置 `PREFECT_API_URL`、`PREFECT_API_AUTH_STRING`、`PREFECT_MCP_TEST_DEPLOYMENT_ID`、`PREFECT_MCP_TEST_URL`、`PREFECT_MCP_TEST_USER`、`PREFECT_MCP_TEST_PASSWORD` 以及与示例执行进程一致的 `PREFECT_MCP_LANGGRAPH_DB`。测试会创建真实运行。设计依据和缺口见 [技术核实](docs/architecture.md)。
