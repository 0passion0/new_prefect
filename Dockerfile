FROM ghcr.io/astral-sh/uv:0.8.0 AS uv
FROM python:3.13-slim
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY prefect_agent_mcp ./prefect_agent_mcp
COPY alembic.ini ./
COPY migrations ./migrations
RUN uv sync --locked --no-dev && mkdir /data && chown 10001:10001 /data
USER 10001:10001
ENV HOME=/data PREFECT_HOME=/data/prefect PREFECT_MCP_DB=/data/prefect-monitor.db
ENTRYPOINT ["/app/.venv/bin/prefect-agent-mcp"]
