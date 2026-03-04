# ═══════════════════════════════════════════════════════════
# Spark-ling MCP Server — Docker Image
# ═══════════════════════════════════════════════════════════
# Lightweight container for deploying the MCP server on
# EC2, ECS/Fargate, or any Docker host.
#
# Build:
#   docker build -t sparkling-mcp -f mcp/Dockerfile .
#
# Run locally:
#   docker run -p 8080:8080 \
#     -e MCP_TRANSPORT=sse \
#     -e MCP_BACKEND=athena \
#     -e S3_BUCKET=sparkling-data-test \
#     -e AWS_REGION=ap-southeast-1 \
#     -v ~/.aws:/home/app/.aws:ro \
#     sparkling-mcp
#
# Run with .env file:
#   docker run -p 8080:8080 --env-file mcp/.env sparkling-mcp
# ═══════════════════════════════════════════════════════════

FROM python:3.11-slim AS base

# Metadata
LABEL maintainer="Spark-ling Team"
LABEL description="MCP server for AI-assisted banking data exploration"
LABEL version="1.0"

# Security: run as non-root
RUN groupadd -r app && useradd -r -g app -m app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/sparkling-mcp

# Install Python dependencies first (layer caching)
COPY mcp/requirements.txt mcp/requirements.txt
RUN pip install --no-cache-dir -r mcp/requirements.txt

# Copy MCP server code
COPY mcp/ mcp/
COPY configs/ configs/
COPY src/ src/

# Copy project metadata (for the resource endpoint)
COPY README.md .

# Fix permissions
RUN chown -R app:app /opt/sparkling-mcp

USER app

# Environment defaults for remote deployment
ENV MCP_TRANSPORT=sse
ENV MCP_PORT=8080
ENV MCP_HOST=0.0.0.0
ENV MCP_BACKEND=athena
ENV MCP_LOG_LEVEL=INFO
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/sparkling-mcp

# Health check — verify server is responding
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${MCP_PORT}/health 2>/dev/null || \
        curl -f http://localhost:${MCP_PORT}/sse 2>/dev/null || exit 1

EXPOSE ${MCP_PORT}

# Start the MCP server
CMD ["python", "-m", "mcp.server"]
