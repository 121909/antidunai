# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.8.3 AS uv

FROM python:3.12.11-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

COPY --from=uv /uv /usr/local/bin/uv

RUN groupadd --gid 10001 burst-guard \
    && useradd --uid 10001 --gid burst-guard --home-dir /app --create-home burst-guard

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable \
    && mkdir -p /app/data \
    && chown -R burst-guard:burst-guard /app

USER 10001:10001
EXPOSE 8080
VOLUME ["/app/data"]

ENTRYPOINT ["python", "-m", "burst_guard"]
