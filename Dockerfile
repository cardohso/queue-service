# One image for the API and the worker: `qs api` / `qs worker` (see docker-compose.yml).
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1

WORKDIR /app
# Dependencies first: this layer is cached until pyproject.toml or uv.lock change.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
RUN uv sync --locked --no-dev

# Not root: nothing here needs it.
RUN useradd --system --uid 10001 qs
USER qs

ENTRYPOINT ["qs"]
CMD ["api"]
