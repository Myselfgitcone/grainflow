FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN pip install --no-cache-dir uv==0.13.5

WORKDIR /app

# Dependencies first so code edits don't reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra orchestration --no-install-project

COPY . .
RUN uv sync --frozen --no-dev --extra orchestration

ENV PATH="/app/.venv/bin:$PATH" \
    DBT_TARGET=trino

# Bake the dbt manifest so Dagster can load without touching the warehouse.
RUN dbt parse --project-dir transform --profiles-dir transform --target trino

CMD ["grainflow", "--help"]
