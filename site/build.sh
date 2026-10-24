#!/usr/bin/env bash
# Builds the public dashboard: fetch sources into DuckDB, run the dbt models,
# export JSON, copy the static page. Vercel runs this on every deploy; the
# refresh-site workflow triggers a deploy every morning.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi

uv sync --frozen --no-dev --extra publish --python 3.12

uv run grainflow publish load
uv run dbt build --project-dir transform --profiles-dir transform \
  --target publish --target-path target-publish
uv run grainflow publish export --out site/dist

cp site/index.html site/app.js site/dist/
mkdir -p site/dist/images
cp docs/images/*.png site/dist/images/
echo "site built in site/dist"
