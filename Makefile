.DEFAULT_GOAL := help
COMPOSE := docker compose
CLI := $(COMPOSE) run --rm cli

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

# --- local development --------------------------------------------------------

setup: ## Install everything into .venv
	uv sync --all-extras

test: ## Unit tests (Spark tests need Java 17+)
	uv run pytest

lint: ## Ruff lint and format check
	uv run ruff check .
	uv run ruff format --check .

contracts: ## Validate contracts and confirm dbt sources match them
	uv run grainflow contracts check
	uv run grainflow contracts dbt-sources --check

dbt-ci: ## Build every dbt model and test on DuckDB fixtures
	cd transform && DBT_TARGET=ci uv run dbt build --profiles-dir . --target-path target-ci

fixtures: ## Regenerate synthetic CI fixtures
	uv run python scripts/make_fixtures.py

check: lint contracts test dbt-ci ## Everything CI runs

# --- the stack ----------------------------------------------------------------

up: ## Start the full stack
	$(COMPOSE) up -d --build

down: ## Stop the stack, keep data
	$(COMPOSE) down

reset: ## Stop the stack and delete all data
	$(COMPOSE) down -v

backfill: ## Load price history, two days of weather and crop condition
	$(CLI) grainflow produce price-history --period 2y
	$(CLI) grainflow produce weather --once
	$(CLI) grainflow load crops --commodity CORN
	$(CLI) grainflow load crops --commodity SOYBEANS

transform: ## Run dbt against Trino now instead of waiting for the schedule
	$(CLI) dbt build --project-dir transform --profiles-dir transform

query: ## Peek at the weekly corn belt mart
	$(COMPOSE) exec trino trino --catalog lake --output-format ALIGNED --execute \
	  "select * from gold.mart_corn_belt_weekly order by week_start desc limit 12"

.PHONY: help setup test lint contracts dbt-ci fixtures check up down reset backfill transform query
