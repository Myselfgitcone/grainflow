"""Weekly crop condition ratings from the USDA NASS Quick Stats API.

Get a free key at https://quickstats.nass.usda.gov/api. NASS returns one row
per state, week and condition class, with the class encoded in unit_desc
("PCT GOOD", "PCT EXCELLENT", ...). Suppressed values come back as "(D)".
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

QUICKSTATS_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
CONDITION_CLASSES = ("VERY POOR", "POOR", "FAIR", "GOOD", "EXCELLENT")


def fetch_condition(
    api_key: str,
    commodity: str,
    year_from: int,
    http: httpx.Client | None = None,
) -> list[dict[str, Any]]:
    params = {
        "key": api_key,
        "source_desc": "SURVEY",
        "sector_desc": "CROPS",
        "commodity_desc": commodity,
        "statisticcat_desc": "CONDITION",
        "agg_level_desc": "STATE",
        "year__GE": year_from,
        "format": "JSON",
    }
    client = http or httpx.Client(timeout=60)
    response = client.get(QUICKSTATS_URL, params=params)
    response.raise_for_status()
    return parse_condition_rows(response.json().get("data", []), commodity)


def parse_condition_rows(rows: list[dict[str, Any]], commodity: str) -> list[dict[str, Any]]:
    records = []
    for row in rows:
        condition = row.get("unit_desc", "").removeprefix("PCT ").strip()
        pct = _number(row.get("Value"))
        week_ending = row.get("week_ending")
        if condition not in CONDITION_CLASSES or pct is None or not week_ending:
            continue

        records.append(
            {
                "commodity": commodity,
                "state_alpha": row.get("state_alpha"),
                "week_ending": week_ending,
                "condition_class": condition,
                "pct": pct,
                "nass_load_time": _load_time(row.get("load_time")),
            }
        )
    return records


def _number(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(str(raw).replace(",", "").strip())
    except ValueError:
        return None  # "(D)", "(NA)", "(Z)" and friends


def _load_time(raw: str | None) -> str | None:
    """NASS publishes load_time without a zone; we keep the wall-clock value as UTC."""
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.strip())
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc).isoformat()
