"""Surface observations from the NOAA / National Weather Service API.

No API key is needed, but NWS asks for a User-Agent that identifies you.
Stations are ASOS sites spread across the main corn belt states so that
weather can be joined to USDA crop condition by state.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Any

import httpx

BASE_URL = "https://api.weather.gov"

CORN_BELT_STATIONS = {
    "KDSM": "IA",  # Des Moines
    "KCID": "IA",  # Cedar Rapids
    "KALO": "IA",  # Waterloo
    "KPIA": "IL",  # Peoria
    "KSPI": "IL",  # Springfield
    "KBMI": "IL",  # Bloomington
    "KIND": "IN",  # Indianapolis
    "KLAF": "IN",  # Lafayette
    "KOMA": "NE",  # Omaha
    "KGRI": "NE",  # Grand Island
    "KMSP": "MN",  # Minneapolis
    "KRST": "MN",  # Rochester
    "KFSD": "SD",  # Sioux Falls
    "KCMH": "OH",  # Columbus
}

# NWS reports values with WMO unit codes. Map each to (target unit, factor).
_UNIT_FACTORS = {
    "wmoUnit:degC": 1.0,
    "wmoUnit:percent": 1.0,
    "wmoUnit:km_h-1": 1.0,
    "wmoUnit:m_s-1": 3.6,  # -> km/h
    "wmoUnit:mm": 1.0,
    "wmoUnit:m": 1000.0,  # -> mm
}


class WeatherClient:
    def __init__(self, user_agent: str, http: httpx.Client | None = None):
        self.http = http or httpx.Client(
            base_url=BASE_URL,
            headers={"User-Agent": user_agent, "Accept": "application/geo+json"},
            timeout=20,
        )

    def recent_observations(self, station_id: str, limit: int = 12) -> list[dict[str, Any]]:
        response = self.http.get(f"/stations/{station_id}/observations", params={"limit": limit})
        response.raise_for_status()
        state = CORN_BELT_STATIONS.get(station_id, "")
        records = (
            parse_observation(station_id, state, feature)
            for feature in response.json().get("features", [])
        )
        return [r for r in records if r is not None]


class SeenObservations:
    """Bounded memory of (station, time) pairs so repeated polls don't resend rows."""

    def __init__(self, capacity: int = 5000):
        self._order: deque[tuple[str, str]] = deque()
        self._seen: set[tuple[str, str]] = set()
        self._capacity = capacity

    def add(self, record: dict[str, Any]) -> bool:
        key = (record["station_id"], record["observed_at"])
        if key in self._seen:
            return False
        self._seen.add(key)
        self._order.append(key)
        if len(self._order) > self._capacity:
            self._seen.discard(self._order.popleft())
        return True


def parse_observation(
    station_id: str, state: str, feature: dict[str, Any]
) -> dict[str, Any] | None:
    props = feature.get("properties") or {}
    observed_at = props.get("timestamp")
    if not observed_at:
        return None

    return {
        "station_id": station_id,
        "state_alpha": state,
        "observed_at": _to_utc_iso(observed_at),
        "temperature_c": _measurement(props.get("temperature")),
        "dewpoint_c": _measurement(props.get("dewpoint")),
        "relative_humidity_pct": _measurement(props.get("relativeHumidity")),
        "wind_speed_kmh": _measurement(props.get("windSpeed")),
        "precipitation_last_hour_mm": _measurement(props.get("precipitationLastHour")),
        "description": props.get("textDescription") or None,
    }


def _measurement(raw: dict[str, Any] | None) -> float | None:
    if not raw or raw.get("value") is None:
        return None
    factor = _UNIT_FACTORS.get(raw.get("unitCode", ""))
    if factor is None:
        return None  # an unexpected unit is worse than a missing value
    return round(float(raw["value"]) * factor, 3)


def _to_utc_iso(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()
