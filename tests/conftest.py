import sys
from pathlib import Path

import pytest

from grainflow.contracts import load_contracts

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "spark" / "jobs"))


@pytest.fixture(scope="session")
def contracts():
    return load_contracts()


@pytest.fixture
def price_bar():
    return {
        "symbol": "ZC=F",
        "bar_interval": "1m",
        "bar_start": "2026-07-14T14:31:00+00:00",
        "open": 412.25,
        "high": 412.75,
        "low": 412.0,
        "close": 412.5,
        "volume": 1830,
        "source": "yahoo",
    }


@pytest.fixture
def weather_observation():
    return {
        "station_id": "KDSM",
        "state_alpha": "IA",
        "observed_at": "2026-07-14T14:54:00+00:00",
        "temperature_c": 29.4,
        "dewpoint_c": 21.1,
        "relative_humidity_pct": 61.2,
        "wind_speed_kmh": 14.8,
        "precipitation_last_hour_mm": None,
        "description": "Partly Cloudy",
    }
