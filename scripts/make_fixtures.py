"""Write small synthetic bronze tables for the dbt CI target (DuckDB).

The numbers are made up: random walks and seasonal curves with a fixed seed.
They exist so `dbt build --target ci` exercises every model and test without
Kafka, Spark or API keys. Never point the real stack at them.

    uv run python scripts/make_fixtures.py
"""

from __future__ import annotations

import csv
import json
import math
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "transform" / "seeds" / "fixtures"
SEED = 7
UTC = timezone.utc
LOADED = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)

SYMBOLS = {"ZC=F": 430.0, "ZS=F": 1040.0, "ZW=F": 560.0}
STATIONS = {"KDSM": "IA", "KPIA": "IL", "KIND": "IN", "KOMA": "NE"}
CLASSES = ["VERY POOR", "POOR", "FAIR", "GOOD", "EXCELLENT"]


def ts(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S+00")


def weekdays(start: date, end: date):
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


def price_bars(rng: random.Random) -> list[dict]:
    rows = []
    offset = 0
    for symbol, price in SYMBOLS.items():
        for day in weekdays(date(2026, 3, 2), date(2026, 9, 11)):
            open_ = price
            close = max(1.0, price * math.exp(rng.gauss(0, 0.015)))
            high = max(open_, close) * (1 + abs(rng.gauss(0, 0.004)))
            low = min(open_, close) * (1 - abs(rng.gauss(0, 0.004)))
            start = datetime(day.year, day.month, day.day, 5, tzinfo=UTC)
            rows.append(
                _bar(
                    symbol,
                    "1d",
                    start,
                    open_,
                    high,
                    low,
                    close,
                    rng.randint(40_000, 180_000),
                    offset,
                )
            )
            offset += 1
            price = close

    # One trading hour of corn minute bars, with a replayed duplicate.
    price = 431.0
    for minute in range(60):
        start = datetime(2026, 9, 11, 14, 30, tzinfo=UTC) + timedelta(minutes=minute)
        close = price + rng.gauss(0, 0.4)
        rows.append(
            _bar(
                "ZC=F",
                "1m",
                start,
                price,
                max(price, close) + 0.25,
                min(price, close) - 0.25,
                close,
                rng.randint(50, 900),
                offset,
            )
        )
        offset += 1
        price = close
    duplicate = dict(rows[-1], _kafka_offset=offset, _ingested_at=ts(LOADED + timedelta(minutes=5)))
    rows.append(duplicate)
    return rows


def _bar(symbol, interval, start, open_, high, low, close, volume, offset) -> dict:
    return {
        "symbol": symbol,
        "bar_interval": interval,
        "bar_start": ts(start),
        "open": round(open_, 2),
        "high": round(high, 2),
        "low": round(low, 2),
        "close": round(close, 2),
        "volume": volume,
        "source": "fixture",
        "_kafka_partition": offset % 3,
        "_kafka_offset": offset,
        "_kafka_timestamp": ts(start),
        "_ingested_at": ts(LOADED),
    }


def weather_observations(rng: random.Random) -> list[dict]:
    rows = []
    start = datetime(2026, 6, 1, tzinfo=UTC)
    for station, state in STATIONS.items():
        bias = rng.uniform(-1.5, 1.5)
        for hour in range(92 * 24):
            observed = start + timedelta(hours=hour, minutes=54)
            day_of_season = hour / 24
            seasonal = 24 + 4 * math.sin(math.pi * day_of_season / 92)
            diurnal = 6 * math.sin(2 * math.pi * ((observed.hour - 11) % 24) / 24)
            temp = seasonal + diurnal + bias + rng.gauss(0, 1.2)
            raining = rng.random() < 0.04
            rows.append(
                {
                    "station_id": station,
                    "state_alpha": state,
                    "observed_at": ts(observed),
                    "temperature_c": round(temp, 1),
                    "dewpoint_c": round(temp - rng.uniform(3, 9), 1),
                    "relative_humidity_pct": round(rng.uniform(40, 95), 1),
                    "wind_speed_kmh": round(abs(rng.gauss(14, 6)), 1),
                    "precipitation_last_hour_mm": round(rng.uniform(0.2, 6), 1) if raining else "",
                    "description": "Rain" if raining else "Partly Cloudy",
                    "_kafka_partition": 0,
                    "_kafka_offset": hour,
                    "_kafka_timestamp": ts(observed),
                    "_ingested_at": ts(LOADED),
                }
            )
    return rows


def crop_condition(rng: random.Random) -> list[dict]:
    rows = []
    for state in STATIONS.values():
        good_excellent = rng.uniform(62, 74)
        week = date(2026, 6, 7)
        while week <= date(2026, 8, 30):
            good_excellent = min(85.0, max(40.0, good_excellent + rng.gauss(-0.6, 2.5)))
            excellent = round(good_excellent * rng.uniform(0.22, 0.32))
            good = round(good_excellent) - excellent
            fair = round((100 - good - excellent) * 0.65)
            poor = round((100 - good - excellent - fair) * 0.7)
            very_poor = 100 - good - excellent - fair - poor
            for condition, pct in zip(
                CLASSES, [very_poor, poor, fair, good, excellent], strict=True
            ):
                rows.append(
                    {
                        "commodity": "CORN",
                        "state_alpha": state,
                        "week_ending": week.isoformat(),
                        "condition_class": condition,
                        "pct": float(pct),
                        "nass_load_time": ts(
                            datetime(week.year, week.month, week.day, 20, tzinfo=UTC)
                            + timedelta(days=1)
                        ),
                        "_kafka_partition": "",
                        "_kafka_offset": "",
                        "_kafka_timestamp": "",
                        "_ingested_at": ts(LOADED),
                    }
                )
            week += timedelta(days=7)
    return rows


def quarantine() -> list[dict]:
    bad_bar = {"symbol": "ZC=F", "bar_interval": "1m", "close": -1}
    return [
        {
            "contract": "price_bar",
            "topic": "market.price_bars.v1",
            "kafka_partition": 1,
            "kafka_offset": 9001,
            "raw_value": json.dumps(bad_bar),
            "reason": "bar_start is required; close below 0",
            "_ingested_at": ts(LOADED),
        },
        {
            "contract": "crop_condition",
            "topic": "",
            "kafka_partition": "",
            "kafka_offset": "",
            "raw_value": json.dumps({"state_alpha": "IL", "pct": 1250.0}),
            "reason": "pct: 1250.0 is greater than the maximum of 100",
            "_ingested_at": ts(LOADED),
        },
    ]


def write(name: str, rows: list[dict]) -> None:
    path = OUT / f"{name}.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{path.relative_to(OUT.parents[2])}: {len(rows)} rows")


def main() -> None:
    rng = random.Random(SEED)
    OUT.mkdir(parents=True, exist_ok=True)
    write("price_bars", price_bars(rng))
    write("weather_observations", weather_observations(rng))
    write("crop_condition", crop_condition(rng))
    write("quarantine", quarantine())


if __name__ == "__main__":
    main()
