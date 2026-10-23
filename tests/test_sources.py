import json
from pathlib import Path

import httpx
import pandas as pd

from grainflow.sources import crops, prices, weather

FIXTURES = Path(__file__).parent / "fixtures"


def yahoo_frame(start: str, periods: int, freq: str = "1min") -> pd.DataFrame:
    index = pd.date_range(start, periods=periods, freq=freq, tz="America/Chicago")
    return pd.DataFrame(
        {
            "Open": [410.0 + i for i in range(periods)],
            "High": [411.0 + i for i in range(periods)],
            "Low": [409.0 + i for i in range(periods)],
            "Close": [410.5 + i for i in range(periods)],
            "Volume": [100 * (i + 1) for i in range(periods)],
        },
        index=index,
    )


# --- prices -------------------------------------------------------------------


def test_bars_from_frame_converts_to_utc_contract_records(contracts):
    bars = prices.bars_from_frame(yahoo_frame("2026-07-14 09:30", 2), "ZC=F", "1m")

    assert bars[0]["bar_start"] == "2026-07-14T14:30:00+00:00"
    assert bars[1]["volume"] == 200
    validator = contracts["price_bar"].validator()
    assert all(validator.errors(b) == [] for b in bars)


def test_bars_from_frame_skips_incomplete_rows():
    frame = yahoo_frame("2026-07-14 09:30", 3)
    frame.iloc[1, frame.columns.get_loc("Close")] = float("nan")
    assert len(prices.bars_from_frame(frame, "ZC=F", "1m")) == 2


def test_live_poller_only_emits_closed_unseen_bars():
    frames = iter([yahoo_frame("2026-07-14 09:30", 3), yahoo_frame("2026-07-14 09:30", 5)])
    poller = prices.LivePoller(
        ["ZC=F"], fetch=lambda s: prices.bars_from_frame(next(frames), s, "1m")
    )

    first = poller.poll()
    second = poller.poll()

    # 3 bars, the last still forming -> 2. Then bars 3 and 4 of 5.
    assert [b["bar_start"][11:16] for b in first] == ["14:30", "14:31"]
    assert [b["bar_start"][11:16] for b in second] == ["14:32", "14:33"]


def test_replay_keeps_order_and_scales_gaps():
    bars = prices.bars_from_frame(yahoo_frame("2026-07-14 09:30", 3), "ZC=F", "1m")
    sleeps = []

    replayed = list(prices.replay(reversed(bars), speed=60, sleep=sleeps.append))

    assert [b["bar_start"] for b in replayed] == [b["bar_start"] for b in bars]
    assert all(b["source"] == "replay" for b in replayed)
    assert sleeps == [1.0, 1.0]  # 60 seconds at 60x


def test_replay_caps_long_gaps():
    bars = prices.bars_from_frame(yahoo_frame("2026-07-10", 2, freq="3D"), "ZC=F", "1d")
    sleeps = []
    list(prices.replay(bars, speed=60, max_gap_seconds=5, sleep=sleeps.append))
    assert sleeps == [5]


# --- weather ------------------------------------------------------------------


def test_parse_observation_normalizes_units(contracts):
    feature = json.loads((FIXTURES / "nws_observation.json").read_text())

    record = weather.parse_observation("KDSM", "IA", feature)

    assert record["observed_at"] == "2026-07-14T14:54:00+00:00"
    assert record["temperature_c"] == 29.4
    assert record["wind_speed_kmh"] == 14.4  # reported as 4 m/s
    assert record["precipitation_last_hour_mm"] is None
    assert contracts["weather_observation"].validator().errors(record) == []


def test_unknown_units_become_missing_values():
    feature = {
        "properties": {
            "timestamp": "2026-07-14T14:54:00Z",
            "temperature": {"unitCode": "wmoUnit:degF", "value": 85},
        }
    }
    assert weather.parse_observation("KDSM", "IA", feature)["temperature_c"] is None


def test_client_reads_feature_collection():
    payload = {"features": [json.loads((FIXTURES / "nws_observation.json").read_text())]}
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    client = weather.WeatherClient(
        "test", http=httpx.Client(transport=transport, base_url=weather.BASE_URL)
    )

    [record] = client.recent_observations("KDSM")

    assert record["state_alpha"] == "IA"


def test_seen_observations_dedupes_and_forgets_oldest():
    seen = weather.SeenObservations(capacity=2)
    a, b, c = ({"station_id": "KDSM", "observed_at": t} for t in ("t1", "t2", "t3"))

    assert [seen.add(a), seen.add(a), seen.add(b), seen.add(c)] == [True, False, True, True]
    assert seen.add(a) is True  # evicted, so it counts as new again


# --- crops --------------------------------------------------------------------


def test_parse_condition_rows(contracts):
    rows = json.loads((FIXTURES / "nass_condition.json").read_text())["data"]

    records = crops.parse_condition_rows(rows, "CORN")

    assert [(r["state_alpha"], r["condition_class"], r["pct"]) for r in records] == [
        ("IA", "GOOD", 54.0),
        ("IA", "EXCELLENT", 21.0),
        ("IL", "FAIR", 1250.0),
    ]
    assert records[0]["nass_load_time"] == "2026-07-13T16:00:42+00:00"
    validator = contracts["crop_condition"].validator()
    assert validator.errors(records[0]) == []
    # 1,250 survives parsing but the contract catches it before it lands.
    assert any("pct" in e for e in validator.errors(records[2]))
