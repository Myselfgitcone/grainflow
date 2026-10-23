"""CBOT grain futures bars from Yahoo Finance.

Yahoo quotes are delayed (~10-15 min) and the 1m history only goes back about
a week, which is fine for a lakehouse demo. Two ways to turn that into a
stream:

* `LivePoller` asks for the latest 1m bars on an interval and emits only the
  ones it has not seen. Works while CBOT is open.
* `replay` plays stored bars back with their original spacing, sped up. Use it
  on weekends or for load testing.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from typing import Any

import pandas as pd

GRAIN_SYMBOLS = {
    "ZC=F": "CORN",
    "ZS=F": "SOYBEANS",
    "ZW=F": "WHEAT",
    "KE=F": "WHEAT",  # KC hard red winter
}

Bar = dict[str, Any]


def fetch_bars(symbol: str, interval: str = "1d", period: str = "2y") -> list[Bar]:
    import yfinance as yf

    frame = yf.Ticker(symbol).history(interval=interval, period=period, auto_adjust=False)
    return bars_from_frame(frame, symbol, interval)


def bars_from_frame(
    frame: pd.DataFrame, symbol: str, interval: str, source: str = "yahoo"
) -> list[Bar]:
    """Turn a yfinance history frame into contract-shaped records."""
    if frame.empty:
        return []

    frame = frame.dropna(subset=["Open", "High", "Low", "Close"])
    index = frame.index
    index = index.tz_localize("UTC") if index.tz is None else index.tz_convert("UTC")

    return [
        {
            "symbol": symbol,
            "bar_interval": interval,
            "bar_start": ts.isoformat(),
            "open": float(row.Open),
            "high": float(row.High),
            "low": float(row.Low),
            "close": float(row.Close),
            "volume": int(row.Volume) if pd.notna(row.Volume) else 0,
            "source": source,
        }
        for ts, row in zip(index, frame.itertuples(index=False), strict=True)
    ]


class LivePoller:
    """Remembers the newest bar per symbol so each poll only yields new ones."""

    def __init__(self, symbols: Iterable[str], fetch: Callable[[str], list[Bar]] | None = None):
        self.symbols = list(symbols)
        self._fetch = fetch or (lambda s: fetch_bars(s, interval="1m", period="1d"))
        self._latest: dict[str, str] = {}

    def poll(self) -> list[Bar]:
        fresh = []
        for symbol in self.symbols:
            bars = self._fetch(symbol)
            # The last bar is still forming; wait for the next poll to send it.
            closed = bars[:-1]
            newest_seen = self._latest.get(symbol, "")
            new = [b for b in closed if b["bar_start"] > newest_seen]
            if new:
                self._latest[symbol] = new[-1]["bar_start"]
                fresh.extend(new)
        return fresh


def replay(
    bars: Iterable[Bar],
    speed: float = 60.0,
    max_gap_seconds: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[Bar]:
    """Yield bars in time order, sleeping (gap / speed) between them.

    Gaps are capped so overnight and weekend breaks don't stall the stream.
    """
    ordered = sorted(bars, key=lambda b: b["bar_start"])
    previous = None
    for bar in ordered:
        current = pd.Timestamp(bar["bar_start"])
        if previous is not None and speed > 0:
            gap = (current - previous).total_seconds() / speed
            sleep(min(max(gap, 0.0), max_gap_seconds))
        previous = current
        yield {**bar, "source": "replay"}
