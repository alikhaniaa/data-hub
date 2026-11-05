"""Yahoo Finance chart API (unofficial but stable): daily / hourly OHLCV for stocks, ETFs, futures, FX, indices."""
from __future__ import annotations

import time

import pandas as pd

from .net import Http, HttpError
from .store import TS

Q1 = "https://query1.finance.yahoo.com/v8/finance/chart/"
Q2 = "https://query2.finance.yahoo.com/v8/finance/chart/"
COLS = ["time", "date", "open", "high", "low", "close", "adj_close", "volume"]


def fetch(http: Http, symbol: str, interval: str = "1d", rng: str | None = None, period1: int | None = None,
          period2: int | None = None) -> tuple[pd.DataFrame | None, dict]:
    """Returns (frame, meta). frame is None when Yahoo has no data for the symbol."""
    params = {"interval": interval, "includePrePost": "false", "events": "div,split"}
    if period1 is not None:
        params["period1"] = int(period1)
        params["period2"] = int(period2 or time.time() + 86400)
    else:
        params["range"] = rng or "max"
    sym = symbol.replace("^", "%5E")
    last = None
    for base in (Q1, Q2):
        try:
            j = http.json(f"{base}{sym}", params=params)
            break
        except HttpError as e:
            if e.status in (404, 400):
                return None, {}
            last = e
    else:
        raise last  # type: ignore[misc]
    res = (j.get("chart") or {}).get("result")
    if not res:
        return None, {}
    r = res[0]
    meta = r.get("meta", {})
    ts = r.get("timestamp") or []
    if not ts:
        return None, meta
    q = r["indicators"]["quote"][0]
    adj = (r["indicators"].get("adjclose") or [{}])[0].get("adjclose")
    off = int(meta.get("gmtoffset") or 0)
    t = pd.to_datetime(pd.Series(ts, dtype="int64"), unit="s", utc=True).astype(TS)
    df = pd.DataFrame({
        "time": t,
        "date": (pd.to_datetime(pd.Series(ts, dtype="int64") + off, unit="s")).dt.floor("D").astype("datetime64[ms]"),
        "open": q.get("open"), "high": q.get("high"), "low": q.get("low"), "close": q.get("close"),
        "adj_close": adj if adj is not None else q.get("close"), "volume": q.get("volume"),
    })
    for c in ("open", "high", "low", "close", "adj_close"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    df = df.dropna(subset=["close"]).drop_duplicates("time", keep="last").reset_index(drop=True)
    return df, meta
