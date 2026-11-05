"""Binance public market data: REST klines (data-api.binance.vision) and the bulk archive (data.binance.vision)."""
from __future__ import annotations

import io
import re
import time
import xml.etree.ElementTree as ET
import zipfile

import numpy as np
import pandas as pd

from ..net import Http, HttpError
from ..store import ts_from_ms

SPOT_HOSTS = ["https://data-api.binance.vision", "https://api.binance.com"]
S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
BULK = "https://data.binance.vision"

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000, "8h": 28_800_000,
    "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000, "1w": 604_800_000,
}
BULK_FIRST = {"1m", "3m", "5m", "15m", "30m"}  # high-res intervals are cheaper through monthly zips
KLINE_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "trades",
              "taker_buy_base", "taker_buy_quote", "ignore"]
OUT_COLS = ["time", "open", "high", "low", "close", "volume", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]


def spot_get(http: Http, path: str, params: dict | None = None):
    last = None
    for host in SPOT_HOSTS:
        try:
            return http.json(host + path, params=params)
        except HttpError as e:
            if e.status in (400,):  # bad symbol etc. -> do not try other hosts
                raise
            last = e
        except Exception as e:  # network trouble on one host
            last = e
    raise last  # type: ignore[misc]


def canon(df: pd.DataFrame) -> pd.DataFrame:
    """Raw kline frame (12 columns) -> canonical typed frame."""
    ot = pd.to_numeric(df["open_time"], errors="coerce")
    df = df.loc[ot.notna()].copy()
    ot = ot.loc[df.index].astype("int64")
    ot = pd.Series(np.where(ot > 10**14, ot // 1000, ot), index=df.index)  # spot archives switched to microseconds in 2025
    out = pd.DataFrame({"time": ts_from_ms(ot.values)})
    for c in ("open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base", "taker_buy_quote"):
        out[c] = pd.to_numeric(df[c], errors="coerce").values
    out["trades"] = pd.to_numeric(df["trades"], errors="coerce").fillna(0).astype("int64").values
    return out[OUT_COLS]


def rest_klines(http: Http, symbol: str, interval: str, start_ms: int, stop=None) -> pd.DataFrame | None:
    """Page forward from start_ms to now. Returns None for an unknown symbol. Drops the still-open candle."""
    step = INTERVAL_MS[interval]
    rows: list = []
    cur = int(start_ms)
    while True:
        if stop is not None and stop.is_set():
            raise InterruptedError("stopped")
        try:
            data = spot_get(http, "/api/v3/klines", {"symbol": symbol, "interval": interval, "startTime": cur, "limit": 1000})
        except HttpError as e:
            if e.status == 400:
                return None
            raise
        if not data:
            break
        rows.extend(data)
        if len(data) < 1000:
            break
        cur = int(data[-1][0]) + step
    if not rows:
        return pd.DataFrame(columns=OUT_COLS)
    df = canon(pd.DataFrame(rows, columns=KLINE_COLS))
    now_ms = int(time.time() * 1000)
    open_ms = df["time"].dt.as_unit("ms").astype("int64")
    return df.loc[open_ms + step <= now_ms].reset_index(drop=True)


# ---------------------------------------------------------------- bulk archive
_NS = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}


def s3_list(http: Http, prefix: str, delimiter: str | None = None, max_pages: int = 100) -> tuple[list[str], list[str]]:
    """List an S3 prefix of the Binance archive -> (common_prefixes, keys)."""
    prefixes, keys = [], []
    marker = ""
    for _ in range(max_pages):
        params = {"prefix": prefix, "max-keys": 1000}
        if delimiter:
            params["delimiter"] = delimiter
        if marker:
            params["marker"] = marker
        r = http.get(S3, params=params)
        root = ET.fromstring(r.content)
        for cp in root.findall("s:CommonPrefixes/s:Prefix", _NS):
            prefixes.append(cp.text)
        for k in root.findall("s:Contents/s:Key", _NS):
            keys.append(k.text)
        trunc = root.findtext("s:IsTruncated", default="false", namespaces=_NS) == "true"
        if not trunc:
            break
        nm = root.findtext("s:NextMarker", namespaces=_NS)
        marker = nm or (keys[-1] if keys else (prefixes[-1] if prefixes else ""))
        if not marker:
            break
    return prefixes, keys


def bulk_csv(http: Http, key: str, names: list[str] | None = None) -> pd.DataFrame | None:
    """Download one archive zip and parse its single CSV. None if the file does not exist."""
    try:
        r = http.get(f"{BULK}/{key}", ok404=True)
    except HttpError as e:
        if e.status in (403, 404):
            return None
        raise
    if r.status_code in (403, 404):
        return None
    z = zipfile.ZipFile(io.BytesIO(r.content))
    with z.open(z.namelist()[0]) as f:
        raw = f.read().decode("utf-8", "replace")
    first = raw[:200].split("\n", 1)[0]
    has_header = bool(re.search(r"[A-Za-z]", first))
    df = pd.read_csv(io.StringIO(raw), header=0 if has_header else None, names=None if has_header else names)
    if has_header and names is not None and len(df.columns) == len(names):
        df.columns = names
    return df


def month_key(market: str, symbol: str, interval: str, ym: str, kind: str = "klines", freq: str = "monthly") -> str:
    return f"data/{market}/{freq}/{kind}/{symbol}/{interval}/{symbol}-{interval}-{ym}.zip"


def bulk_klines(http: Http, key: str) -> pd.DataFrame | None:
    df = bulk_csv(http, key, KLINE_COLS)
    if df is None or df.empty:
        return None
    if "open_time" not in df.columns:
        df.columns = KLINE_COLS[: len(df.columns)]
    return canon(df)
