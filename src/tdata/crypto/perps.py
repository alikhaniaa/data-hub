"""Perpetual futures history from Bybit (reachable from most regions): candles, funding, open interest, long/short ratio."""
from __future__ import annotations

import concurrent.futures as cf
import time

import pandas as pd

from ..collector import Collector, Ctx, register
from ..net import HttpError
from ..store import last_value, p, read, ts_from_ms, upsert

BY = "https://api.bybit.com"
IV = {"1d": ("D", 86_400_000), "1h": ("60", 3_600_000), "15m": ("15", 900_000), "5m": ("5", 300_000), "4h": ("240", 14_400_000)}


def by_get(ctx: Ctx, path: str, **params) -> dict:
    j = ctx.http.json(BY + path, params=params)
    if j.get("retCode") != 0:
        raise RuntimeError(f"bybit {path}: {j.get('retMsg')}")
    return j["result"]


def _ms(ts) -> int:
    return int(ts.timestamp() * 1000)


def klines(ctx: Ctx, symbol: str, interval: str, start_ms: int | None) -> pd.DataFrame:
    code, step = IV[interval]
    now = int(time.time() * 1000)
    end = now
    rows: list = []
    for _ in range(400):
        params = dict(category="linear", symbol=symbol, interval=code, limit=1000, end=end)
        if start_ms:
            params["start"] = start_ms
        lst = by_get(ctx, "/v5/market/kline", **params)["list"]
        if not lst:
            break
        rows += lst
        oldest = int(lst[-1][0])
        if len(lst) < 1000 or (start_ms and oldest <= start_ms):
            break
        end = oldest - 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close", "volume", "turnover"])
    df["t"] = df["t"].astype("int64")
    df = df[df["t"] + step <= now].drop_duplicates("t").sort_values("t")
    out = pd.DataFrame({"time": ts_from_ms(df["t"].values)})
    for c in ("open", "high", "low", "close", "volume", "turnover"):
        out[c] = pd.to_numeric(df[c].values)
    return out


def paged_back(ctx: Ctx, path: str, tkey: str, last_ms: int | None, limit: int, max_pages: int = 300, **params) -> list[dict]:
    """Walk backwards with endTime until we reach what we already have (or the start of history)."""
    out: list[dict] = []
    end = None
    for _ in range(max_pages):
        q = dict(params, limit=limit)
        q["endTime"] = end or int(time.time() * 1000)  # Bybit wants endTime whenever startTime is given
        if last_ms:
            q["startTime"] = last_ms
        lst = by_get(ctx, path, **q)["list"]
        if not lst:
            break
        out += lst
        oldest = int(lst[-1][tkey])
        if len(lst) < limit or (last_ms and oldest <= last_ms):
            break
        end = oldest - 1
    return out


def sync_symbol(ctx: Ctx, sym: str, intervals: list[str], with_oi: bool) -> int:
    added_total = 0
    for iv in intervals:
        path = p("crypto", "perps", "bybit", iv, f"{sym}.parquet")
        last = None if ctx.full else last_value(path, "time")
        _, step = IV[iv]
        start = _ms(last) + step if last is not None else None
        if last is not None and time.time() * 1000 < _ms(last) + 2 * step:
            continue
        df = klines(ctx, sym, iv, start)
        if len(df):
            added_total += upsert(path, df, ["time"], "time")[0]

    # funding
    path = p("crypto", "perps", "bybit", "funding", f"{sym}.parquet")
    last = None if ctx.full else last_value(path, "time")
    rows = paged_back(ctx, "/v5/market/funding/history", "fundingRateTimestamp", _ms(last) + 1 if last is not None else None, 200,
                      category="linear", symbol=sym)
    if rows:
        df = pd.DataFrame(rows)
        out = pd.DataFrame({"time": ts_from_ms(df["fundingRateTimestamp"].astype("int64").values), "funding_rate": pd.to_numeric(df["fundingRate"]).values})
        out = out[out["time"] >= pd.Timestamp("2019-01-01", tz="UTC")]  # Bybit occasionally returns an epoch-0 row
        added_total += upsert(path, out, ["time"], "time")[0]

    if with_oi:
        path = p("crypto", "perps", "bybit", "open_interest_1h", f"{sym}.parquet")
        last = None if ctx.full else last_value(path, "time")
        rows = paged_back(ctx, "/v5/market/open-interest", "timestamp", _ms(last) + 1 if last is not None else None, 200,
                          category="linear", symbol=sym, intervalTime="1h")
        if rows:
            df = pd.DataFrame(rows)
            out = pd.DataFrame({"time": ts_from_ms(df["timestamp"].astype("int64").values), "open_interest": pd.to_numeric(df["openInterest"]).values})
            added_total += upsert(path, out, ["time"], "time")[0]
        path = p("crypto", "perps", "bybit", "long_short_ratio_1h", f"{sym}.parquet")
        last = None if ctx.full else last_value(path, "time")
        try:
            rows = paged_back(ctx, "/v5/market/account-ratio", "timestamp", _ms(last) + 1 if last is not None else None, 500,
                              category="linear", symbol=sym, period="1h")
        except RuntimeError:
            rows = []
        if rows:
            df = pd.DataFrame(rows)
            out = pd.DataFrame({"time": ts_from_ms(df["timestamp"].astype("int64").values), "buy_ratio": pd.to_numeric(df["buyRatio"]).values,
                                "sell_ratio": pd.to_numeric(df["sellRatio"]).values})
            added_total += upsert(path, out, ["time"], "time")[0]
    return added_total


@register
class BybitPerps(Collector):
    name = "crypto_perps"
    group = "crypto"
    title = "Perpetual futures history (Bybit)"
    description = ("USDT perpetual candles (1d, 1h), funding-rate history for ~400 contracts, plus 1h open interest and "
                   "long/short account ratio for the top ~150. Derivatives sentiment/positioning features for RL.")
    first_run = "10-25 min"
    needs = ["crypto_universe"]

    def run(self, ctx: Ctx) -> dict:
        cfg = ctx.cfg["crypto"]
        ctx.http.throttle("bybit", 0.06)
        ins = read(p("crypto", "meta", "bybit_linear_symbols.parquet"))
        if ins is None:
            raise RuntimeError("run crypto_universe first")
        ins = ins[(ins["contract_type"] == "LinearPerpetual") & (ins["quote"] == "USDT") & (ins["status"] == "Trading")]
        ins = ins.sort_values("turnover_24h", ascending=False)
        syms = list(ins["symbol"].head(int(cfg["perp_top_n"])))
        oi_set = set(ins.sort_values("oi_value", ascending=False)["symbol"].head(int(cfg["perp_oi_top_n"])))
        total = len(syms)
        done = rows = errs = 0
        errors = []
        with cf.ThreadPoolExecutor(max_workers=max(4, int(ctx.cfg["workers"]))) as ex:
            futs = {ex.submit(sync_symbol, ctx, s, cfg["perp_intervals"], s in oi_set): s for s in syms}
            for f in cf.as_completed(futs):
                try:
                    rows += f.result()
                except InterruptedError:
                    ex.shutdown(cancel_futures=True)
                    raise
                except Exception as e:
                    errs += 1
                    errors.append(f"{futs[f]}: {type(e).__name__}: {str(e)[:80]}")
                done += 1
                if done % 10 == 0 or done == total:
                    ctx.progress(done, total, f"{done}/{total} perps, +{rows} rows")
        for e in errors[:10]:
            ctx.info("error " + e)
        return {"perps": total, "rows_added": rows, "errors": errs}
