"""Hyperliquid perpetuals: candles + funding history for the main DEX and the HIP-3 commodity/stock/FX markets (xyz:...)."""
from __future__ import annotations

import concurrent.futures as cf
import time

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import last_value, p, read, ts_from_ms, upsert

HL = "https://api.hyperliquid.xyz/info"
STEP = {"1h": 3_600_000, "1d": 86_400_000}


def fname(coin: str) -> str:
    return coin.replace(":", "__").replace("/", "_")


def _ms(ts) -> int:
    return int(ts.timestamp() * 1000)


def candles(ctx: Ctx, coin: str, interval: str, start_ms: int) -> pd.DataFrame:
    step = STEP[interval]
    now = int(time.time() * 1000)
    rows: list[dict] = []
    cur = start_ms
    for _ in range(200):
        lst = ctx.http.post_json(HL, {"type": "candleSnapshot", "req": {"coin": coin, "interval": interval, "startTime": cur, "endTime": now}})
        if not lst:
            break
        rows += lst
        last_t = int(lst[-1]["t"])
        if len(lst) < 5000 or last_t + step >= now:
            break
        cur = last_t + step
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates("t").sort_values("t")
    df = df[df["t"].astype("int64") + step <= now]
    out = pd.DataFrame({"time": ts_from_ms(df["t"].astype("int64").values)})
    for src, dst in (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"), ("v", "volume")):
        out[dst] = pd.to_numeric(df[src].values)
    out["trades"] = df["n"].astype("int64").values
    return out


def funding(ctx: Ctx, coin: str, start_ms: int) -> pd.DataFrame:
    rows: list[dict] = []
    cur = start_ms
    for _ in range(400):
        lst = ctx.http.post_json(HL, {"type": "fundingHistory", "coin": coin, "startTime": cur})
        if not lst:
            break
        rows += lst
        if len(lst) < 500:
            break
        cur = int(lst[-1]["time"]) + 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).drop_duplicates("time").sort_values("time")
    return pd.DataFrame({"time": ts_from_ms(df["time"].astype("int64").values), "funding_rate": pd.to_numeric(df["fundingRate"].values),
                         "premium": pd.to_numeric(df["premium"].values)})


def sync_coin(ctx: Ctx, coin: str) -> int:
    n = 0
    for iv, step in STEP.items():
        path = p("crypto", "hyperliquid", iv, f"{fname(coin)}.parquet")
        last = None if ctx.full else last_value(path, "time")
        if last is not None and time.time() * 1000 < _ms(last) + 2 * step:
            continue
        df = candles(ctx, coin, iv, _ms(last) + step if last is not None else 0)
        if len(df):
            n += upsert(path, df, ["time"], "time")[0]
    path = p("crypto", "hyperliquid", "funding", f"{fname(coin)}.parquet")
    last = None if ctx.full else last_value(path, "time")
    if last is None or time.time() * 1000 > _ms(last) + 2 * 3_600_000:
        df = funding(ctx, coin, _ms(last) + 1 if last is not None else 0)
        if len(df):
            n += upsert(path, df, ["time"], "time")[0]
    return n


@register
class HyperliquidHistory(Collector):
    name = "crypto_hyperliquid"
    group = "crypto"
    title = "Hyperliquid perps (+ oil/gold/stock perps)"
    description = ("1h/1d candles and hourly funding for the most active Hyperliquid perps, plus the 24/7 commodity & index markets "
                   "(Brent, WTI, nat-gas, TTF, gold, silver, copper, XLE, DXY, VIX...). These trade on weekends.")
    first_run = "5-10 min"
    needs = ["crypto_universe"]

    def run(self, ctx: Ctx) -> dict:
        cfg = ctx.cfg["crypto"]
        ctx.http.throttle("hyperliquid", 0.12)
        meta = read(p("crypto", "meta", "hyperliquid_perps.parquet"))
        if meta is None:
            raise RuntimeError("run crypto_universe first")
        main = meta[(meta["dex"] == "main") & (~meta["delisted"])]["name"].tolist()
        # rank main-dex coins by 24h notional volume
        ctxs = ctx.http.post_json(HL, {"type": "metaAndAssetCtxs"})
        vol = {u["name"]: float(c.get("dayNtlVlm") or 0) for u, c in zip(ctxs[0]["universe"], ctxs[1])}
        main = sorted(main, key=lambda c: -vol.get(c, 0))[: int(cfg["hl_top_n"])]
        xyz = [f"xyz:{c}" for c in cfg["hl_xyz_commodities"]]
        coins = list(dict.fromkeys(xyz + main))
        total = len(coins)
        done = rows = errs = 0
        errors = []
        with cf.ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(sync_coin, ctx, c): c for c in coins}
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
                    ctx.progress(done, total, f"{done}/{total} coins, +{rows} rows")
        for e in errors[:10]:
            ctx.info("error " + e)
        return {"coins": total, "rows_added": rows, "errors": errs}
