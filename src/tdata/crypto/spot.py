"""Spot OHLCV for every Binance USDT pair (incl. delisted ones), incremental, multi-interval."""
from __future__ import annotations

import concurrent.futures as cf
import json
import re
import time
from datetime import datetime, timezone

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import last_value, p, read, upsert, write
from . import binance as bn

LEVERAGED = re.compile(r"^(.+?)(UP|DOWN|BULL|BEAR)$")


def _ms(ts) -> int:
    return int(ts.timestamp() * 1000)


def _month_of(key: str) -> str:
    m = re.search(r"-(\d{4}-\d{2})\.zip$", key)
    return m.group(1) if m else ""


def delisted_symbols(ctx: Ctx, known_symbols: set[str], quote: str) -> list[str]:
    cache = p("crypto", "meta", "s3_spot_symbols.json")
    syms: list[str] | None = None
    if cache.exists() and time.time() - cache.stat().st_mtime < 24 * 3600 and not ctx.full:
        try:
            syms = json.loads(cache.read_text())
        except Exception:
            syms = None
    if syms is None:
        prefixes, _ = bn.s3_list(ctx.http, "data/spot/monthly/klines/", delimiter="/")
        syms = sorted(x.rstrip("/").split("/")[-1] for x in prefixes)
        cache.write_text(json.dumps(syms))
    stems = {s[: -len(quote)] for s in syms if s.endswith(quote)} | {s[: -len(quote)] for s in known_symbols if s.endswith(quote)}
    out = []
    for s in syms:
        if not s.endswith(quote) or s in known_symbols:
            continue
        stem = s[: -len(quote)]
        m = LEVERAGED.match(stem)
        if m and m.group(1) in stems:
            continue  # BTCUPUSDT etc.
        out.append(s)
    return out


def _bulk_frames(ctx: Ctx, symbol: str, interval: str, since_ms: int | None) -> list[pd.DataFrame]:
    """Complete past months from the bulk archive (cheap for high-resolution intervals)."""
    _, keys = bn.s3_list(ctx.http, f"data/spot/monthly/klines/{symbol}/{interval}/")
    months = sorted({_month_of(k) for k in keys if k.endswith(".zip")} - {""})
    cur_month = datetime.now(timezone.utc).strftime("%Y-%m")
    first_month = datetime.fromtimestamp(since_ms / 1000, timezone.utc).strftime("%Y-%m") if since_ms else "0000-00"
    frames = []
    for ym in months:
        if ym >= cur_month or ym < first_month:
            continue
        df = bn.bulk_klines(ctx.http, bn.month_key("spot", symbol, interval, ym))
        if df is not None and len(df):
            frames.append(df)
    return frames


def sync_pair(ctx: Ctx, symbol: str, interval: str, since_ms: int | None, delisted: bool) -> int:
    step = bn.INTERVAL_MS[interval]
    path = p("crypto", "spot", interval, f"{symbol}.parquet")
    last = None if ctx.full else last_value(path, "time")
    now_ms = int(time.time() * 1000)
    frames: list[pd.DataFrame] = []
    start_ms = since_ms or 0
    if last is not None:
        last_ms = _ms(last)
        if delisted or now_ms < last_ms + 2 * step:
            return 0  # delisted history is final / nothing new could have closed yet
        start_ms = last_ms + step

    if last is None and interval in bn.BULK_FIRST:
        frames = _bulk_frames(ctx, symbol, interval, since_ms)
        if frames:
            start_ms = max(start_ms, _ms(max(f["time"].max() for f in frames)) + step)

    df = bn.rest_klines(ctx.http, symbol, interval, start_ms, stop=ctx.stop)
    if df is not None and len(df):
        frames.append(df)
    elif delisted and not frames and last is None:  # REST had nothing -> try the archive
        frames = _bulk_frames(ctx, symbol, interval, since_ms)

    if not frames:
        return 0
    new = pd.concat(frames, ignore_index=True)
    if since_ms:
        new = new[new["time"] >= pd.Timestamp(since_ms, unit="ms", tz="UTC")]
    added, _ = upsert(path, new, keys=["time"], sort_by="time")
    return added


@register
class SpotOHLCV(Collector):
    name = "crypto_spot"
    group = "crypto"
    title = "Spot OHLCV (all Binance USDT pairs)"
    description = ("Candles with volume, quote volume, trade count and taker-buy volume for every USDT pair (incl. delisted): "
                   "1d + 1h for all; 15m/5m/1m for the most liquid. Incremental after the first run.")
    first_run = "20-45 min"
    needs = ["crypto_universe"]

    def run(self, ctx: Ctx) -> dict:
        cfg = ctx.cfg["crypto"]
        quote = cfg["spot_quote"]
        sm = read(p("crypto", "meta", "binance_spot_symbols.parquet"))
        if sm is None:
            raise RuntimeError("run crypto_universe first")
        act = sm[(sm["quote"] == quote) & (sm["status"].isin(["TRADING", "BREAK"])) & (sm["spot_allowed"] == True)].copy()  # noqa: E712
        minv = float(cfg.get("min_quote_volume_usd", 0) or 0)
        if minv:
            act = act[act["quote_volume_24h"].fillna(0) >= minv]
        act = act.sort_values("quote_volume_24h", ascending=False)
        ranked = list(act["symbol"])
        status = dict(zip(act["symbol"], act["status"]))
        all_iv = cfg["spot_intervals_all"]
        tiers = cfg["spot_intervals_top"]

        jobs: list[tuple[str, list[tuple[str, int | None]], bool]] = []
        for i, s in enumerate(ranked):
            plan = [(iv, None) for iv in all_iv]
            for iv, t in tiers.items():
                if i < int(t["top_n"]):
                    since = int(datetime.fromisoformat(t["since"]).replace(tzinfo=timezone.utc).timestamp() * 1000)
                    plan.append((iv, since))
            # Binance keeps delisted pairs in exchangeInfo with status BREAK: their history is final
            jobs.append((s, plan, status[s] != "TRADING"))
        if cfg.get("include_delisted", True):
            try:
                dl = delisted_symbols(ctx, set(sm["symbol"]), quote)
                ctx.info(f"{len(ranked)} active + {len(dl)} delisted/historic {quote} pairs")
                for s in dl:
                    jobs.append((s, [(iv, None) for iv in all_iv], True))
            except Exception as e:
                ctx.info(f"delisted list unavailable: {e}")
        total = len(jobs)
        done = rows = errs = 0
        errors: list[str] = []

        def work(job):
            sym, plan, dele = job
            n = 0
            for iv, since in plan:
                n += sync_pair(ctx, sym, iv, since, dele)
            return n

        workers = max(4, int(ctx.cfg["workers"]) * 2)
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(work, j): j for j in jobs}
            for f in cf.as_completed(futs):
                sym = futs[f][0]
                try:
                    rows += f.result()
                except InterruptedError:
                    ex.shutdown(cancel_futures=True)
                    raise
                except Exception as e:
                    errs += 1
                    errors.append(f"{sym}: {type(e).__name__}: {str(e)[:80]}")
                done += 1
                if done % 25 == 0 or done == total:
                    ctx.progress(done, total, f"{done}/{total} pairs, +{rows} rows")
        for e in errors[:10]:
            ctx.info("error " + e)
        return {"pairs": total, "rows_added": rows, "errors": errs}
