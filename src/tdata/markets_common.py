"""Incremental Yahoo Finance sync for an instrument basket (daily + hourly bars, instrument metadata)."""
from __future__ import annotations

import concurrent.futures as cf
import re
import time

import pandas as pd

from . import yahoo
from .net import HttpError
from .collector import Ctx
from .store import last_value, p, upsert, write, now_utc


def safe(sym: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", sym)


def _range_for_age(age_days: float, intraday: bool) -> str:
    if intraday:
        return "5d" if age_days <= 3 else "1mo" if age_days <= 25 else "730d"
    return "1mo" if age_days <= 25 else "6mo" if age_days <= 170 else "2y" if age_days <= 700 else "max"


def sync_symbol(ctx: Ctx, sym: str, hourly: bool = True) -> dict:
    out = {"symbol": sym, "daily": 0, "hourly": 0, "meta": {}}
    for interval, folder in (("1d", "daily"), ("1h", "hourly")):
        if interval == "1h" and not hourly:
            continue
        path = p("markets", folder, f"{safe(sym)}.parquet")
        last = None if ctx.full else last_value(path, "time")
        if last is None:
            rng = "730d" if interval == "1h" else "max"
        else:
            age = (now_utc() - last.to_pydatetime()).total_seconds() / 86400
            rng = _range_for_age(age, interval == "1h")
        if interval == "1d" and rng == "max":  # range=max silently degrades to monthly bars; period1=0 keeps them daily
            df, meta = yahoo.fetch(ctx.http, sym, "1d", period1=0)
        else:
            df, meta = None, {}
            for r_try in dict.fromkeys([rng, "700d", "365d", "180d", "60d", "30d"] if rng == "730d" else [rng]):
                try:
                    df, meta = yahoo.fetch(ctx.http, sym, interval, rng=r_try)
                    break
                except HttpError as e:
                    if e.status != 422:  # 422 = range starts before the instrument's history; retry shorter
                        raise
        if meta:
            out["meta"] = meta
        if df is None or df.empty:
            continue
        # Daily bars are keyed on the exchange-local date: the still-forming bar of the current session can carry a
        # different timestamp from the final bar of the same day, and the newer row must replace it.
        added, _ = upsert(path, df, keys=["date"] if interval == "1d" else ["time"], sort_by="time")
        out[folder] = added
    return out


def sync_basket(ctx: Ctx, symbols: list[str], rows: list[dict], hourly: bool = True, tag: str = "") -> dict:
    ctx.http.throttle("yahoo", 0.12)
    ok = fail = 0
    added_d = added_h = 0
    metas = {}
    missing: list[str] = []
    total = len(symbols)
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(sync_symbol, ctx, s, hourly): s for s in symbols}
        for i, f in enumerate(cf.as_completed(futs), 1):
            s = futs[f]
            try:
                r = f.result()
                added_d += r["daily"]
                added_h += r["hourly"]
                if r["meta"]:
                    metas[s] = r["meta"]
                    ok += 1
                else:
                    missing.append(s)
            except InterruptedError:
                ex.shutdown(cancel_futures=True)
                raise
            except Exception as e:
                fail += 1
                missing.append(f"{s}({type(e).__name__})")
            ctx.progress(i, total, f"{i}/{total} symbols")
    # instrument metadata table
    recs = []
    for r in rows:
        m = metas.get(r["symbol"], {})
        recs.append({**r, "yahoo_name": m.get("longName") or m.get("shortName"), "currency": m.get("currency"),
                     "exchange": m.get("fullExchangeName"), "type": m.get("instrumentType"),
                     "first_trade": pd.to_datetime(m["firstTradeDate"], unit="s", utc=True).as_unit("ms") if m.get("firstTradeDate") else pd.NaT,
                     "available": r["symbol"] in metas})
    meta_df = pd.DataFrame(recs)
    write(meta_df, p("markets", "meta", f"instruments_{tag}.parquet"))
    if missing:
        ctx.info(f"no data for: {', '.join(missing[:40])}")
    return {"symbols": total, "available": ok, "daily_rows_added": added_d, "hourly_rows_added": added_h, "failed": fail}
