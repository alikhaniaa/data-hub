"""Macro & cross-asset context for crypto: equities, rates, dollar, FX, commodities, crypto-equities, ETFs, FRED series."""
from __future__ import annotations

import io

import pandas as pd

from .. import basket
from ..collector import Collector, Ctx, register
from ..markets_common import sync_basket
from ..store import p, upsert, ts_parse


@register
class MacroMarkets(Collector):
    name = "markets_macro"
    group = "macro"
    title = "Macro & cross-asset prices (Yahoo)"
    description = ("S&P/Nasdaq/VIX, yields, DXY, FX majors + EM, gold/silver/oil/gas/copper, credit ETFs, sector ETFs, "
                   "crypto equities (MSTR, COIN, miners) and spot BTC/ETH ETFs - daily (full history) and hourly (2y).")
    first_run = "1-3 min"

    def run(self, ctx: Ctx) -> dict:
        rows = [r for r in basket.meta_rows() if r["basket"] == "macro"]
        return sync_basket(ctx, basket.macro_symbols(), rows, hourly=True, tag="macro")


@register
class Fred(Collector):
    name = "macro_fred"
    group = "macro"
    title = "FRED economic series"
    description = "Fed funds, SOFR, yields, curve, breakevens, Fed balance sheet, reverse repo, M2, CPI, unemployment, credit spreads, Brent/WTI, broad dollar."
    first_run = "1-2 min"
    default = False  # fred.stlouisfed.org is unreachable from some networks; macro_official covers the essentials

    def run(self, ctx: Ctx) -> dict:
        ok = fail = 0
        rows_total = 0
        consecutive = 0
        for sid in ctx.cfg["fred_series"]:
            if consecutive >= 2:
                ctx.info("FRED unreachable from here right now - skipping the rest (try again later)")
                break
            try:
                r = ctx.http.get("https://fred.stlouisfed.org/graph/fredgraph.csv", params={"id": sid}, timeout=15, retries=0)
                df = pd.read_csv(io.StringIO(r.text))
                df.columns = ["date", "value"]
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df = df.dropna(subset=["value"])
                df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ms]")
                _, tot = upsert(p("macro", "fred", f"{sid}.parquet"), df, keys=["date"], sort_by="date")
                rows_total += tot
                ok += 1
                consecutive = 0
            except Exception as e:
                fail += 1
                consecutive += 1
                ctx.info(f"{sid}: {type(e).__name__} {str(e)[:80]}")
        return {"series_ok": ok, "series_failed": fail, "rows": rows_total}
