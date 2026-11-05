"""Model-ready views: long daily panels (one parquet each) assembled from the per-symbol files.
These are what you load for research / RL environments instead of thousands of small files."""
from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq

from ..collector import Collector, Ctx, register
from ..markets_common import safe
from ..paths import DATA
from ..store import p, read, write


def _read_all(folder, columns, name_from_file=True) -> pd.DataFrame:
    frames = []
    for f in sorted(folder.glob("*.parquet")):
        try:
            df = pq.read_table(f, columns=columns).to_pandas()
        except Exception:
            continue
        if len(df):
            df["symbol"] = f.stem
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@register
class Views(Collector):
    name = "build_views"
    group = "crypto"
    title = "Build model-ready panels"
    description = ("Combines per-symbol files into: crypto_daily_panel (all coins), derivs_daily_panel (funding/OI/long-short), "
                   "markets_daily_panel + wide close matrices (macro basket), and a joint daily close table. Run after the collectors.")
    first_run = "1-2 min"
    needs = ["crypto_spot", "crypto_perps", "markets_macro"]

    def run(self, ctx: Ctx) -> dict:
        out: dict = {}
        v = lambda n: p("views", n)

        # 1) crypto daily panel ----------------------------------------------------------------------
        d = _read_all(DATA / "crypto" / "spot" / "1d", ["time", "open", "high", "low", "close", "volume", "quote_volume", "trades", "taker_buy_quote"])
        if len(d):
            d["date"] = d["time"].dt.tz_localize(None).dt.floor("D").astype("datetime64[ms]")
            d = d.drop(columns=["time"])
            d = d.sort_values(["symbol", "date"]).reset_index(drop=True)
            d["ret"] = d.groupby("symbol")["close"].pct_change()
            d["taker_buy_ratio"] = d["taker_buy_quote"] / d["quote_volume"].where(d["quote_volume"] > 0)
            write(d, v("crypto_daily_panel.parquet"))
            wide = d.pivot(index="date", columns="symbol", values="close")
            write(wide.reset_index(), v("crypto_daily_close_wide.parquet"))
            out["crypto_daily_rows"] = len(d)
            out["crypto_symbols"] = d["symbol"].nunique()

        # 2) derivatives daily panel -----------------------------------------------------------------------
        base = DATA / "crypto" / "perps" / "bybit"
        fund = _read_all(base / "funding", ["time", "funding_rate"])
        if len(fund):
            fund["date"] = fund["time"].dt.tz_localize(None).dt.floor("D").astype("datetime64[ms]")
            g = fund.groupby(["symbol", "date"]).agg(funding_mean=("funding_rate", "mean"), funding_sum=("funding_rate", "sum"), funding_n=("funding_rate", "size")).reset_index()
            oi = _read_all(base / "open_interest_1h", ["time", "open_interest"])
            if len(oi):
                oi["date"] = oi["time"].dt.tz_localize(None).dt.floor("D").astype("datetime64[ms]")
                g = g.merge(oi.sort_values("time").groupby(["symbol", "date"]).agg(oi_last=("open_interest", "last"), oi_mean=("open_interest", "mean")).reset_index(), on=["symbol", "date"], how="outer")
            ls = _read_all(base / "long_short_ratio_1h", ["time", "buy_ratio"])
            if len(ls):
                ls["date"] = ls["time"].dt.tz_localize(None).dt.floor("D").astype("datetime64[ms]")
                g = g.merge(ls.groupby(["symbol", "date"]).agg(long_ratio_mean=("buy_ratio", "mean")).reset_index(), on=["symbol", "date"], how="outer")
            pk = _read_all(base / "1d", ["time", "close", "turnover"])
            if len(pk):
                pk["date"] = pk["time"].dt.tz_localize(None).dt.floor("D").astype("datetime64[ms]")
                g = g.merge(pk.rename(columns={"close": "perp_close", "turnover": "perp_turnover"}).drop(columns=["time"]), on=["symbol", "date"], how="outer")
            g = g.sort_values(["symbol", "date"]).reset_index(drop=True)
            write(g, v("derivs_daily_panel.parquet"))
            out["derivs_rows"] = len(g)

        # 3) markets (Yahoo) ------------------------------------------------------------------------------------
        m = _read_all(DATA / "markets" / "daily", ["date", "open", "high", "low", "close", "adj_close", "volume"])
        if len(m):
            m = m.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last")
            write(m, v("markets_daily_panel.parquet"))
            wide = m.pivot(index="date", columns="symbol", values="close")
            write(wide.reset_index(), v("markets_daily_close_wide.parquet"))
            out["markets_rows"] = len(m)
            out["markets_symbols"] = m["symbol"].nunique()

        # 4) joint daily table: key crypto closes + every Yahoo symbol, aligned by date ----------------------------------------------
        try:
            cw = read(v("crypto_daily_close_wide.parquet"))
            mw = read(v("markets_daily_close_wide.parquet"))
            if cw is not None and mw is not None:
                keep_c = [c for c in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "PAXGUSDT") if c in cw.columns]
                keep_m = [c for c in mw.columns if c != "date"]
                joint = cw[["date"] + keep_c].merge(mw[["date"] + keep_m], on="date", how="outer").sort_values("date")
                write(joint, v("joint_daily_close.parquet"))
                out["joint_cols"] = joint.shape[1] - 1
        except Exception as e:
            ctx.info(f"joint table: {e}")
        return out
