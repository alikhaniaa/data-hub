"""Point-in-time market snapshots. Free APIs have no history for these, so history only exists if we record it.
Schedule this often (every 5-60 min) to build order-flow-ish datasets: tickers, funding, OI, basis, book depth."""
from __future__ import annotations

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import append_daily, now_utc, p, read
from .binance import spot_get

BY = "https://api.bybit.com"
HL = "https://api.hyperliquid.xyz/info"


def _now() -> pd.Timestamp:
    return pd.Timestamp(now_utc()).as_unit("ms")


def _num(df: pd.DataFrame, cols) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@register
class Snapshots(Collector):
    name = "crypto_snapshots"
    group = "crypto"
    title = "Live snapshots (tickers, funding, OI, order-book depth)"
    description = ("Bybit all-perp tickers (mark/index/funding/OI/volume), Binance spot 24h tickers, Hyperliquid contexts incl. "
                   "oil/gold perps, OKX funding, and top-of-book depth profile of the 25 most liquid spot pairs. Appends to today's file.")
    first_run = "20 s"

    def run(self, ctx: Ctx) -> dict:
        http = ctx.http
        t = _now()
        n = {}

        # Bybit linear tickers ------------------------------------------------------------
        lst = http.json(BY + "/v5/market/tickers", params={"category": "linear"})["result"]["list"]
        df = pd.DataFrame(lst)
        keep = ["symbol", "lastPrice", "markPrice", "indexPrice", "fundingRate", "nextFundingTime", "openInterest", "openInterestValue",
                "volume24h", "turnover24h", "price24hPcnt", "bid1Price", "ask1Price", "bid1Size", "ask1Size", "basis", "basisRate", "predictedDeliveryPrice"]
        df = df[[c for c in keep if c in df.columns]]
        df = _num(df, [c for c in df.columns if c not in ("symbol",)])
        df["nextFundingTime"] = pd.to_datetime(df["nextFundingTime"], unit="ms", utc=True).astype("datetime64[ms, UTC]")
        df.insert(0, "snapshot_time", t)
        append_daily("bybit_tickers", df, root="crypto/snapshots")
        n["bybit_tickers"] = len(df)

        # Binance spot 24h --------------------------------------------------------------------
        tk = pd.DataFrame(spot_get(http, "/api/v3/ticker/24hr"))
        tk = tk[tk["symbol"].str.endswith("USDT")]
        tk = tk[["symbol", "lastPrice", "bidPrice", "askPrice", "bidQty", "askQty", "priceChangePercent", "weightedAvgPrice", "volume", "quoteVolume", "count", "highPrice", "lowPrice"]]
        tk = _num(tk, [c for c in tk.columns if c != "symbol"])
        tk = tk[tk["quoteVolume"] > 0]
        tk.insert(0, "snapshot_time", t)
        append_daily("binance_spot_tickers", tk, root="crypto/snapshots")
        n["binance_spot_tickers"] = len(tk)

        # Hyperliquid (main + xyz commodities) ------------------------------------------------
        rows = []
        for dex in (None, "xyz"):
            body = {"type": "metaAndAssetCtxs"} if dex is None else {"type": "metaAndAssetCtxs", "dex": dex}
            try:
                meta, cx = http.post_json(HL, body)
            except Exception as e:
                ctx.info(f"hyperliquid {dex}: {e}")
                continue
            for u, c in zip(meta["universe"], cx):
                rows.append({"coin": u["name"], "dex": dex or "main", "mark_px": c.get("markPx"), "mid_px": c.get("midPx"), "oracle_px": c.get("oraclePx"),
                             "funding": c.get("funding"), "premium": c.get("premium"), "open_interest": c.get("openInterest"),
                             "day_volume_usd": c.get("dayNtlVlm"), "prev_day_px": c.get("prevDayPx"), "impact_bid": (c.get("impactPxs") or [None, None])[0],
                             "impact_ask": (c.get("impactPxs") or [None, None])[1]})
        if rows:
            h = _num(pd.DataFrame(rows), ["mark_px", "mid_px", "oracle_px", "funding", "premium", "open_interest", "day_volume_usd", "prev_day_px", "impact_bid", "impact_ask"])
            h.insert(0, "snapshot_time", t)
            append_daily("hyperliquid_ctx", h, root="crypto/snapshots")
            n["hyperliquid_ctx"] = len(h)

        # order-book depth profile, 25 most liquid spot pairs -----------------------------------------
        top = tk.sort_values("quoteVolume", ascending=False)["symbol"].head(25).tolist()
        prof = []
        for s in top:
            try:
                d = spot_get(http, "/api/v3/depth", {"symbol": s, "limit": 500})
            except Exception:
                continue
            bids = pd.DataFrame(d["bids"], columns=["p", "q"]).astype(float)
            asks = pd.DataFrame(d["asks"], columns=["p", "q"]).astype(float)
            if bids.empty or asks.empty:
                continue
            mid = (bids["p"].iloc[0] + asks["p"].iloc[0]) / 2
            row = {"snapshot_time": t, "symbol": s, "mid": mid, "spread_bps": (asks["p"].iloc[0] - bids["p"].iloc[0]) / mid * 1e4}
            for bps in (10, 50, 100, 200):
                row[f"bid_depth_usd_{bps}bps"] = float((bids.loc[bids["p"] >= mid * (1 - bps / 1e4), "p"] * bids.loc[bids["p"] >= mid * (1 - bps / 1e4), "q"]).sum())
                row[f"ask_depth_usd_{bps}bps"] = float((asks.loc[asks["p"] <= mid * (1 + bps / 1e4), "p"] * asks.loc[asks["p"] <= mid * (1 + bps / 1e4), "q"]).sum())
            prof.append(row)
        if prof:
            append_daily("binance_depth_profile", pd.DataFrame(prof), root="crypto/snapshots")
            n["depth_profile"] = len(prof)
        return n
