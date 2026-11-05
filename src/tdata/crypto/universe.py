"""Reference data: which coins exist, where they trade, market caps, supply, categories, contract addresses."""
from __future__ import annotations

import json
import re

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import append_daily, now_utc, p, ts_from_ms, write, read
from .binance import SPOT_HOSTS, spot_get

CG = "https://api.coingecko.com/api/v3"
BYBIT = "https://api.bybit.com"
OKX = "https://www.okx.com"
HL = "https://api.hyperliquid.xyz/info"


def cg_get(ctx: Ctx, path: str, params: dict | None = None):
    key = ctx.cfg["crypto"].get("coingecko_key") or ""
    headers = {"x-cg-demo-api-key": key} if key else {}
    return ctx.http.json(CG + path, params=params, headers=headers)


def strip_k(base: str) -> str:
    """1000PEPE -> PEPE, 1MBABYDOGE -> BABYDOGE, 10000SATS -> SATS (perp contract multipliers)."""
    return re.sub(r"^(1000000|10000|1000|1M)", "", base)


@register
class Universe(Collector):
    name = "crypto_universe"
    group = "crypto"
    title = "Coin universe & reference data"
    description = ("Binance/Bybit/OKX/Hyperliquid symbol lists, CoinGecko top-1000 snapshot (price, market cap, FDV, supply, "
                   "ATH, returns), categories, contract addresses, and a unified coin <-> venue symbol map.")
    first_run = "1-2 min"

    def run(self, ctx: Ctx) -> dict:
        cfg = ctx.cfg["crypto"]
        http = ctx.http
        key = cfg.get("coingecko_key") or ""
        http.throttle("coingecko", 2.2 if key else 6.5)
        meta = lambda n: p("crypto", "meta", n)
        stats: dict = {}

        # 1) Binance spot ----------------------------------------------------------------
        ctx.info("Binance spot exchangeInfo + 24h tickers")
        ei = spot_get(http, "/api/v3/exchangeInfo")
        rows = []
        for s in ei["symbols"]:
            f = {x["filterType"]: x for x in s.get("filters", [])}
            rows.append({
                "symbol": s["symbol"], "base": s["baseAsset"], "quote": s["quoteAsset"], "status": s["status"],
                "spot_allowed": s.get("isSpotTradingAllowed"), "margin_allowed": s.get("isMarginTradingAllowed"),
                "tick_size": float(f.get("PRICE_FILTER", {}).get("tickSize", 0) or 0),
                "step_size": float(f.get("LOT_SIZE", {}).get("stepSize", 0) or 0),
                "min_notional": float((f.get("NOTIONAL") or f.get("MIN_NOTIONAL") or {}).get("minNotional", 0) or 0),
            })
        bsp = pd.DataFrame(rows)
        tick = pd.DataFrame(spot_get(http, "/api/v3/ticker/24hr"))
        tick = tick[["symbol", "lastPrice", "quoteVolume", "priceChangePercent", "count"]].rename(columns={
            "lastPrice": "last_price", "quoteVolume": "quote_volume_24h", "priceChangePercent": "change_24h_pct", "count": "trades_24h"})
        for c in ("last_price", "quote_volume_24h", "change_24h_pct", "trades_24h"):
            tick[c] = pd.to_numeric(tick[c], errors="coerce")
        bsp = bsp.merge(tick, on="symbol", how="left")
        bsp["asof"] = pd.Timestamp(now_utc()).as_unit("ms")
        write(bsp, meta("binance_spot_symbols.parquet"))
        stats["binance_spot_symbols"] = len(bsp)

        # 2) Bybit linear (USDT / USDC perps + dated futures) ---------------------------------
        ctx.info("Bybit instruments")
        rows, cursor = [], ""
        while True:
            d = http.json(BYBIT + "/v5/market/instruments-info", params={"category": "linear", "limit": 1000, "cursor": cursor})["result"]
            for x in d["list"]:
                rows.append({
                    "symbol": x["symbol"], "base": x["baseCoin"], "quote": x["quoteCoin"], "settle": x.get("settleCoin"),
                    "contract_type": x.get("contractType"), "status": x["status"], "launch_ms": int(x.get("launchTime") or 0),
                    "funding_interval_min": int(x.get("fundingInterval") or 0),
                    "max_leverage": float(x["leverageFilter"]["maxLeverage"]), "tick_size": float(x["priceFilter"]["tickSize"]),
                    "min_qty": float(x["lotSizeFilter"]["minOrderQty"]), "qty_step": float(x["lotSizeFilter"]["qtyStep"]),
                    "min_notional": float(x["lotSizeFilter"].get("minNotionalValue") or 0),
                })
            cursor = d.get("nextPageCursor") or ""
            if not cursor:
                break
        byb = pd.DataFrame(rows)
        # turnover from tickers to rank
        tk = http.json(BYBIT + "/v5/market/tickers", params={"category": "linear"})["result"]["list"]
        tk = pd.DataFrame(tk)[["symbol", "turnover24h", "openInterestValue", "fundingRate"]]
        for c in ("turnover24h", "openInterestValue", "fundingRate"):
            tk[c] = pd.to_numeric(tk[c], errors="coerce")
        byb = byb.merge(tk.rename(columns={"turnover24h": "turnover_24h", "openInterestValue": "oi_value", "fundingRate": "funding_rate"}), on="symbol", how="left")
        byb["launch"] = pd.to_datetime(byb["launch_ms"], unit="ms", utc=True).astype("datetime64[ms, UTC]")
        write(byb, meta("bybit_linear_symbols.parquet"))
        stats["bybit_linear"] = len(byb)

        # 3) OKX swaps -----------------------------------------------------------------------
        ctx.info("OKX instruments")
        okx = pd.DataFrame(http.json(OKX + "/api/v5/public/instruments", params={"instType": "SWAP"})["data"])
        keep = [c for c in ("instId", "instFamily", "ctType", "ctVal", "ctValCcy", "settleCcy", "state", "listTime", "lever", "tickSz", "lotSz", "minSz") if c in okx.columns]
        okx = okx[keep].rename(columns={"instId": "symbol", "instFamily": "family"})
        write(okx, meta("okx_swap_symbols.parquet"))
        stats["okx_swaps"] = len(okx)

        # 4) Hyperliquid (main dex + HIP-3 builder dexes: stocks, commodities, FX ...) ------------
        ctx.info("Hyperliquid perps")
        dexs = [None] + [d["name"] for d in http.post_json(HL, {"type": "perpDexs"}) if d]
        rows = []
        for dx in dexs:
            try:
                body = {"type": "meta"} if dx is None else {"type": "meta", "dex": dx}
                for i, u in enumerate(http.post_json(HL, body)["universe"]):
                    rows.append({"dex": dx or "main", "name": u["name"], "sz_decimals": u.get("szDecimals"), "max_leverage": u.get("maxLeverage"),
                                 "only_isolated": bool(u.get("onlyIsolated", False)), "delisted": bool(u.get("isDelisted", False))})
            except Exception as e:
                ctx.info(f"hyperliquid dex {dx} skipped: {e}")
        hl = pd.DataFrame(rows)
        write(hl, meta("hyperliquid_perps.parquet"))
        stats["hyperliquid_perps"] = len(hl)

        # 5) CoinGecko ------------------------------------------------------------------------
        top = int(cfg.get("coingecko_top", 1000))
        ctx.info(f"CoinGecko markets top {top} (rate-limited, ~{top // 250 * 7}s)")
        mk = []
        for page in range(1, (top + 249) // 250 + 1):
            try:
                mk += cg_get(ctx, "/coins/markets", {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": page,
                                                      "sparkline": "false", "price_change_percentage": "1h,24h,7d,14d,30d,200d,1y"})
            except Exception as e:
                ctx.info(f"coingecko page {page} failed: {e}")
                break
            ctx.progress(page, (top + 249) // 250, "coingecko markets")
        if mk:
            cgdf = pd.DataFrame(mk).drop(columns=["image", "roi", "sparkline_in_7d"], errors="ignore")
            cgdf["snapshot_time"] = pd.Timestamp(now_utc()).as_unit("ms")
            for c in cgdf.columns:
                if c.endswith("_date") or c == "last_updated":
                    cgdf[c] = pd.to_datetime(cgdf[c], utc=True, errors="coerce").astype("datetime64[ms, UTC]")
            day = now_utc().strftime("%Y%m%d")
            write(cgdf, p("crypto", "universe", "coingecko_markets", f"{day}.parquet"))
            write(cgdf, p("crypto", "universe", "coingecko_markets_latest.parquet"))
            stats["coingecko_coins"] = len(cgdf)
        else:
            cgdf = read(p("crypto", "universe", "coingecko_markets_latest.parquet"))

        try:
            cats = pd.DataFrame(cg_get(ctx, "/coins/categories"))
            cats["top_3_coins"] = cats["top_3_coins"].map(lambda x: json.dumps(x))
            cats["snapshot_time"] = pd.Timestamp(now_utc()).as_unit("ms")
            cats["updated_at"] = pd.to_datetime(cats["updated_at"], utc=True, errors="coerce").astype("datetime64[ms, UTC]")
            cats = cats.drop(columns=["content"], errors="ignore")
            append_daily("coingecko_categories", cats, root="crypto/snapshots")
            stats["categories"] = len(cats)
        except Exception as e:
            ctx.info(f"categories skipped: {e}")

        try:
            g = cg_get(ctx, "/global")["data"]
            row = {"snapshot_time": pd.Timestamp(now_utc()).as_unit("ms"), "active_cryptos": g.get("active_cryptocurrencies"),
                   "markets": g.get("markets"), "total_mcap_usd": g["total_market_cap"]["usd"], "total_volume_usd": g["total_volume"]["usd"],
                   "mcap_change_24h_pct": g.get("market_cap_change_percentage_24h_usd")}
            for k, v in g.get("market_cap_percentage", {}).items():
                row[f"dominance_{k}"] = v
            append_daily("coingecko_global", pd.DataFrame([row]), root="crypto/snapshots")
        except Exception as e:
            ctx.info(f"global skipped: {e}")

        try:
            lst = cg_get(ctx, "/coins/list", {"include_platform": "true"})
            cl = pd.DataFrame([{"id": c["id"], "symbol": c["symbol"], "name": c["name"], "platforms": json.dumps(c.get("platforms") or {})} for c in lst])
            write(cl, meta("coingecko_coins_list.parquet"))
            stats["coingecko_all_coins"] = len(cl)
        except Exception as e:
            ctx.info(f"coin list skipped: {e}")

        # 6) unified symbol map ------------------------------------------------------------------
        ctx.info("building symbol map")
        if cgdf is not None and len(cgdf):
            c2 = cgdf.sort_values("market_cap_rank").copy()
            c2["SYM"] = c2["symbol"].str.upper()
            best = c2.drop_duplicates("SYM", keep="first").set_index("SYM")
            dup = set(c2.loc[c2["SYM"].duplicated(keep=False), "SYM"])
            usdt = bsp[(bsp["quote"] == "USDT")].copy()
            usdt["cg_id"] = usdt["base"].map(best["id"])
            usdt["cg_rank"] = usdt["base"].map(best["market_cap_rank"])
            usdt["cg_mcap"] = usdt["base"].map(best["market_cap"])
            usdt["cg_ambiguous_symbol"] = usdt["base"].isin(dup)
            usdt["on_bybit_perp"] = usdt["base"].isin(set(byb.loc[byb["quote"] == "USDT", "base"].map(strip_k)))
            usdt["on_okx_swap"] = usdt["base"].isin(set(okx["family"].dropna().str.split("-").str[0].map(strip_k)))
            usdt["on_hyperliquid"] = usdt["base"].isin(set(hl.loc[~hl["delisted"], "name"].map(lambda s: strip_k(s.split(":")[-1].lstrip("k")))))
            write(usdt, meta("symbol_map.parquet"))
            stats["symbol_map_rows"] = len(usdt)
        return stats
