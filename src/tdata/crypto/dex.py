"""Long-tail / DEX-only tokens: GeckoTerminal pools across major chains plus DexScreener token-profile and boost lists.
Snapshots every run (new pools, trending pools, top-volume pools) and daily/hourly candles for the biggest pools."""
from __future__ import annotations

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import append_daily, now_utc, p, ts_from_s, upsert

G = "https://api.geckoterminal.com/api/v2"
WANT = ["eth", "solana", "bsc", "base", "arbitrum", "polygon_pos", "avax", "ton", "sui-network", "tron"]  # extend via config crypto.dex_networks
HDR = {"Accept": "application/json;version=20230302"}


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def pool_rows(items: list[dict], network: str | None, source: str, t) -> list[dict]:
    rows = []
    for it in items:
        a = it["attributes"]
        rel = it.get("relationships", {})
        net = network or (rel.get("network", {}).get("data", {}) or {}).get("id") or it["id"].split("_")[0]
        pc = a.get("price_change_percentage") or {}
        vol = a.get("volume_usd") or {}
        tx = (a.get("transactions") or {}).get("h24") or {}
        base_id = (rel.get("base_token", {}).get("data", {}) or {}).get("id", "")
        rows.append({
            "snapshot_time": t, "source": source, "network": net, "pool": a.get("address"), "name": a.get("name"),
            "dex": (rel.get("dex", {}).get("data", {}) or {}).get("id"), "base_token": base_id.split("_", 1)[-1] if base_id else None,
            "price_usd": _f(a.get("base_token_price_usd")), "fdv_usd": _f(a.get("fdv_usd")), "mcap_usd": _f(a.get("market_cap_usd")),
            "liquidity_usd": _f(a.get("reserve_in_usd")), "created": a.get("pool_created_at"),
            "vol_h1": _f(vol.get("h1")), "vol_h6": _f(vol.get("h6")), "vol_h24": _f(vol.get("h24")),
            "chg_m5": _f(pc.get("m5")), "chg_h1": _f(pc.get("h1")), "chg_h6": _f(pc.get("h6")), "chg_h24": _f(pc.get("h24")),
            "buys_h24": tx.get("buys"), "sells_h24": tx.get("sells"), "buyers_h24": tx.get("buyers"), "sellers_h24": tx.get("sellers"),
        })
    return rows


@register
class Dex(Collector):
    name = "crypto_dex"
    group = "crypto"
    title = "DEX long-tail tokens (GeckoTerminal + DexScreener)"
    description = ("New / trending / top-volume DEX pools on up to 10 chains (price, FDV, liquidity, 5m-24h change, buyers/sellers), candles for the biggest pools, "
                   "DexScreener latest token profiles & boosted tokens. Many of these tokens are illiquid or short-lived: "
                   "filter on liquidity and pool age.")
    first_run = "5-8 min"

    def run(self, ctx: Ctx) -> dict:
        http = ctx.http
        http.throttle("geckoterminal", 2.3)
        http.throttle("dexscreener", 0.5)
        t = pd.Timestamp(now_utc()).as_unit("ms")
        rows: list[dict] = []
        nets = []
        for pg in (1, 2, 3):
            try:
                nets += [n["id"] for n in http.json(f"{G}/networks", params={"page": pg}, headers=HDR)["data"]]
            except Exception:
                break
        want = ctx.cfg["crypto"].get("dex_networks") or WANT
        nets = [n for n in want if n in nets] or want[:6]
        ctx.info(f"networks: {nets}")
        try:
            rows += pool_rows(http.json(f"{G}/networks/trending_pools", params={"page": 1}, headers=HDR)["data"], None, "trending_global", t)
        except Exception as e:
            ctx.info(f"global trending: {type(e).__name__}")
        for i, net in enumerate(nets, 1):
            for source, path, pages, extra in (("top_volume", "pools", (1, 2), {"sort": "h24_volume_usd_desc"}), ("new", "new_pools", (1,), {}), ("trending", "trending_pools", (1,), {})):
                for pg in pages:
                    try:
                        j = http.json(f"{G}/networks/{net}/{path}", params={"page": pg, **extra}, headers=HDR)
                    except InterruptedError:
                        raise
                    except Exception as e:
                        ctx.info(f"{net}/{path} p{pg}: {type(e).__name__}")
                        break
                    rows += pool_rows(j.get("data", []), net, source, t)
            ctx.progress(i, len(nets), f"network {net}")
        n = {"pool_rows": len(rows)}
        if rows:
            df = pd.DataFrame(rows)
            df["created"] = pd.to_datetime(df["created"], utc=True, errors="coerce").astype("datetime64[ms, UTC]")
            append_daily("pools", df, root="crypto/dex")
            # candles for the biggest liquid pools
            big = (df[(df["source"] == "top_volume") & (df["liquidity_usd"].fillna(0) >= 250_000)].sort_values("vol_h24", ascending=False)
                   .drop_duplicates(["network", "pool"]).head(25))
            ok = 0
            for k, (_, r) in enumerate(big.iterrows(), 1):
                for tf, lim in (("day", 400), ("hour", 500)):
                    path = p("crypto", "dex", f"ohlcv_{tf}", f"{r['network']}__{r['pool']}.parquet")
                    try:
                        j = http.json(f"{G}/networks/{r['network']}/pools/{r['pool']}/ohlcv/{tf}", params={"aggregate": 1, "limit": lim if not path.exists() else 30}, headers=HDR)
                        lst = j["data"]["attributes"]["ohlcv_list"]
                    except InterruptedError:
                        raise
                    except Exception:
                        continue
                    if lst:
                        c = pd.DataFrame(lst, columns=["t", "open", "high", "low", "close", "volume_usd"]).sort_values("t")
                        out = pd.DataFrame({"time": ts_from_s(c["t"].values), "open": c["open"].values, "high": c["high"].values, "low": c["low"].values,
                                            "close": c["close"].values, "volume_usd": c["volume_usd"].values})
                        upsert(path, out, ["time"], "time")
                        ok += 1
                if k % 5 == 0:
                    ctx.progress(k, len(big), "pool candles")
            n["pool_candle_series"] = ok
        # DexScreener token profiles and boosts
        try:
            prof = http.json("https://api.dexscreener.com/token-profiles/latest/v1")
            boosts = http.json("https://api.dexscreener.com/token-boosts/top/v1")
            pr = pd.DataFrame([{"snapshot_time": t, "list": "profile", "chain": x.get("chainId"), "token": x.get("tokenAddress"), "description": (x.get("description") or "")[:300], "url": x.get("url")} for x in prof])
            bo = pd.DataFrame([{"snapshot_time": t, "list": "boost_top", "chain": x.get("chainId"), "token": x.get("tokenAddress"), "description": (x.get("description") or "")[:300],
                                "url": x.get("url"), "boost_total": x.get("totalAmount")} for x in boosts])
            both = pd.concat([pr, bo], ignore_index=True)
            if len(both):
                append_daily("dexscreener_tokens", both, root="crypto/dex")
                n["dexscreener_rows"] = len(both)
        except Exception as e:
            ctx.info(f"dexscreener: {type(e).__name__}")
        return n
