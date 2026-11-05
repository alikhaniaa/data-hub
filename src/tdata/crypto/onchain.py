"""On-chain fundamentals: CoinMetrics Community API (free, no key) + mempool.space for Bitcoin network state."""
from __future__ import annotations

import json
import time

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import append_daily, last_value, now_utc, p, read, ts_from_s, upsert, write

CM = "https://community-api.coinmetrics.io/v4"
MP = "https://mempool.space/api"


def _catalog(ctx: Ctx) -> dict:
    cache = p("onchain", "_catalog.json")
    if cache.exists() and time.time() - cache.stat().st_mtime < 24 * 3600 and not ctx.full:
        try:
            return json.loads(cache.read_text(encoding="utf-8"))
        except Exception:
            pass
    d = ctx.http.json(f"{CM}/catalog/assets", timeout=120)["data"]
    out = {}
    for a in d:
        ms = []
        for m in a.get("metrics", []):
            for f in m.get("frequencies", []):
                if f.get("frequency") == "1d" and f.get("community"):
                    ms.append(m["metric"])
                    break
        if ms:
            out[a["asset"]] = ms
    cache.write_text(json.dumps(out), encoding="utf-8")
    return out


@register
class OnChain(Collector):
    name = "crypto_onchain"
    group = "crypto"
    title = "On-chain metrics (CoinMetrics community + mempool.space)"
    description = ("Daily active addresses, transfers, fees, supply, realized/market cap, NVT, hash rate, volatility... for ~28 major assets "
                   "(all community-tier metrics), plus Bitcoin mempool / fee / difficulty / mining-pool state.")
    first_run = "3-8 min"

    def run(self, ctx: Ctx) -> dict:
        http = ctx.http
        http.throttle("coinmetrics", 0.7)
        cat = _catalog(ctx)
        assets = [a for a in ctx.cfg["crypto"]["onchain_assets"] if a in cat]
        n_rows = 0
        metrics_total = 0
        for i, a in enumerate(assets, 1):
            path = p("onchain", "coinmetrics", f"{a}.parquet")
            last = None if ctx.full else last_value(path, "time")
            start = (last - pd.Timedelta(days=3)).strftime("%Y-%m-%d") if last is not None else None
            ms = cat[a]
            frames = []
            for k in range(0, len(ms), 40):
                chunk = ms[k:k + 40]
                params = {"assets": a, "metrics": ",".join(chunk), "frequency": "1d", "page_size": 10000}
                if start:
                    params["start_time"] = start
                url = f"{CM}/timeseries/asset-metrics"
                rows = []
                for _ in range(40):
                    try:
                        d = http.json(url, params=params, timeout=90)
                    except Exception as e:
                        ctx.info(f"{a} chunk {k}: {type(e).__name__} {str(e)[:70]}")
                        break
                    rows += d.get("data", [])
                    nxt = d.get("next_page_url")
                    if not nxt:
                        break
                    url, params = nxt, None
                if rows:
                    df = pd.DataFrame(rows).drop(columns=["asset"], errors="ignore")
                    df["time"] = pd.to_datetime(df["time"], utc=True).astype("datetime64[ms, UTC]")
                    for c in df.columns:
                        if c != "time":
                            df[c] = pd.to_numeric(df[c], errors="coerce")
                    frames.append(df.set_index("time"))
            if frames:
                wide = pd.concat(frames, axis=1).reset_index()
                wide = wide.loc[:, ~wide.columns.duplicated()]
                added, tot = upsert(path, wide, ["time"], "time")
                n_rows += added
                metrics_total += wide.shape[1] - 1
            ctx.progress(i, len(assets), f"{a}")

        # bitcoin network state ---------------------------------------------------------------------------
        stt = pd.Timestamp(now_utc()).as_unit("ms")
        try:
            hr = http.json(f"{MP}/v1/mining/hashrate/all", timeout=60)
            h = pd.DataFrame(hr["hashrates"])
            upsert(p("onchain", "btc_hashrate_mempool.parquet"), pd.DataFrame({"time": ts_from_s(h["timestamp"].values), "hashrate": h["avgHashrate"].astype(float).values}), ["time"], "time")
            dd = pd.DataFrame(hr["difficulty"])
            upsert(p("onchain", "btc_difficulty.parquet"), pd.DataFrame({"time": ts_from_s(dd["time"].values), "height": dd["height"].astype("int64").values,
                                                                         "difficulty": dd["difficulty"].astype(float).values, "adjustment": dd["adjustment"].astype(float).values}), ["time"], "time")
        except Exception as e:
            ctx.info(f"mempool hashrate: {type(e).__name__}")
        try:
            fee = http.json(f"{MP}/v1/fees/recommended")
            mp = http.json(f"{MP}/mempool")
            tip = http.text(f"{MP}/blocks/tip/height")
            row = {"snapshot_time": stt, "block_height": int(tip), "fee_fastest": fee["fastestFee"], "fee_hour": fee["hourFee"], "fee_economy": fee["economyFee"],
                   "mempool_count": mp["count"], "mempool_vsize": mp["vsize"], "mempool_total_fee_sat": mp["total_fee"]}
            try:
                ln = http.json(f"{MP}/v1/lightning/statistics/latest")["latest"]
                row.update({"ln_channels": ln.get("channel_count"), "ln_nodes": ln.get("node_count"), "ln_capacity_sat": ln.get("total_capacity")})
            except Exception:
                pass
            append_daily("btc_network", pd.DataFrame([row]), root="onchain/snapshots")
        except Exception as e:
            ctx.info(f"mempool snapshot: {type(e).__name__}")
        try:
            pools = http.json(f"{MP}/v1/mining/pools/1w")["pools"]
            pp = pd.DataFrame([{"snapshot_time": stt, "pool": x["name"], "blocks_1w": x["blockCount"], "share": x.get("share")} for x in pools])
            append_daily("btc_mining_pools", pp, root="onchain/snapshots")
        except Exception:
            pass
        return {"assets": len(assets), "rows_added": n_rows, "metric_columns": metrics_total}
