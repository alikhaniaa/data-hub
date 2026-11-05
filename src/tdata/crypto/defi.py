"""DeFi / on-chain economy from DefiLlama (free tier): TVL, stablecoin supply, DEX volume, fees, yields, hacks."""
from __future__ import annotations

import json

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import now_utc, p, ts_from_s, upsert, write

LL = "https://api.llama.fi"
ST = "https://stablecoins.llama.fi"
YL = "https://yields.llama.fi"


def _fresh(path, hours: float = 20) -> bool:
    """True when the file was written recently (history endpoints change once a day, no need to re-pull)."""
    import time
    try:
        return time.time() - path.stat().st_mtime < hours * 3600
    except OSError:
        return False


def _snap_time() -> pd.Timestamp:
    return pd.Timestamp(now_utc()).as_unit("ms")


def _num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@register
class DeFi(Collector):
    name = "crypto_defi"
    group = "crypto"
    title = "DeFi, stablecoins, DEX volume, fees, yields (DefiLlama)"
    description = ("Protocol & chain TVL (current + history), total and per-coin stablecoin supply, DEX volume, protocol fees/revenue, "
                   "yield pools (APY history accumulates daily), hack database.")
    first_run = "2-4 min"

    def run(self, ctx: Ctx) -> dict:
        http = ctx.http
        http.throttle("llama.fi", 0.15)
        day = now_utc().strftime("%Y%m%d")
        st = _snap_time()
        n: dict = {}

        # protocols ----------------------------------------------------------------------------
        prot = pd.DataFrame(http.json(f"{LL}/protocols"))
        keep = ["id", "name", "slug", "symbol", "category", "chains", "tvl", "change_1h", "change_1d", "change_7d", "mcap", "listedAt", "gecko_id", "parentProtocol", "audits", "url"]
        prot = prot[[c for c in keep if c in prot.columns]].copy()
        prot["chains"] = prot["chains"].map(lambda x: json.dumps(x) if isinstance(x, list) else x)
        prot["n_chains"] = prot["chains"].map(lambda x: len(json.loads(x)) if isinstance(x, str) and x.startswith("[") else 0)
        _num(prot, ["tvl", "change_1h", "change_1d", "change_7d", "mcap", "listedAt"])
        prot["mcap_to_tvl"] = prot["mcap"] / prot["tvl"].where(prot["tvl"] > 0)
        prot["snapshot_time"] = st
        prot = prot.drop(columns=["audits", "parentProtocol", "url"], errors="ignore")
        write(prot, p("defi", "protocols", f"{day}.parquet"))
        write(prot, p("defi", "protocols_latest.parquet"))
        n["protocols"] = len(prot)

        # chains + history ---------------------------------------------------------------------------
        chains = pd.DataFrame(http.json(f"{LL}/v2/chains"))
        chains["snapshot_time"] = st
        write(chains, p("defi", "chains_latest.parquet"))
        tot = pd.DataFrame(http.json(f"{LL}/v2/historicalChainTvl"))
        upsert(p("defi", "tvl_history", "_ALL.parquet"), pd.DataFrame({"time": ts_from_s(tot["date"].values), "tvl": tot["tvl"].astype(float).values}), ["time"], "time")
        top_chains = chains.sort_values("tvl", ascending=False)["name"].head(40).tolist()
        for i, ch in enumerate(top_chains, 1):
            if _fresh(p("defi", "tvl_history", f"{ch.replace(' ', '_')}.parquet")) and not ctx.full:
                continue
            try:
                h = pd.DataFrame(http.json(f"{LL}/v2/historicalChainTvl/{ch}"))
                if len(h):
                    upsert(p("defi", "tvl_history", f"{ch.replace(' ', '_')}.parquet"), pd.DataFrame({"time": ts_from_s(h["date"].values), "tvl": h["tvl"].astype(float).values}), ["time"], "time")
            except Exception as e:
                ctx.info(f"chain {ch}: {e}")
            ctx.progress(i, len(top_chains), "chain TVL history")
        n["chains"] = len(top_chains)

        # protocol-level TVL history for the 80 biggest ---------------------------------------------------
        cnt = 0
        for slug in prot.sort_values("tvl", ascending=False)["slug"].head(80):
            if _fresh(p("defi", "protocol_tvl", f"{slug}.parquet")) and not ctx.full:
                cnt += 1
                continue
            try:
                d = http.json(f"{LL}/protocol/{slug}")
                arr = d.get("tvl") or []
                if arr:
                    h = pd.DataFrame(arr)
                    upsert(p("defi", "protocol_tvl", f"{slug}.parquet"), pd.DataFrame({"time": ts_from_s(h["date"].values), "tvl": h["totalLiquidityUSD"].astype(float).values}), ["time"], "time")
                    cnt += 1
            except Exception as e:
                ctx.info(f"protocol {slug}: {type(e).__name__}")
        n["protocol_tvl_series"] = cnt

        # stablecoins ---------------------------------------------------------------------------------
        sl = http.json(f"{ST}/stablecoins", params={"includePrices": "true"})["peggedAssets"]
        sdf = pd.DataFrame([{
            "id": s["id"], "name": s["name"], "symbol": s["symbol"], "peg_type": s.get("pegType"), "mechanism": s.get("pegMechanism"),
            "circulating_usd": (s.get("circulating") or {}).get("peggedUSD"), "prev_day": (s.get("circulatingPrevDay") or {}).get("peggedUSD"),
            "prev_week": (s.get("circulatingPrevWeek") or {}).get("peggedUSD"), "prev_month": (s.get("circulatingPrevMonth") or {}).get("peggedUSD"),
            "price": s.get("price"), "chains": json.dumps(s.get("chains") or []), "snapshot_time": st} for s in sl])
        _num(sdf, ["circulating_usd", "prev_day", "prev_week", "prev_month", "price"])
        write(sdf, p("defi", "stablecoins_latest.parquet"))
        write(sdf, p("defi", "stablecoins", f"{day}.parquet"))

        def chart(url, path):
            arr = http.json(url)
            rows = [(int(a["date"]), (a.get("totalCirculatingUSD") or {}).get("peggedUSD")) for a in arr]
            df = pd.DataFrame(rows, columns=["t", "supply_usd"]).dropna()
            if len(df):
                upsert(path, pd.DataFrame({"time": ts_from_s(df["t"].values), "supply_usd": df["supply_usd"].astype(float).values}), ["time"], "time")

        chart(f"{ST}/stablecoincharts/all", p("defi", "stablecoin_supply", "_ALL.parquet"))
        for _, s in sdf.sort_values("circulating_usd", ascending=False).head(10).iterrows():
            try:
                chart(f"{ST}/stablecoincharts/all?stablecoin={s['id']}", p("defi", "stablecoin_supply", f"{s['symbol']}.parquet"))
            except Exception as e:
                ctx.info(f"stablecoin {s['symbol']}: {e}")
        for ch in ("Ethereum", "Tron", "Solana", "BSC", "Arbitrum", "Base", "Polygon"):
            try:
                chart(f"{ST}/stablecoincharts/{ch}", p("defi", "stablecoin_supply", f"chain_{ch}.parquet"))
            except Exception:
                pass
        n["stablecoins"] = len(sdf)

        # DEX volume + fees ---------------------------------------------------------------------------------
        for kind, fname in (("dexs", "dex_volume"), ("fees", "fees")):
            try:
                d = http.json(f"{LL}/overview/{kind}", params={"excludeTotalDataChartBreakdown": "true"} if kind == "dexs" else {"excludeTotalDataChart": "true", "excludeTotalDataChartBreakdown": "true"}, timeout=120)
                if d.get("totalDataChart"):
                    c = pd.DataFrame(d["totalDataChart"], columns=["t", "value"])
                    upsert(p("defi", f"{fname}_total.parquet"), pd.DataFrame({"time": ts_from_s(c["t"].values), "usd": c["value"].astype(float).values}), ["time"], "time")
                rows = []
                for x in d.get("protocols", []):
                    rows.append({"name": x.get("name"), "module": x.get("module"), "category": x.get("category"), "chains": json.dumps(x.get("chains") or []),
                                 "total24h": x.get("total24h"), "total7d": x.get("total7d"), "total30d": x.get("total30d"), "total1y": x.get("total1y"),
                                 "change_1d": x.get("change_1d"), "change_7d": x.get("change_7d"), "change_1m": x.get("change_1m"), "snapshot_time": st})
                pdf = _num(pd.DataFrame(rows), ["total24h", "total7d", "total30d", "total1y", "change_1d", "change_7d", "change_1m"])
                write(pdf, p("defi", f"{fname}_protocols_latest.parquet"))
                write(pdf, p("defi", fname + "_protocols", f"{day}.parquet"))
                n[fname] = len(pdf)
            except Exception as e:
                ctx.info(f"{kind}: {type(e).__name__} {str(e)[:80]}")

        # yields -------------------------------------------------------------------------------------------
        try:
            ys = http.json(f"{YL}/pools", timeout=120)["data"]
            y = pd.DataFrame(ys)
            keepy = ["pool", "chain", "project", "symbol", "tvlUsd", "apyBase", "apyReward", "apy", "apyPct1D", "apyPct7D", "apyPct30D", "stablecoin",
                     "ilRisk", "exposure", "predictedClass", "volumeUsd1d", "volumeUsd7d", "poolMeta"]
            y = y[[c for c in keepy if c in y.columns]]
            y = _num(y, ["tvlUsd", "apyBase", "apyReward", "apy", "apyPct1D", "apyPct7D", "apyPct30D", "volumeUsd1d", "volumeUsd7d"])
            y = y[y["tvlUsd"] >= 1_000_000].copy()
            y["snapshot_time"] = st
            write(y, p("defi", "yields", f"{day}.parquet"))
            n["yield_pools"] = len(y)
        except Exception as e:
            ctx.info(f"yields: {type(e).__name__} {str(e)[:80]}")

        # hacks -----------------------------------------------------------------------------------------------
        try:
            hk = pd.DataFrame(http.json(f"{LL}/hacks"))
            hk["date"] = ts_from_s(hk["date"].values)
            hk["chain"] = hk["chain"].map(lambda x: json.dumps(x) if isinstance(x, list) else x)
            keep = [c for c in ["date", "name", "classification", "technique", "amount", "chain", "bridgeHack", "targetType", "returnedFunds"] if c in hk.columns]
            write(hk[keep], p("defi", "hacks.parquet"))
            n["hacks"] = len(hk)
        except Exception as e:
            ctx.info(f"hacks: {type(e).__name__}")
        return n
