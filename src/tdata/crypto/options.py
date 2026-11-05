"""Deribit (public API): DVOL implied-volatility index history + live option-chain and futures-curve snapshots for BTC / ETH / SOL.
Implied vol is the market's own fear gauge; the chain snapshot gives skew, term structure and put/call positioning once you accumulate history."""
from __future__ import annotations

import re
import time

import pandas as pd

from ..collector import Collector, Ctx, register
from ..store import append_daily, last_value, now_utc, p, ts_from_ms, upsert

D = "https://www.deribit.com/api/v2/public"
MONTHS = {m: i for i, m in enumerate(["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}
INST = re.compile(r"^([A-Z]+)-(\d{1,2})([A-Z]{3})(\d{2})-(\d+(?:d\d+)?)-([CP])$")


def dvol(ctx: Ctx, cur: str, res: int, start_ms: int) -> pd.DataFrame:
    end = int(time.time() * 1000)
    rows: list = []
    for _ in range(80):
        j = ctx.http.json(f"{D}/get_volatility_index_data", params={"currency": cur, "start_timestamp": start_ms, "end_timestamp": end, "resolution": res}, timeout=60)["result"]
        data = j.get("data") or []
        if not data:
            break
        rows += data
        cont = j.get("continuation")
        if not cont or cont <= start_ms or len(data) < 2:
            break
        end = int(cont) - 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["t", "open", "high", "low", "close"]).drop_duplicates("t").sort_values("t")
    return pd.DataFrame({"time": ts_from_ms(df["t"].values), "open": df["open"].values, "high": df["high"].values, "low": df["low"].values, "close": df["close"].values})


@register
class Options(Collector):
    name = "crypto_options"
    group = "crypto"
    title = "Options & implied volatility (Deribit)"
    description = "DVOL index (BTC, ETH) hourly + daily history; every run snapshots the full BTC/ETH/SOL option chain (IV, OI, volume) and the futures curve."
    first_run = "1 min"

    def run(self, ctx: Ctx) -> dict:
        http = ctx.http
        http.throttle("deribit", 0.15)
        n: dict = {}
        for cur in ("BTC", "ETH"):
            for res, name, first in ((86400, "1d", 0), (3600, "1h", int((time.time() - 400 * 86400) * 1000))):
                path = p("crypto", "options", f"dvol_{cur}_{name}.parquet")
                last = None if ctx.full else last_value(path, "time")
                start = int(last.timestamp() * 1000) - res * 1000 * 2 if last is not None else first
                df = dvol(ctx, cur, res, start)
                if len(df):
                    n[f"dvol_{cur}_{name}"] = upsert(path, df, ["time"], "time")[1]
        t = pd.Timestamp(now_utc()).as_unit("ms")
        for cur in ("BTC", "ETH", "SOL"):
            try:
                opt = http.json(f"{D}/get_book_summary_by_currency", params={"currency": cur, "kind": "option"}, timeout=60)["result"]
            except Exception as e:
                ctx.info(f"{cur} options: {type(e).__name__}")
                continue
            rows = []
            for o in opt:
                m = INST.match(o["instrument_name"])
                if not m:
                    continue
                exp = pd.Timestamp(year=2000 + int(m.group(4)), month=MONTHS[m.group(3)], day=int(m.group(2)), hour=8, tz="UTC")
                rows.append({"snapshot_time": t, "underlying": cur, "instrument": o["instrument_name"], "expiry": exp, "strike": float(m.group(5).replace("d", ".")), "cp": m.group(6),
                             "mark_iv": o.get("mark_iv"), "mark_price": o.get("mark_price"), "underlying_price": o.get("underlying_price"), "open_interest": o.get("open_interest"),
                             "volume": o.get("volume"), "volume_usd": o.get("volume_usd"), "bid": o.get("bid_price"), "ask": o.get("ask_price")})
            if rows:
                df = pd.DataFrame(rows)
                df["expiry"] = df["expiry"].astype("datetime64[ms, UTC]")
                append_daily(f"chain_{cur}", df, root="crypto/options/snapshots")
                n[f"{cur}_options"] = len(df)
                c = df[df["cp"] == "C"]["open_interest"].sum()
                pu = df[df["cp"] == "P"]["open_interest"].sum()
                if c:
                    n[f"{cur}_put_call_oi"] = round(float(pu / c), 3)
            try:
                fut = http.json(f"{D}/get_book_summary_by_currency", params={"currency": cur, "kind": "future"}, timeout=60)["result"]
                fr = pd.DataFrame([{"snapshot_time": t, "underlying": cur, "instrument": f["instrument_name"], "mark_price": f.get("mark_price"), "open_interest": f.get("open_interest"),
                                    "volume_usd": f.get("volume_usd"), "estimated_delivery_price": f.get("estimated_delivery_price"), "funding_8h": f.get("funding_8h"),
                                    "current_funding": f.get("current_funding")} for f in fut])
                append_daily(f"futures_{cur}", fr, root="crypto/options/snapshots")
            except Exception as e:
                ctx.info(f"{cur} futures: {type(e).__name__}")
        return n
