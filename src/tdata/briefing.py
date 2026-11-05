"""Snapshot: reads whatever has been collected and prints the current picture (no network access)."""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from .markets_common import safe
from .paths import DATA
from .store import p, read, status_load


def _ser(path, col="close", idx="date"):
    df = read(path)
    if df is None or df.empty:
        return None
    s = df.set_index(idx)[col].dropna()
    s.index = pd.to_datetime(s.index).tz_localize(None) if getattr(s.index, "tz", None) is not None else pd.to_datetime(s.index)
    return s[~s.index.duplicated(keep="last")].sort_index()


def _chg(s: pd.Series, days: int) -> float | None:
    if s is None or len(s) < 2:
        return None
    t = s.index[-1] - pd.Timedelta(days=days)
    prev = s[s.index <= t]
    return None if prev.empty or prev.iloc[-1] == 0 else (s.iloc[-1] / prev.iloc[-1] - 1) * 100


def _f(x, nd=2, pct=False):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "   n/a"
    return f"{x:+.1f}%" if pct else f"{x:,.{nd}f}"


def section(title: str) -> str:
    return f"\n=== {title} " + "=" * max(3, 74 - len(title))


def markets_block() -> list[str]:
    rows = [("^GSPC", "S&P 500"), ("^NDX", "Nasdaq 100"), ("^VIX", "VIX"), ("^TNX", "US10y yld"), ("DX-Y.NYB", "DXY"),
            ("EURUSD=X", "EUR/USD"), ("USDJPY=X", "USD/JPY"), ("GC=F", "Gold"), ("SI=F", "Silver"), ("CL=F", "WTI"),
            ("BZ=F", "Brent"), ("HG=F", "Copper"), ("HYG", "HY credit"), ("TLT", "20y+ Tsy"), ("IBIT", "IBIT"),
            ("MSTR", "Strategy"), ("COIN", "Coinbase")]
    out = [f"{'asset':14s}{'last':>12s}{'1d':>8s}{'7d':>8s}{'30d':>8s}  asof"]
    for sym, name in rows:
        s = _ser(p("markets", "daily", f"{safe(sym)}.parquet"))
        if s is None:
            continue
        out.append(f"{name:14s}{_f(s.iloc[-1]):>12s}{_f(_chg(s,1),pct=True):>8s}{_f(_chg(s,7),pct=True):>8s}{_f(_chg(s,30),pct=True):>8s}  {s.index[-1]:%m-%d}")
    for sym, name in (("BTCUSDT", "Bitcoin"), ("ETHUSDT", "Ethereum"), ("SOLUSDT", "Solana")):
        s = _ser(p("crypto", "spot", "1d", f"{sym}.parquet"), "close", "time")
        if s is not None:
            out.append(f"{name:14s}{_f(s.iloc[-1]):>12s}{_f(_chg(s,1),pct=True):>8s}{_f(_chg(s,7),pct=True):>8s}{_f(_chg(s,30),pct=True):>8s}  {s.index[-1]:%m-%d}")
    return out if len(out) > 1 else ["(no market data yet - run markets_macro / crypto_spot)"]


def crypto_block() -> list[str]:
    out = []
    fg = read(p("sentiment", "fear_greed.parquet"))
    if fg is not None and len(fg):
        v = fg["fear_greed"]
        ago = lambda n: str(int(v.iloc[-1 - n])) if len(v) > n else "n/a"
        out.append(f"Fear & Greed: {int(v.iloc[-1])} ({fg['label'].iloc[-1]}), 7d ago {ago(7)}, 30d ago {ago(30)}")
    gl = sorted((DATA / "crypto" / "snapshots" / "coingecko_global").glob("*.parquet")) if (DATA / "crypto" / "snapshots" / "coingecko_global").exists() else []
    if gl:
        g = pd.read_parquet(gl[-1]).iloc[-1]
        out.append(f"Total crypto mcap ${g['total_mcap_usd']/1e12:.2f}T, 24h {g['mcap_change_24h_pct']:+.1f}%, BTC dominance {g.get('dominance_btc', float('nan')):.1f}%, ETH {g.get('dominance_eth', float('nan')):.1f}%")
    bt = sorted((DATA / "crypto" / "snapshots" / "bybit_tickers").glob("*.parquet")) if (DATA / "crypto" / "snapshots" / "bybit_tickers").exists() else []
    if bt:
        t = pd.read_parquet(bt[-1])
        t = t[t["snapshot_time"] == t["snapshot_time"].max()]
        top = t.sort_values("openInterestValue", ascending=False).head(6)
        out.append("perp funding / OI (Bybit, top by OI): " + "; ".join(f"{r['symbol'][:-4]} {r['fundingRate']*100:+.3f}% OI ${r['openInterestValue']/1e9:.2f}B" for _, r in top.iterrows()))
    return out or ["(no crypto sentiment data yet)"]


def freshness_block() -> list[str]:
    st = status_load()
    out = []
    for n, s in sorted(st.items()):
        fin = s.get("finished") or s.get("started") or ""
        out.append(f"  {n:20s} {s.get('state','?'):8s} {str(fin)[:16]}  {s.get('error') or ''}")
    return out or ["(nothing has been run yet)"]


def render() -> str:
    parts = [f"TRADING DATA HUB - snapshot   {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC"]
    for title, fn in (("PRICES", markets_block),
                      ("CRYPTO", crypto_block),
                      ("COLLECTOR STATUS", freshness_block)):
        parts.append(section(title))
        try:
            parts.extend(fn())
        except Exception as e:
            parts.append(f"(section failed: {type(e).__name__}: {e})")
    return "\n".join(parts)
