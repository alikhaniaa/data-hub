"""`python -m tdata doctor` - tests every data host from THIS machine/network (geo-blocks and VPN changes show up here)."""
from __future__ import annotations

import concurrent.futures as cf
import time

import requests

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) trading-data-hub/1.0"}
# (name, method, url, json body, which collector needs it)
CHECKS = [
    ("Binance spot (data-api)", "GET", "https://data-api.binance.vision/api/v3/ping", None, "crypto_spot"),
    ("Binance bulk archive", "GET", "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2026-10-01.zip", None, "crypto_spot (1m/5m/15m)"),
    ("Binance futures (optional)", "GET", "https://fapi.binance.com/fapi/v1/ping", None, "not used (often geo-blocked)"),
    ("Bybit", "GET", "https://api.bybit.com/v5/market/time", None, "crypto_perps, crypto_snapshots"),
    ("OKX", "GET", "https://www.okx.com/api/v5/public/time", None, "crypto_universe"),
    ("Hyperliquid", "POST", "https://api.hyperliquid.xyz/info", {"type": "meta"}, "crypto_hyperliquid"),
    ("Deribit", "GET", "https://www.deribit.com/api/v2/public/get_time", None, "crypto_options"),
    ("CoinGecko", "GET", "https://api.coingecko.com/api/v3/ping", None, "crypto_universe, crypto_sentiment"),
    ("DefiLlama", "GET", "https://api.llama.fi/v2/chains", None, "crypto_defi"),
    ("CoinMetrics community", "GET", "https://community-api.coinmetrics.io/v4/catalog/assets", None, "crypto_onchain"),
    ("mempool.space", "GET", "https://mempool.space/api/blocks/tip/height", None, "crypto_onchain"),
    ("GeckoTerminal", "GET", "https://api.geckoterminal.com/api/v2/networks", None, "crypto_dex"),
    ("DexScreener", "GET", "https://api.dexscreener.com/token-profiles/latest/v1", None, "crypto_dex"),
    ("Alternative.me (Fear&Greed)", "GET", "https://api.alternative.me/fng/?limit=1", None, "crypto_sentiment"),
    ("Yahoo Finance", "GET", "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F?range=5d&interval=1d", None, "markets_macro"),
    ("US Treasury", "GET", "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/debt_to_penny?page[size]=1", None, "macro_official"),
    ("NY Fed", "GET", "https://markets.newyorkfed.org/api/rates/secured/sofr/last/1.json", None, "macro_official"),
    ("FRED (optional)", "GET", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10", None, "macro_fred (often unreachable)"),
    ("Google News RSS", "GET", "https://news.google.com/rss/search?q=bitcoin", None, "crypto_sentiment"),
]


def _one(c):
    name, method, url, body, need = c
    t = time.time()
    try:
        r = requests.post(url, json=body, headers=H, timeout=20) if method == "POST" else requests.get(url, headers=H, timeout=20, stream=True)
        if method == "GET":
            r.raw.read(512)
        ok = r.status_code < 400
        return name, ok, f"HTTP {r.status_code}", time.time() - t, need
    except Exception as e:
        return name, False, type(e).__name__, time.time() - t, need


def run() -> int:
    with cf.ThreadPoolExecutor(10) as ex:
        res = list(ex.map(_one, CHECKS))
    print(f"{'source':30s} {'status':14s} {'time':>6s}  used by")
    bad = 0
    for name, ok, st, dt, need in res:
        print(f"{name:30s} {('OK ' if ok else 'FAIL ') + st:14s} {dt:5.1f}s  {need}")
        bad += (not ok) and "optional" not in name and "not used" not in need
    print(f"\n{len(res) - bad}/{len(res)} reachable (optional ones excluded from the failure count). "
          "A FAIL usually means a geo-block / VPN change / site outage; the matching collector just skips that part.")
    return 1 if bad else 0
