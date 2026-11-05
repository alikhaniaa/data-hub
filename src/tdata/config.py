"""Configuration: defaults live here, config/config.json overrides them (deep merge)."""
from __future__ import annotations

import copy
import json

from .paths import CONFIG_FILE

DEFAULTS: dict = {
    "http": {"timeout": 30, "retries": 4, "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) trading-data-hub/1.0"},
    "workers": 12,
    "crypto": {
        # --- spot OHLCV (Binance public market data) ---
        "spot_quote": "USDT",
        "spot_intervals_all": ["1d", "1h"],  # every symbol, full history
        # extra high-resolution tiers: only for the N most liquid symbols, from `since`
        "spot_intervals_top": {
            "15m": {"top_n": 30, "since": "2023-01-01"},
            "5m": {"top_n": 10, "since": "2024-06-01"},
            "1m": {"top_n": 5, "since": "2025-06-01"},
        },
        "include_delisted": True,  # pull history of delisted pairs too (avoids survivorship bias)
        "min_quote_volume_usd": 0,  # skip dead pairs with 24h volume below this (0 = keep all)
        # --- derivatives ---
        "perp_intervals": ["1d", "1h"],
        "perp_top_n": 400,  # Bybit USDT perps ranked by 24h turnover
        "perp_oi_top_n": 150,  # open interest / long-short history for the top N
        "hl_top_n": 300,  # Hyperliquid perps with candles + funding history
        "hl_xyz_commodities": ["BRENTOIL", "CL", "NATGAS", "TTF", "GOLD", "SILVER", "COPPER", "HO", "URANIUM",
                                  "ALUMINIUM", "PLATINUM", "PALLADIUM", "XLE", "CVX", "PLTR", "DXY", "VIX", "SP500",
                                  "EUR", "JPY", "WHEAT", "CORN", "TLT"],
        # --- universe / reference ---
        "coingecko_top": 1000,
        "coingecko_key": "",  # optional demo/pro key to speed things up
        "onchain_assets": ["btc", "eth", "sol", "bnb", "xrp", "ada", "doge", "trx", "ltc", "link", "avax", "dot",
                            "matic", "atom", "xlm", "bch", "etc", "uni", "aave", "shib", "ton", "near", "apt", "arb",
                            "op", "usdt", "usdc", "dai"],
    },
    "fred_series": ["DGS10", "DGS2", "T10Y2Y", "T10YIE", "FEDFUNDS", "SOFR", "WALCL", "RRPONTSYD", "M2SL", "CPIAUCSL",
                    "UNRATE", "BAMLH0A0HYM2", "DCOILBRENTEU", "DCOILWTICO", "DTWEXBGS", "VIXCLS"],
}


def _merge(base: dict, over: dict) -> dict:
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _merge(base[k], v)
        else:
            base[k] = v
    return base


def load_config() -> dict:
    cfg = copy.deepcopy(DEFAULTS)
    try:
        if CONFIG_FILE.exists():
            _merge(cfg, json.loads(CONFIG_FILE.read_text(encoding="utf-8")))
    except Exception as e:  # a broken user file must not break the app
        print(f"[config] could not read {CONFIG_FILE}: {e}")
    return cfg
