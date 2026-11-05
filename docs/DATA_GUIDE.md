# Data guide

What each collector writes, how to load it, and the pitfalls to keep in mind
when using it for modelling. The exact schemas of the files on your disk are
listed in `docs/DATA_CATALOG.md`, which `python -m tdata catalog` generates.

## What is collected

| Layer | Collector | Files (under `data/`) | Notes |
|---|---|---|---|
| Coin universe and reference | `crypto_universe` | `crypto/meta/*.parquet`, `crypto/universe/coingecko_markets/YYYYMMDD.parquet`, `crypto/snapshots/coingecko_{categories,global}` | Binance, Bybit, OKX and Hyperliquid symbol lists; a daily CoinGecko top-N snapshot (price, market cap, supply, returns); `symbol_map.parquet` links Binance pairs, CoinGecko ids and perp availability. |
| Spot OHLCV | `crypto_spot` | `crypto/spot/{1d,1h,15m,5m,1m}/SYMBOL.parquet` | Binance USDT pairs, including delisted ones. `time` is the candle open (UTC). 1d and 1h for every pair; 15m/5m/1m only for the most liquid pairs from a start date (set in `config/config.json`). |
| Perpetual futures | `crypto_perps` | `crypto/perps/bybit/{1d,1h}/`, `funding/`, `open_interest_1h/`, `long_short_ratio_1h/` | Bybit USDT perps: candles, funding, open interest and long/short ratio for the top N by turnover. |
| Hyperliquid | `crypto_hyperliquid` | `crypto/hyperliquid/{1d,1h,funding}/` | Most active perps plus the `xyz:` commodity, index and FX perps listed in the config. |
| Options / implied vol | `crypto_options` | `crypto/options/dvol_*`, `crypto/options/snapshots/*` | Deribit DVOL (BTC, ETH) and option-chain / futures-curve snapshots. |
| Live snapshots | `crypto_snapshots` | `crypto/snapshots/{bybit_tickers,binance_spot_tickers,hyperliquid_ctx,binance_depth_profile}/YYYYMMDD.parquet` | Tickers, funding/OI and order-book depth. There is no free history for these, so they only accumulate while the collector is run regularly. |
| DEX long tail | `crypto_dex` | `crypto/dex/pools/YYYYMMDD.parquet`, `crypto/dex/ohlcv_{day,hour}/NETWORK__POOL.parquet`, `crypto/dex/dexscreener_*/` | GeckoTerminal new/trending/top-volume pools on up to 10 chains, candles for the 25 largest liquid pools, DexScreener token lists. |
| DeFi / stablecoins | `crypto_defi` | `defi/...` | DefiLlama TVL, stablecoin supply, DEX volume, fees, yield pools, hacks. |
| On-chain | `crypto_onchain` | `onchain/coinmetrics/ASSET.parquet`, `onchain/*` | CoinMetrics community metrics and Bitcoin network data from mempool.space. |
| Sentiment and news | `crypto_sentiment` | `sentiment/fear_greed.parquet`, `news/articles_crypto.parquet`, `crypto/snapshots/coingecko_trending` | Fear & Greed index, CoinGecko trending, headlines and summaries from crypto RSS feeds and Google News topic feeds. |
| Macro / cross-asset | `markets_macro`, `macro_official`, `macro_fred` | `markets/{daily,hourly}/SYMBOL.parquet`, `macro/official/*.parquet`, `macro/fred/*.parquet` | Yahoo Finance prices for the basket in `src/tdata/basket.py`; US Treasury, NY Fed, BLS and World Bank series; FRED (off by default because it is unreachable from some networks). |
| Panels | `build_views` | `views/*.parquet` | `crypto_daily_panel` (long format), `derivs_daily_panel`, `markets_daily_panel`, wide close matrices and `joint_daily_close` (crypto majors plus every Yahoo symbol, aligned by date). |

## Loading

```python
import pandas as pd
D = "data"  # relative to the project root
btc   = pd.read_parquet(f"{D}/crypto/spot/1h/BTCUSDT.parquet")        # time is tz-aware UTC
panel = pd.read_parquet(f"{D}/views/crypto_daily_panel.parquet")      # symbol, date, ohlcv, ret, taker_buy_ratio
fund  = pd.read_parquet(f"{D}/crypto/perps/bybit/funding/BTCUSDT.parquet")
```

## Pitfalls

1. **Survivorship bias.** Spot history keeps delisted pairs, but CoinGecko
   market caps are a snapshot of the day the collector ran. Build a tradable
   universe from past volume ranks, not from today's top list.
2. **Look-ahead.** `time` is the candle open; a bar is only known at
   `time + interval`. Yahoo daily bars carry the exchange-local `date`, so US
   closes land after crypto's UTC midnight; lag equity features by a day when
   joining.
3. **Venue differences.** Spot comes from Binance, derivatives from Bybit and
   Hyperliquid. Funding intervals differ per symbol.
4. **Renames and multipliers** (`1000PEPEUSDT`, MATIC to POL, reused tickers):
   check `symbol_map` and price discontinuities.
5. **Stablecoins, pegged and leveraged tokens** appear as ordinary USDT pairs.
6. **Illiquid pairs** have tiny volume and wide spreads; filter on volume.
7. **Gaps** from exchange maintenance and delistings: detect with
   `time.diff() > interval` and mask instead of forward-filling.
8. **Costs**: fees, funding and slippage matter; a model with free trading will
   exploit it.
9. **Timezones**: everything stored is UTC except the Yahoo `date` column.
10. **Regime shifts**: use walk-forward splits, not random ones.

## Not available from free sources

Liquidation history, L2/L3 order-book history, exchange flows and entity
labels, token-unlock calendars, social metrics, point-in-time market caps and
a full options history all require paid data providers.

An optional CoinGecko key can be set as `crypto.coingecko_key` in
`config/config.json`; it only raises the rate limit.
