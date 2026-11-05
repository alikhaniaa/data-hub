"""Sentiment & crypto news: Fear & Greed index, CoinGecko trending, crypto news RSS (headline text for NLP features)."""
from __future__ import annotations

import pandas as pd

from ..collector import Collector, Ctx, register
from ..newsutil import ingest_feeds
from ..store import append_daily, now_utc, p, upsert, ts_from_s

CRYPTO_FEEDS = [
    ("coindesk", "https://www.coindesk.com/arc/outboundfeeds/rss/", "en"),
    ("cointelegraph", "https://cointelegraph.com/rss", "en"),
    ("theblock", "https://www.theblock.co/rss.xml", "en"),
    ("decrypt", "https://decrypt.co/feed", "en"),
    ("bitcoinmagazine", "https://bitcoinmagazine.com/.rss/full/", "en"),
    ("cryptoslate", "https://cryptoslate.com/feed/", "en"),
    ("beincrypto", "https://beincrypto.com/feed/", "en"),
    ("newsbtc", "https://www.newsbtc.com/feed/", "en"),
    ("utoday", "https://u.today/rss", "en"),
    ("cryptopotato", "https://cryptopotato.com/feed/", "en"),
    ("ambcrypto", "https://ambcrypto.com/feed/", "en"),
    ("dailyhodl", "https://dailyhodl.com/feed/", "en"),
]
CRYPTO_GNEWS = ["bitcoin", "ethereum", "crypto regulation SEC", "stablecoin", "bitcoin ETF flows", "crypto exchange hack", "altcoin"]


@register
class Sentiment(Collector):
    name = "crypto_sentiment"
    group = "crypto"
    title = "Sentiment & crypto news"
    description = "Fear & Greed index (full history), CoinGecko trending coins, headlines+summaries from 12 crypto outlets and Google News topic feeds."
    first_run = "1 min"

    def run(self, ctx: Ctx) -> dict:
        http = ctx.http
        n: dict = {}
        try:
            d = http.json("https://api.alternative.me/fng/", params={"limit": 0, "format": "json"}, timeout=60)["data"]
            df = pd.DataFrame(d)
            out = pd.DataFrame({"time": ts_from_s(df["timestamp"].astype("int64").values), "fear_greed": pd.to_numeric(df["value"]).values, "label": df["value_classification"].values})
            upsert(p("sentiment", "fear_greed.parquet"), out, ["time"], "time")
            n["fear_greed_days"] = len(out)
        except Exception as e:
            ctx.info(f"fear&greed: {type(e).__name__} {str(e)[:60]}")
        try:
            key = ctx.cfg["crypto"].get("coingecko_key") or ""
            http.throttle("coingecko", 2.2 if key else 6.5)
            tr = http.json("https://api.coingecko.com/api/v3/search/trending", headers={"x-cg-demo-api-key": key} if key else {})
            rows = []
            t = pd.Timestamp(now_utc()).as_unit("ms")
            for i, c in enumerate(tr.get("coins", []), 1):
                it = c["item"]
                rows.append({"snapshot_time": t, "rank": i, "id": it["id"], "symbol": it["symbol"], "name": it["name"], "mcap_rank": it.get("market_cap_rank"),
                             "score": it.get("score")})
            if rows:
                append_daily("coingecko_trending", pd.DataFrame(rows), root="crypto/snapshots")
        except Exception as e:
            ctx.info(f"trending: {type(e).__name__} {str(e)[:60]}")
        feeds = [(n_, u, lang) for n_, u, lang in CRYPTO_FEEDS]
        for q in CRYPTO_GNEWS:
            feeds.append((f"gnews:{q}", f"https://news.google.com/rss/search?q={q.replace(' ', '+')}+when:3d&hl=en-US&gl=US&ceid=US:en", "en"))
        added = ingest_feeds(ctx, feeds, domain="crypto")
        n["news_new"] = added
        return n
