"""RSS/Atom ingestion into one deduplicated article table per domain (e.g. crypto)."""
from __future__ import annotations

import concurrent.futures as cf
import email.utils
import hashlib
import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pandas as pd

from .collector import Ctx
from .store import now_utc, p, upsert

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
ATOM = "{http://www.w3.org/2005/Atom}"
CONTENT = "{http://purl.org/rss/1.0/modules/content/}encoded"
DC = "{http://purl.org/dc/elements/1.1/}"


def clean(s: str | None, n: int = 1500) -> str:
    if not s:
        return ""
    s = html.unescape(_TAG.sub(" ", s))
    return _WS.sub(" ", s).strip()[:n]


def parse_date(s: str | None) -> pd.Timestamp | None:
    if not s:
        return None
    s = s.strip()
    try:
        dt = email.utils.parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return pd.Timestamp(dt).tz_convert("UTC")
    except Exception:
        pass
    try:
        return pd.Timestamp(s).tz_convert("UTC") if pd.Timestamp(s).tzinfo else pd.Timestamp(s, tz="UTC")
    except Exception:
        return None


def parse_feed(content: bytes) -> list[dict]:
    root = ET.fromstring(content)
    out = []
    items = root.findall(".//item")
    if items:
        for it in items:
            src = it.find("source")
            out.append({
                "title": clean(it.findtext("title"), 400),
                "summary": clean(it.findtext(CONTENT) if (it.findtext("description") or "") == "" else it.findtext("description")),
                "url": (it.findtext("link") or it.findtext("guid") or "").strip(),
                "published": parse_date(it.findtext("pubDate") or it.findtext(DC + "date")),
                "publisher": (src.text.strip() if src is not None and src.text else None),
            })
        return out
    for it in root.findall(f".//{ATOM}entry"):
        link = ""
        for l in it.findall(f"{ATOM}link"):
            if l.get("rel", "alternate") == "alternate":
                link = l.get("href", "")
                break
        out.append({
            "title": clean(it.findtext(f"{ATOM}title"), 400),
            "summary": clean(it.findtext(f"{ATOM}summary") or it.findtext(f"{ATOM}content")),
            "url": link,
            "published": parse_date(it.findtext(f"{ATOM}published") or it.findtext(f"{ATOM}updated")),
            "publisher": None,
        })
    return out


def ingest_feeds(ctx: Ctx, feeds: list[tuple[str, str, str]], domain: str, workers: int = 8) -> int:
    """feeds: [(source_name, url, lang)] -> returns number of new articles stored."""
    now = pd.Timestamp(now_utc()).as_unit("ms")
    rows: list[dict] = []
    ok = bad = 0

    def one(feed):
        name, url, lang = feed
        r = ctx.http.get(url, timeout=40)
        return feed, parse_feed(r.content)

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(one, f) for f in feeds]
        for f in cf.as_completed(futs):
            try:
                (name, url, lang), items = f.result()
            except InterruptedError:
                raise
            except Exception as e:
                bad += 1
                continue
            ok += 1
            for it in items:
                if not it["url"] and not it["title"]:
                    continue
                key = it["url"] or it["title"]
                rows.append({
                    "id": hashlib.sha1(key.encode("utf-8", "ignore")).hexdigest()[:20],
                    "domain": domain, "source": it["publisher"] or name.split(":")[0], "feed": name, "lang": lang,
                    "title": it["title"], "summary": it["summary"], "url": it["url"],
                    "published": it["published"] if it["published"] is not None else now, "fetched_at": now,
                })
    ctx.info(f"feeds ok={ok} failed={bad} items={len(rows)}")
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    df["published"] = pd.to_datetime(df["published"], utc=True).astype("datetime64[ms, UTC]")
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True).astype("datetime64[ms, UTC]")
    # keep the earliest fetch time for already-known articles
    path = p("news", f"articles_{domain}.parquet")
    added, total = upsert(path, df, keys=["id"], sort_by="published", keep="first")
    ctx.info(f"{domain}: +{added} new articles ({total} total)")
    return added
