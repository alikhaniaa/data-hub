"""Offline tests: storage layer, locks, runner, parsers, snapshot and CLI.

No network is used (HTTP calls are made to fail) and all files go to a temporary
data root. Run with `python tests/test_offline.py`; the test functions are
written in pytest style, so `python -m pytest tests` also collects them.
"""
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(tempfile.mkdtemp(prefix="tdata_test_"))
(ROOT / "config").mkdir()
(ROOT / "config" / "config.json").write_text('{"crypto": {"perp_top_n": 123}}', encoding="utf-8")
os.environ["TDATA_ROOT"] = str(ROOT)  # must be set before tdata is imported
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd  # noqa: E402
import requests  # noqa: E402


def _no_network(*args, **kwargs):
    raise requests.ConnectionError("network disabled in tests")


requests.Session.request = _no_network

from tdata import basket, briefing, catalog, collector, config, markets_common, newsutil, store, yahoo  # noqa: E402
from tdata.__main__ import main  # noqa: E402

collector.load_all()


def test_registry():
    reg = collector.REGISTRY
    real = {n: c for n, c in reg.items() if not n.startswith("t_")}
    assert len(real) == 14
    for c in real.values():
        assert all(d in reg for d in c.needs)
    order = collector.order(list(real))
    assert order.index("crypto_universe") < order.index("crypto_hyperliquid")
    assert {c.group for c in real.values()} == {"crypto", "macro"}


def test_config_merge():
    cfg = config.load_config()
    assert cfg["crypto"]["perp_top_n"] == 123  # from the file
    assert cfg["http"]["retries"] == 4  # from the defaults


def test_upsert_and_watermark():
    path = store.p("t", "upsert.parquet")
    a = pd.DataFrame({"time": store.ts_from_ms([1000, 2000, 3000]), "v": [1.0, 2.0, 3.0]})
    assert store.upsert(path, a, ["time"], "time") == (3, 3)
    b = pd.DataFrame({"time": store.ts_from_ms([3000, 4000]), "v": [30.0, 4.0], "extra": ["x", "y"]})
    assert store.upsert(path, b, ["time"], "time") == (1, 4)
    df = store.read(path)
    assert df.loc[df["time"] == store.ts_from_ms([3000])[0], "v"].iloc[0] == 30.0  # newer row wins
    assert "extra" in df.columns  # schema grows
    assert store.last_value(path, "time") == store.ts_from_ms([4000])[0]
    assert not list(path.parent.glob("*.tmp"))  # atomic write leaves no temp files


def test_mixed_type_column_written_as_text():
    path = store.p("t", "mixed.parquet")
    store.write(pd.DataFrame({"id": [1, "a"], "v": [1, 2]}), path)
    assert store.read(path)["id"].tolist() == ["1", "a"]


def test_lock_files():
    lock = collector._lock("t_lock")
    assert lock is not None and lock.exists()
    assert collector._lock("t_lock") is None  # refused while held
    old = time.time() - 5 * 3600
    os.utime(lock, (old, old))
    again = collector._lock("t_lock")  # stale lock (> 4 h) is taken over
    assert again is not None
    again.unlink()


calls = []


@collector.register
class _First(collector.Collector):
    name = "t_first"

    def run(self, ctx):
        time.sleep(0.2)
        calls.append("first")
        return {"rows": 1}


@collector.register
class _Second(collector.Collector):
    name = "t_second"
    needs = ["t_first"]

    def run(self, ctx):
        calls.append("second")
        raise ValueError("boom")


def test_runner_dependencies_failures_and_busy():
    calls.clear()
    res = collector.run_many(["t_second", "t_first"], log=lambda m: None, parallel=4)
    assert calls == ["first", "second"]
    assert [r["ok"] for r in res] == [True, False] and "boom" in res[1]["error"]
    st = store.status_load()
    assert st["t_first"]["state"] == "ok" and st["t_first"]["rows"] == 1
    assert st["t_second"]["state"] == "error"
    held = collector._lock("t_first")
    assert collector.run_one("t_first", log=lambda m: None).get("skipped") == "busy"
    held.unlink()


def test_yahoo_daily_bar_replaced_on_same_date():
    """A still-forming daily bar and the final bar of the same date must end up as one row."""
    frames = [
        pd.DataFrame({"time": store.ts_from_s([1762171200, 1762257600]), "date": pd.to_datetime(["2025-11-03", "2025-11-04"]).astype("datetime64[ms]"),
                      "open": [1.0, 2.0], "high": [1.0, 2.0], "low": [1.0, 2.0], "close": [1.0, 2.5], "adj_close": [1.0, 2.5], "volume": [10.0, 5.0]}),
        pd.DataFrame({"time": store.ts_from_s([1762257600 + 6 * 3600]), "date": pd.to_datetime(["2025-11-04"]).astype("datetime64[ms]"),
                      "open": [2.0], "high": [3.0], "low": [2.0], "close": [2.9], "adj_close": [2.9], "volume": [20.0]}),
    ]
    original = yahoo.fetch
    yahoo.fetch = lambda *a, **k: (frames.pop(0), {"symbol": "TEST"})
    try:
        ctx = collector.make_ctx("t_yahoo", log=lambda m: None)
        markets_common.sync_symbol(ctx, "TEST", hourly=False)
        markets_common.sync_symbol(ctx, "TEST", hourly=False)
    finally:
        yahoo.fetch = original
    df = store.read(store.p("markets", "daily", "TEST.parquet"))
    assert len(df) == 2
    assert df["date"].is_unique
    assert df.loc[df["date"] == pd.Timestamp("2025-11-04"), "close"].iloc[0] == 2.9


def test_yahoo_range_selection():
    assert markets_common._range_for_age(2, True) == "5d"
    assert markets_common._range_for_age(800, False) == "max"


def test_feed_parsing():
    rss = (b'<?xml version="1.0"?><rss><channel><item><title>A &amp; B</title><link>http://x/1</link>'
           b'<description>&lt;p&gt;hello&lt;/p&gt;</description><pubDate>Tue, 03 Nov 2026 10:00:00 GMT</pubDate></item></channel></rss>')
    items = newsutil.parse_feed(rss)
    assert len(items) == 1 and items[0]["title"] == "A & B" and items[0]["summary"] == "hello"
    assert str(items[0]["published"]).startswith("2026-11-03 10:00")
    atom = (b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>T</title><link rel="alternate" href="http://y"/>'
            b'<updated>2026-11-01T00:00:00Z</updated></entry></feed>')
    assert newsutil.parse_feed(atom)[0]["url"] == "http://y"


def test_basket_rows():
    assert all(len(r) == 4 for r in basket.MACRO)
    assert set(basket.meta_rows()[0]) == {"basket", "symbol", "name", "group", "note"}


def test_snapshot_and_catalog_from_stored_data():
    daily = pd.DataFrame({"time": store.ts_from_s([1730000000 + 86400 * i for i in range(40)]),
                          "date": pd.date_range("2024-10-27", periods=40, freq="D").astype("datetime64[ms]"),
                          "close": [100.0 + i for i in range(40)]})
    store.write(daily, store.p("markets", "daily", f"{markets_common.safe('^GSPC')}.parquet"))
    fg = pd.DataFrame({"time": store.ts_from_s([1730000000, 1730086400]), "fear_greed": [40, 45], "label": ["Fear", "Fear"]})
    store.write(fg, store.p("sentiment", "fear_greed.parquet"))  # shorter than 7 days of history
    txt = briefing.render()
    assert "S&P 500" in txt and "Fear & Greed: 45" in txt and "section failed" not in txt
    out = Path(catalog.write_catalog())
    assert "markets/daily" in out.read_text(encoding="utf-8")


def test_cli():
    assert main(["list"]) == 0
    assert main(["status"]) == 0
    assert main(["run", "no_such_collector"]) == 2


if __name__ == "__main__":
    failed = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except Exception as e:  # report every failure, then exit non-zero
                failed += 1
                print("FAIL", name, type(e).__name__, e)
    sys.exit(1 if failed else 0)
