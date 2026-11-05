# Trading Data Hub

A Python toolkit that collects public market data (crypto spot and
derivatives, DeFi, on-chain, sentiment, cross-asset prices and US macro
series) and stores it as Parquet files that are updated incrementally. It only
reads data; it does not place trades.

Each data source is a small collector class. A shared framework handles HTTP
retries and throttling, incremental "watermarks", atomic file writes,
per-collector lock files, run status, and dependency ordering. The same engine
is driven from a command line, a small Tkinter window, or Windows Task
Scheduler.

## Features

- 14 collectors, listed with `python -m tdata list`: a coin universe and
  symbol map (Binance, Bybit, OKX, Hyperliquid, CoinGecko), Binance spot OHLCV (including delisted pairs), Bybit perpetuals (candles,
  funding, open interest, long/short ratio), Hyperliquid perps, Deribit
  implied volatility and option snapshots, exchange ticker and order-book
  depth snapshots, DEX long-tail pools and candles (GeckoTerminal,
  DexScreener), DefiLlama, CoinMetrics and mempool.space, Fear & Greed and
  crypto news RSS, a Yahoo Finance cross-asset basket, US Treasury / NY Fed /
  BLS / World Bank series, FRED (optional), and a step that builds
  model-ready daily panels. They are grouped as `crypto` and `macro`, so
  `run crypto` or `run macro` runs one group.
- Incremental updates: each file's last timestamp is read back and only newer
  data is requested. `--full` rebuilds from scratch.
- Atomic writes (temp file + `os.replace`) and upserts keyed on timestamp (on date for daily
  Yahoo bars), so
  an interrupted run does not leave half-written files.
- A lock file per collector (`data/_meta/locks/`), so the GUI and a scheduled
  task never write the same dataset at once. Locks older than four hours are
  treated as stale.
- Collectors run in a thread pool; a collector that declares `needs` waits for
  its dependencies.
- `doctor` checks which data hosts are reachable from the current network
  (useful when a source is geo-blocked), and `catalog` writes
  `docs/DATA_CATALOG.md` with the schema, row count and time range of every
  dataset on disk.

See [docs/DATA_GUIDE.md](docs/DATA_GUIDE.md) for what each collector stores and
the usual pitfalls (survivorship bias, look-ahead, venue differences) when the
data is used for modelling.

## Setup

Checked with Python 3.12, pandas 3.1 and pyarrow 26. The lower bounds in
`requirements.txt` have not been tested. On Windows, `setup.bat` creates
`.venv` and installs the requirements. Elsewhere:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
python -m tdata list
```

The GUI needs Tkinter (`python3-tk` on Debian/Ubuntu).

## Usage

```sh
python -m tdata list                         # all collectors
python -m tdata run all --parallel 5         # every default collector
python -m tdata run crypto_spot crypto_perps # specific collectors
python -m tdata run crypto_spot --full       # rebuild from scratch
python -m tdata status                       # last run of each collector
python -m tdata doctor                       # which data hosts are reachable
python -m tdata catalog                      # write docs/DATA_CATALOG.md
python -m tdata briefing                     # text snapshot from stored data (no network)
python -m tdata gui                          # Tkinter window
```

Data goes to `data/` and logs to `logs/` under the project root (the folder
that contains `config/config.json`; override with the `TDATA_ROOT`
environment variable). What to collect (intervals, top-N limits, start
dates) is set in `config/config.json`, which is merged over the defaults in
`src/tdata/config.py`.

The first full run downloads a lot of history and takes a long time; later
runs only fetch what is new.

### Windows helpers

| File | Purpose |
|---|---|
| `setup.bat` | Create `.venv` and install requirements |
| `TradingDataHub.bat` | Open the GUI |
| `update_all.bat` | Run every default collector, then print the snapshot |
| `briefing.bat` | Print the snapshot from stored data |
| `schedule_tasks.bat` / `schedule_remove.bat` | Create / remove three Task Scheduler jobs (every 15 min, hourly, daily) that call `run_fast.bat`, `run_hourly.bat`, `run_daily.bat` |
| `build_exe.bat` | Optional PyInstaller build of the GUI (`app_entry.py`) |

## Project layout

```
src/tdata/
  __main__.py        command line
  collector.py       Collector base class, registry, locks, parallel runner
  store.py           Parquet read/write/upsert, watermarks, run status
  net.py             HTTP session with retries, backoff and per-host throttling
  config.py          defaults + config/config.json merge
  paths.py           project root and data/log folders
  yahoo.py           Yahoo Finance chart API client
  markets_common.py  incremental Yahoo sync for a basket
  basket.py          the cross-asset instrument list
  newsutil.py        RSS/Atom parsing and deduplicated article table
  briefing.py        text snapshot from stored data
  catalog.py         dataset catalogue generator
  doctor.py          reachability check of the data hosts
  gui.py             Tkinter front end
  crypto/            one module per data source, plus views.py (panels)
config/config.json   user settings
docs/DATA_GUIDE.md   what is stored and how to use it
tests/               offline tests
```

## Testing

```sh
python tests/test_offline.py
```

The tests run offline (HTTP calls are made to fail) against a temporary data
folder. They cover config merging, Parquet upserts and watermarks, atomic
writes, lock files, the parallel runner (dependency order, failure capture,
busy collectors), the daily Yahoo bar replacement, RSS/Atom parsing, the
snapshot and catalogue, and the CLI. They do not call the real data sources.
The collectors depend on free public APIs whose formats change without
notice; `python -m tdata doctor` shows which hosts are reachable and
`python -m tdata status` records failures.

## Limitations

- All sources are free public APIs. Some are unofficial (Yahoo Finance) or
  rate-limited, and any of them can change or disappear; a failing source is
  logged and the run continues. Collectors with several independent parts
  (for example `crypto_sentiment`) catch errors per part, so such a collector
  can finish with state `ok` even when every part failed; the log shows which
  requests failed.
- Some hosts block certain regions (for example Binance futures, which is why
  derivatives come from Bybit, Hyperliquid and Deribit). `doctor` shows what
  is reachable.
- Snapshot-type data (order-book depth, tickers, funding/OI snapshots) has no
  free history; it only exists from the time you start collecting it.
- The scheduling helpers and the GUI's folder buttons are Windows-specific.
  The command line works on any platform.

## Licence

MIT, see `LICENSE`.
