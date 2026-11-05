"""Parquet storage helpers (atomic writes, upserts, incremental watermarks) + run status registry."""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .paths import DATA, META

TS = "datetime64[ms, UTC]"


def ts_from_ms(s) -> pd.Series:
    """epoch milliseconds -> tz-aware UTC timestamps at ms resolution."""
    return pd.to_datetime(pd.Series(s).astype("int64"), unit="ms", utc=True).astype(TS)


def ts_from_s(s) -> pd.Series:
    return pd.to_datetime(pd.Series(s).astype("int64"), unit="s", utc=True).astype(TS)


def ts_parse(s) -> pd.Series:
    return pd.to_datetime(pd.Series(s), utc=True, errors="coerce", format="mixed").astype(TS)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def p(*parts) -> Path:
    path = DATA.joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def read(path: Path, columns=None) -> pd.DataFrame | None:
    if not Path(path).exists():
        return None
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception:
        return None


def write(df: pd.DataFrame, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        df.to_parquet(tmp, index=False, compression="zstd")
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # mixed-type object columns (e.g. ids that are sometimes int, sometimes str): store them as text
        df = df.copy()
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].map(lambda v: None if v is None or (isinstance(v, float) and v != v) else str(v))
        df.to_parquet(tmp, index=False, compression="zstd")
    os.replace(tmp, path)


def upsert(path: Path, new: pd.DataFrame, keys: list[str], sort_by: str | list[str] | None = None, keep: str = "last") -> tuple[int, int]:
    """Merge `new` into the parquet at `path` (new rows win on key collisions unless keep="first"). Returns (rows_added, total_rows)."""
    path = Path(path)
    if new is None or len(new) == 0:
        old = read(path)
        return 0, 0 if old is None else len(old)
    old = read(path)
    if old is None or len(old) == 0:
        df = new
        added = len(new.drop_duplicates(subset=keys))
    else:
        for c in new.columns:  # keep schema stable when new columns appear
            if c not in old.columns:
                old[c] = pd.NA
        df = pd.concat([old, new[[c for c in old.columns if c in new.columns] + [c for c in new.columns if c not in old.columns]]], ignore_index=True)
        before = len(old)
        df = df.drop_duplicates(subset=keys, keep=keep)
        added = len(df) - before
    if sort_by:
        df = df.sort_values(sort_by, kind="stable")
    df = df.reset_index(drop=True)
    write(df, path)
    return max(added, 0), len(df)


def last_value(path: Path, col: str):
    """Max of a column, reading only that column."""
    if not Path(path).exists():
        return None
    try:
        t = pq.read_table(path, columns=[col])
        if t.num_rows == 0:
            return None
        return pd.Series(t.column(col).to_pandas()).max()
    except Exception:
        return None


def append_daily(name: str, df: pd.DataFrame, root: str = "snapshots") -> Path:
    """Append rows to today's snapshot file (data/<root>/<name>/YYYYMMDD.parquet)."""
    day = now_utc().strftime("%Y%m%d")
    path = p(*root.split("/"), name, f"{day}.parquet")
    old = read(path)
    out = df if old is None else pd.concat([old, df], ignore_index=True)
    write(out, path)
    return path


# ---------------------------------------------------------------- status registry
_status_lock = threading.Lock()
STATUS_FILE = META / "status.json"


def status_load() -> dict:
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def status_update(name: str, **fields) -> None:
    with _status_lock:
        st = status_load()
        st.setdefault(name, {}).update(fields)
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATUS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, indent=1, default=str), encoding="utf-8")
        os.replace(tmp, STATUS_FILE)


def dir_stats(path: Path) -> tuple[int, int]:
    """(file_count, bytes) under a folder."""
    n = b = 0
    if path.exists():
        for f in path.rglob("*.parquet"):
            n += 1
            b += f.stat().st_size
    return n, b


def human(n: float) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"
