"""Generates docs/DATA_CATALOG.md from what is actually on disk (schemas, rows, time ranges, sizes)."""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow.parquet as pq

from .paths import DATA, ROOT
from .store import human

TIME_COLS = ("time", "date", "snapshot_time", "published", "post_date", "fetched_at")


def _stats(files):
    rows = 0
    size = 0
    tmin = tmax = None
    tcol = None
    schema = None
    for f in files:
        try:
            pf = pq.ParquetFile(f)
        except Exception:
            continue
        md = pf.metadata
        rows += md.num_rows
        size += f.stat().st_size
        if schema is None:
            schema = pf.schema_arrow
        names = [md.schema.column(i).name for i in range(md.num_columns)]
        for tc in TIME_COLS:
            if tc in names:
                i = names.index(tc)
                for rg in range(md.num_row_groups):
                    st = md.row_group(rg).column(i).statistics
                    if st is not None and st.has_min_max:
                        tcol = tc
                        tmin = st.min if tmin is None or st.min < tmin else tmin
                        tmax = st.max if tmax is None or st.max > tmax else tmax
                break
    return rows, size, tcol, tmin, tmax, schema


def build() -> str:
    groups: dict = {}
    for f in sorted(DATA.rglob("*.parquet")):
        if "_meta" in f.parts:
            continue
        groups.setdefault(f.parent, []).append(f)
    lines = [f"# Data catalog (generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC)", "",
             f"Root: `{DATA}`. All timestamps are UTC unless a column is named `date` (exchange-local calendar date).", "",
             "| dataset (folder) | files | rows | size | time column | from | to |", "|---|---:|---:|---:|---|---|---|"]
    detail = []
    total = 0
    for folder, files in groups.items():
        rel = folder.relative_to(DATA).as_posix()
        rows, size, tcol, tmin, tmax, schema = _stats(files)
        total += size
        lines.append(f"| `{rel}` | {len(files)} | {rows:,} | {human(size)} | {tcol or ''} | {str(tmin)[:16] if tmin is not None else ''} | {str(tmax)[:16] if tmax is not None else ''} |")
        if schema is not None:
            cols = ", ".join(f"`{n}` {str(t).replace('timestamp[ms, tz=UTC]', 'ts')}" for n, t in zip(schema.names, schema.types))
            detail.append(f"### `{rel}`\n{len(files)} file(s), e.g. `{files[0].name}`\n\nColumns: {cols}\n")
    lines.append(f"\n**Total on disk: {human(total)}**\n")
    lines.append("## Schemas\n")
    lines.extend(detail)
    return "\n".join(lines)


def write_catalog() -> str:
    txt = build()
    out = ROOT / "docs" / "DATA_CATALOG.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text(txt, encoding="utf-8")
    return str(out)
