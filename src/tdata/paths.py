"""Project paths. The project root is the folder that contains config/config.json."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def _find_root() -> Path:
    env = os.environ.get("TDATA_ROOT")
    if env:
        return Path(env).resolve()
    if getattr(sys, "frozen", False):
        start = Path(sys.executable).resolve().parent
    else:
        start = Path(__file__).resolve().parent
    for p in [start, *start.parents]:
        if (p / "config" / "config.json").exists():
            return p
    return Path.cwd()


ROOT = _find_root()
DATA = ROOT / "data"
LOGS = ROOT / "logs"
CONFIG_FILE = ROOT / "config" / "config.json"
META = DATA / "_meta"

for _p in (DATA, LOGS, META):
    _p.mkdir(parents=True, exist_ok=True)
