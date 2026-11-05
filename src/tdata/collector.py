"""Collector framework: every data source is a Collector; the CLI and the GUI both drive them."""
from __future__ import annotations

import concurrent.futures as cf
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Callable

from .config import load_config
from .net import Http
from .store import status_update, now_utc


@dataclass
class Ctx:
    cfg: dict
    http: Http
    log: Callable[[str], None] = print
    stop: threading.Event = field(default_factory=threading.Event)
    full: bool = False  # ignore watermarks and rebuild from scratch
    name: str = ""
    _progress: Callable[[str, int, int, str], None] | None = None

    def progress(self, done: int, total: int, msg: str = ""):
        if self._progress:
            self._progress(self.name, done, total, msg)

    def info(self, msg: str):
        self.log(f"[{self.name}] {msg}")


class Collector:
    name = "base"
    group = "crypto"  # used by `run <group>` and the GUI filter
    title = ""
    description = ""
    first_run = "minutes"  # rough guide shown in the UI
    default = True  # included in "update everything"
    needs: list[str] = []  # names of collectors that should run before this one

    def run(self, ctx: Ctx) -> dict:
        raise NotImplementedError


REGISTRY: dict[str, Collector] = {}


def register(cls):
    inst = cls()
    REGISTRY[inst.name] = inst
    return cls


MODULES = [
    "crypto.universe", "crypto.spot", "crypto.perps", "crypto.hyperliquid", "crypto.snapshots", "crypto.defi",
    "crypto.onchain", "crypto.sentiment", "crypto.options", "crypto.dex", "crypto.macro", "crypto.official", "crypto.views",
]


def load_all():
    """Import every collector module so they register themselves."""
    import importlib
    for m in MODULES:
        full = f"{__package__}.{m}"
        try:
            importlib.import_module(full)
        except ModuleNotFoundError as e:
            if e.name != full:  # a missing third-party dependency is a real error
                print(f"[load] {full}: {e}")
    return REGISTRY


def make_ctx(name: str, log=print, stop=None, full=False, progress=None, cfg=None) -> Ctx:
    cfg = cfg or load_config()
    stop = stop or threading.Event()
    h = cfg["http"]
    http = Http(timeout=h["timeout"], retries=h["retries"], user_agent=h["user_agent"], stop=stop)
    return Ctx(cfg=cfg, http=http, log=log, stop=stop, full=full, name=name, _progress=progress)


def _lock(name: str):
    """Cross-process lock so the GUI and a scheduled task never run the same collector at once. Returns path or None if busy."""
    import os
    from .paths import META
    d = META / "locks"
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name}.lock"
    for _ in range(2):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"{os.getpid()} {time.time()}".encode())
            os.close(fd)
            return path
        except FileExistsError:
            try:
                if time.time() - path.stat().st_mtime > 4 * 3600:  # stale lock from a crashed run
                    path.unlink()
                    continue
            except OSError:
                pass
            return None
    return None


def run_one(name: str, log=print, stop=None, full=False, progress=None, cfg=None) -> dict:
    col = REGISTRY[name]
    ctx = make_ctx(name, log, stop, full, progress, cfg)
    lock = _lock(name)
    if lock is None:
        ctx.info("skipped: another process is already running this collector")
        return {"name": name, "ok": True, "skipped": "busy"}
    t0 = time.time()
    status_update(name, state="running", started=now_utc().isoformat(), error=None)
    ctx.info("start")
    try:
        stats = col.run(ctx) or {}
        dt = time.time() - t0
        status_update(name, state="ok", finished=now_utc().isoformat(), seconds=round(dt, 1), error=None, **{k: v for k, v in stats.items() if isinstance(v, (int, float, str))})
        ctx.info(f"done in {dt:.0f}s {stats}")
        return {"name": name, "ok": True, **stats}
    except InterruptedError:
        status_update(name, state="stopped", finished=now_utc().isoformat())
        ctx.info("stopped")
        return {"name": name, "ok": False, "error": "stopped"}
    except Exception as e:
        tb = traceback.format_exc()
        status_update(name, state="error", finished=now_utc().isoformat(), error=f"{type(e).__name__}: {e}")
        ctx.log(f"[{name}] FAILED: {type(e).__name__}: {e}\n{tb}")
        return {"name": name, "ok": False, "error": str(e)}
    finally:
        try:
            lock.unlink()
        except OSError:
            pass


def order(names: list[str]) -> list[str]:
    """Respect `needs` (simple topological order)."""
    seen, out = set(), []

    def visit(n):
        if n in seen or n not in REGISTRY:
            return
        seen.add(n)
        for d in REGISTRY[n].needs:
            if d in names:
                visit(d)
        out.append(n)

    for n in names:
        visit(n)
    return out


def run_many(names: list[str], log=print, stop=None, full=False, progress=None, parallel: int = 4, cfg=None) -> list[dict]:
    """Run collectors; those without dependencies in the selection run concurrently."""
    names = order(names)
    stop = stop or threading.Event()
    results: dict[str, dict] = {}
    done_evt = {n: threading.Event() for n in names}

    def job(n):
        for d in REGISTRY[n].needs:
            if d in done_evt:
                done_evt[d].wait()
        try:
            if not stop.is_set():
                results[n] = run_one(n, log, stop, full, progress, cfg)
        finally:
            done_evt[n].set()

    with cf.ThreadPoolExecutor(max_workers=max(1, parallel)) as ex:
        futs = [ex.submit(job, n) for n in names]
        for f in futs:
            f.result()
    return [results[n] for n in names if n in results]
