"""Command line: python -m tdata <command>

  list                      show all collectors
  run <name|group|all> ...  run collectors (names, or a group name, or all)
  gui                       open the Windows app
  briefing                  print a market + crypto snapshot from stored data (no network)
  status                    last run of every collector
  doctor                    test every data source from this network (finds geo-blocks)
  catalog                   write docs/DATA_CATALOG.md (schemas, rows, time ranges of everything on disk)
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
from datetime import datetime

from . import __version__


def _logger(path):
    lock = threading.Lock()
    f = open(path, "a", encoding="utf-8", buffering=1)

    def log(msg: str):
        line = f"{datetime.now():%H:%M:%S} {msg}"
        with lock:
            print(line, flush=True)
            f.write(line + "\n")

    return log


def main(argv=None):
    from .collector import REGISTRY, load_all, run_many
    from .paths import LOGS

    ap = argparse.ArgumentParser(prog="tdata", description="Trading Data Hub")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("list")
    sub.add_parser("gui")
    sub.add_parser("status")
    sub.add_parser("briefing")
    sub.add_parser("catalog")
    sub.add_parser("doctor")
    rp = sub.add_parser("run")
    rp.add_argument("names", nargs="+")
    rp.add_argument("--full", action="store_true", help="ignore watermarks and rebuild")
    rp.add_argument("--parallel", type=int, default=4)
    rp.add_argument("--skip", nargs="*", default=[])
    ns = ap.parse_args(argv)

    load_all()
    if ns.cmd in (None, "gui"):
        from .gui import main as gui_main
        return gui_main()
    if ns.cmd == "list":
        for c in REGISTRY.values():
            print(f"{c.name:22s} [{c.group:6s}] {c.title}  (first run: {c.first_run})")
        return 0
    if ns.cmd == "status":
        from .store import status_load
        for n, s in status_load().items():
            print(f"{n:22s} {s.get('state','?'):8s} {str(s.get('finished',''))[:19]}  {s.get('error') or ''}")
        return 0
    if ns.cmd == "doctor":
        from .doctor import run as doctor_run
        return doctor_run()
    if ns.cmd == "catalog":
        from .catalog import write_catalog
        print("written:", write_catalog())
        return 0
    if ns.cmd == "briefing":
        from .briefing import render
        print(render())
        return 0
    if ns.cmd == "run":
        names: list[str] = []
        for n in ns.names:
            if n == "all":
                names += [k for k, c in REGISTRY.items() if c.default]
            elif n in {c.group for c in REGISTRY.values()}:
                names += [k for k, c in REGISTRY.items() if c.group == n and c.default]
            elif n in REGISTRY:
                names.append(n)
            else:
                print(f"unknown collector: {n}\nknown: {', '.join(REGISTRY)}")
                return 2
        names = [n for n in dict.fromkeys(names) if n not in ns.skip]
        log = _logger(LOGS / f"run_{datetime.now():%Y%m%d}.log")
        t0 = time.time()
        res = run_many(names, log=log, full=ns.full, parallel=ns.parallel)
        bad = [r for r in res if not r.get("ok")]
        log(f"finished {len(res) - len(bad)}/{len(res)} ok in {time.time() - t0:.0f}s" + (f"; failed: {[b['name'] for b in bad]}" if bad else ""))
        return 1 if bad else 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
