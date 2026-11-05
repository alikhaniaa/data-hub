"""Trading Data Hub - simple Windows app (tkinter). Pick collectors, press Update, watch progress, read the snapshot."""
from __future__ import annotations

import os
import queue
import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, scrolledtext, ttk

from .collector import REGISTRY, load_all, run_many
from .paths import DATA, LOGS
from .store import dir_stats, human, status_load


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Trading Data Hub")  # data folder is shown in the Log tab and status bar
        self.geometry("1180x760")
        self.minsize(980, 600)
        try:
            ttk.Style(self).theme_use("clam")
        except tk.TclError:
            pass
        self.q: queue.Queue = queue.Queue()
        self.stop_evt = threading.Event()
        self.running = False
        self.auto_job = None
        self.sel: dict[str, bool] = {n: c.default for n, c in REGISTRY.items()}
        self.prog: dict[str, str] = {}
        self._build()
        self.refresh_status()
        self.after(150, self._pump)
        self.protocol("WM_DELETE_WINDOW", self._close)
        self.log_line(f"Data folder: {DATA}")
        self.log_line(f"Loaded {len(REGISTRY)} collectors. Press 'Update ALL selected' to refresh data; the Snapshot tab reads whatever is stored.")
        self.nb.bind("<<NotebookTabChanged>>", self._tab_changed)
        self.after(400, self._load_async)

    # ----------------------------------------------------------------------------- layout
    def _build(self):
        top = ttk.Frame(self, padding=(10, 8))
        top.pack(fill="x")
        self.btn_all = ttk.Button(top, text="Update ALL selected", command=lambda: self.start(None))
        self.btn_all.pack(side="left")
        ttk.Button(top, text="Crypto only", command=lambda: self.start("crypto")).pack(side="left", padx=(6, 0))
        ttk.Button(top, text="Macro only", command=lambda: self.start("macro")).pack(side="left", padx=(6, 0))
        self.btn_stop = ttk.Button(top, text="Stop", command=self.stop, state="disabled")
        self.btn_stop.pack(side="left", padx=(12, 0))
        self.full = tk.BooleanVar(value=False)
        ttk.Checkbutton(top, text="Full rebuild (ignore existing data)", variable=self.full).pack(side="left", padx=(16, 0))
        ttk.Label(top, text="Parallel:").pack(side="left", padx=(16, 2))
        self.par = tk.IntVar(value=4)
        ttk.Spinbox(top, from_=1, to=8, width=3, textvariable=self.par).pack(side="left")

        auto = ttk.Frame(self, padding=(10, 0))
        auto.pack(fill="x")
        self.auto_on = tk.BooleanVar(value=False)
        ttk.Checkbutton(auto, text="Auto-update every", variable=self.auto_on, command=self._auto_toggle).pack(side="left")
        self.auto_min = tk.IntVar(value=30)
        ttk.Spinbox(auto, from_=5, to=720, increment=5, width=5, textvariable=self.auto_min).pack(side="left", padx=4)
        ttk.Label(auto, text="minutes (while this window is open; selected collectors only)").pack(side="left")
        ttk.Button(auto, text="Open data folder", command=lambda: os.startfile(DATA)).pack(side="right")
        ttk.Button(auto, text="Open logs", command=lambda: os.startfile(LOGS)).pack(side="right", padx=6)

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=10, pady=8)

        # collectors tab
        t1 = ttk.Frame(self.nb)
        self.nb.add(t1, text="Collectors")
        cols = ("sel", "name", "group", "title", "state", "finished", "secs", "info")
        self.tree = ttk.Treeview(t1, columns=cols, show="headings", selectmode="browse")
        widths = (40, 150, 55, 330, 70, 125, 55, 330)
        heads = ("Run", "Collector", "Group", "What it gathers", "State", "Last finished (UTC)", "Secs", "Progress / result")
        for c, w, h in zip(cols, widths, heads):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor="w" if c not in ("sel", "secs") else "center")
        self.tree.pack(fill="both", expand=True, side="left")
        sb = ttk.Scrollbar(t1, orient="vertical", command=self.tree.yview)
        sb.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=sb.set)
        self.tree.bind("<Double-1>", self._toggle)
        self.tree.bind("<<TreeviewSelect>>", self._describe)
        for n, c in REGISTRY.items():
            self.tree.insert("", "end", iid=n, values=("", n, c.group, c.title, "", "", "", ""))
        self.desc = tk.StringVar(value="Double-click a row to include/exclude it. Select a row to read its description.")
        ttk.Label(self, textvariable=self.desc, wraplength=1120, foreground="#444").pack(fill="x", padx=12)

        # snapshot tab
        t2 = ttk.Frame(self.nb)
        self.nb.add(t2, text="Snapshot")
        bar = ttk.Frame(t2)
        bar.pack(fill="x")
        ttk.Button(bar, text="Refresh snapshot", command=self.refresh_board).pack(side="left", pady=4)
        ttk.Label(bar, text="  (reads stored data only - update first)").pack(side="left")
        self.board = scrolledtext.ScrolledText(t2, font=("Consolas", 10), wrap="none")
        self.board.pack(fill="both", expand=True)

        # log tab
        t3 = ttk.Frame(self.nb)
        self.nb.add(t3, text="Log")
        self.log = scrolledtext.ScrolledText(t3, font=("Consolas", 9), wrap="word", state="disabled")
        self.log.pack(fill="both", expand=True)

        # data tab
        t4 = ttk.Frame(self.nb)
        self.nb.add(t4, text="Data on disk")
        ttk.Button(t4, text="Recalculate", command=self.refresh_disk).pack(anchor="w", pady=4)
        self.disk = scrolledtext.ScrolledText(t4, font=("Consolas", 10), wrap="none")
        self.disk.pack(fill="both", expand=True)

        bot = ttk.Frame(self, padding=(10, 0, 10, 8))
        bot.pack(fill="x")
        self.status = tk.StringVar(value=f"Idle.   Data folder: {DATA}")
        ttk.Label(bot, textvariable=self.status).pack(side="left")
        self.bar = ttk.Progressbar(bot, mode="indeterminate", length=220)
        self.bar.pack(side="right")
        self._redraw_sel()

    # ----------------------------------------------------------------------------- selection
    def _redraw_sel(self):
        for n in REGISTRY:
            self.tree.set(n, "sel", "[x]" if self.sel[n] else "[ ]")

    def _toggle(self, ev):
        iid = self.tree.identify_row(ev.y)
        if iid:
            self.sel[iid] = not self.sel[iid]
            self._redraw_sel()

    def _describe(self, _ev=None):
        s = self.tree.selection()
        if s:
            c = REGISTRY[s[0]]
            self.desc.set(f"{c.title}: {c.description}   [first run ~{c.first_run}]")

    # ----------------------------------------------------------------------------- running
    def log_line(self, msg: str):
        self.q.put(("log", msg))

    def progress_cb(self, name, done, total, msg):
        self.q.put(("prog", name, f"{done}/{total} {msg}" if total else msg))

    def start(self, group):
        if self.running:
            messagebox.showinfo("Busy", "An update is already running.")
            return
        names = [n for n, c in REGISTRY.items() if self.sel[n] and (group is None or c.group == group)]
        if not names:
            messagebox.showinfo("Nothing selected", "Tick at least one collector (double-click a row).")
            return
        self._run(names)

    def _run(self, names):
        self.running = True
        self.stop_evt = threading.Event()
        self.btn_stop.configure(state="normal")
        self.btn_all.configure(state="disabled")
        self.bar.start(12)
        self.status.set(f"Running {len(names)} collectors ...")
        full = self.full.get()
        par = max(1, int(self.par.get()))
        for n in names:
            self.tree.set(n, "state", "queued")

        def worker():
            t0 = time.time()
            try:
                res = run_many(names, log=self.log_line, stop=self.stop_evt, full=full, progress=self.progress_cb, parallel=par)
                bad = [r["name"] for r in res if not r.get("ok")]
                self.q.put(("done", f"Finished {len(res) - len(bad)}/{len(res)} in {time.time() - t0:.0f}s" + (f" - failed: {', '.join(bad)}" if bad else "")))
            except Exception as e:
                self.q.put(("done", f"Run crashed: {e}"))

        threading.Thread(target=worker, daemon=True).start()

    def stop(self):
        self.stop_evt.set()
        self.status.set("Stopping ...")

    def _pump(self):
        try:
            while True:
                item = self.q.get_nowait()
                kind = item[0]
                if kind == "log":
                    line = f"{datetime.now():%H:%M:%S} {item[1]}\n"
                    self.log.configure(state="normal")
                    self.log.insert("end", line)
                    self.log.see("end")
                    self.log.configure(state="disabled")
                    self._try_state(item[1])
                elif kind == "prog":
                    self.tree.set(item[1], "info", item[2])
                elif kind == "board":
                    self.board.delete("1.0", "end")
                    self.board.insert("end", item[1])
                elif kind == "disk":
                    self.disk.delete("1.0", "end")
                    self.disk.insert("end", item[1])
                elif kind == "done":
                    self.running = False
                    self.bar.stop()
                    self.btn_all.configure(state="normal")
                    self.btn_stop.configure(state="disabled")
                    self.status.set(item[1] + f"   ({datetime.now():%H:%M:%S})")
                    self.refresh_status()
                    self._load_async()  # board + disk usage, off the UI thread
        except queue.Empty:
            pass
        self.after(200, self._pump)

    def _try_state(self, msg: str):
        # "[name] start" / "[name] done in ..." / "[name] FAILED"
        if msg.startswith("[") and "]" in msg:
            name, rest = msg[1:].split("]", 1)
            if name in REGISTRY:
                if rest.strip() == "start":
                    self.tree.set(name, "state", "running")
                elif rest.strip().startswith("done"):
                    self.tree.set(name, "state", "ok")
                    self.tree.set(name, "info", rest.strip()[:140])
                elif "FAILED" in rest:
                    self.tree.set(name, "state", "ERROR")
                    self.tree.set(name, "info", rest.strip()[:140])

    def refresh_status(self):
        st = status_load()
        for n in REGISTRY:
            s = st.get(n)
            if not s:
                continue
            if self.tree.set(n, "state") in ("running", "queued") and self.running:
                continue
            self.tree.set(n, "state", s.get("state", ""))
            self.tree.set(n, "finished", str(s.get("finished", ""))[:16].replace("T", " "))
            self.tree.set(n, "secs", s.get("seconds", ""))
            extra = {k: v for k, v in s.items() if k not in ("state", "started", "finished", "seconds", "error")}
            self.tree.set(n, "info", (s.get("error") or ", ".join(f"{k}={v}" for k, v in extra.items()))[:140])

    # ----------------------------------------------------------------------------- auto update
    def _auto_toggle(self):
        if self.auto_job:
            self.after_cancel(self.auto_job)
            self.auto_job = None
        if self.auto_on.get():
            self.status.set(f"Auto-update on: every {self.auto_min.get()} min (first run now)")
            self._auto_run()
            self.auto_job = self.after(max(1, int(self.auto_min.get())) * 60_000, self._auto_tick)

    def _auto_tick(self):
        if not self.auto_on.get():
            return
        self._auto_run()
        self.auto_job = self.after(max(1, int(self.auto_min.get())) * 60_000, self._auto_tick)

    def _auto_run(self):
        if not self.running:
            names = [n for n, c in REGISTRY.items() if self.sel[n]]
            if names:
                self._run(names)

    # ----------------------------------------------------------------------------- tabs
    def _load_async(self):
        """Build the board and the disk-usage text off the UI thread so the window opens instantly."""
        self.board.delete("1.0", "end")
        self.board.insert("end", "Building the snapshot from stored data ... (a few seconds)")

        def work():
            try:
                from .briefing import render
                self.q.put(("board", render()))
            except Exception as e:
                self.q.put(("board", f"Could not build the snapshot: {type(e).__name__}: {e}"))
            try:
                self.q.put(("disk", self._disk_text()))
            except Exception as e:
                self.q.put(("disk", f"Could not scan the data folder: {e}"))

        threading.Thread(target=work, daemon=True).start()

    def _tab_changed(self, _ev=None):
        if self.running:
            return
        tab = self.nb.tab(self.nb.select(), "text")
        if tab == "Snapshot" and len(self.board.get("1.0", "end").strip()) < 80:
            self._load_async()

    def refresh_board(self):
        from .briefing import render
        self.board.delete("1.0", "end")
        self.board.insert("end", "building ...")
        self.update_idletasks()
        txt = render()
        self.board.delete("1.0", "end")
        self.board.insert("end", txt)

    def refresh_disk(self):
        self.disk.delete("1.0", "end")
        self.disk.insert("end", self._disk_text())

    def _disk_text(self) -> str:
        lines = []
        tot_b = 0
        for d in sorted(DATA.iterdir()) if DATA.exists() else []:
            if d.is_dir():
                n, b = dir_stats(d)
                tot_b += b
                lines.append(f"{d.name:20s} {n:7d} files  {human(b):>10s}")
                for sd in sorted(x for x in d.iterdir() if x.is_dir()):
                    n2, b2 = dir_stats(sd)
                    lines.append(f"   {sd.name:17s} {n2:7d} files  {human(b2):>10s}")
        lines.append(f"\nTOTAL {human(tot_b)}   in {DATA}")
        return "\n".join(lines) if tot_b else f"No data yet in {DATA}\nPress 'Update ALL selected' on the Collectors tab."

    def _close(self):
        if self.running and not messagebox.askyesno("Quit", "An update is running. Stop it and quit?"):
            return
        self.stop_evt.set()
        self.destroy()


def main():
    load_all()
    App().mainloop()
    return 0
