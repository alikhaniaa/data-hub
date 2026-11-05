"""HTTP helper: one shared session, retries with backoff, per-host throttling, 429 handling."""
from __future__ import annotations

import threading
import time
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter


class HttpError(Exception):
    def __init__(self, status: int, url: str, body: str = ""):
        super().__init__(f"HTTP {status} for {url[:140]} {body[:120]}")
        self.status = status
        self.url = url


class _Throttle:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_at - now)
            self.next_at = max(now, self.next_at) + self.min_interval
        if delay:
            time.sleep(delay)


class Http:
    def __init__(self, timeout: float = 30, retries: int = 4, user_agent: str = "tdata/1.0", stop: threading.Event | None = None):
        self.timeout = timeout
        self.retries = retries
        self.stop = stop or threading.Event()
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": user_agent, "Accept": "*/*", "Accept-Encoding": "gzip, deflate"})
        ad = HTTPAdapter(pool_connections=64, pool_maxsize=64)
        self.s.mount("https://", ad)
        self.s.mount("http://", ad)
        self._throttles: dict[str, _Throttle] = {}
        self._tl = threading.Lock()

    def throttle(self, host_contains: str, min_interval: float):
        """Minimum seconds between requests to any host containing `host_contains`."""
        with self._tl:
            self._throttles[host_contains] = _Throttle(min_interval)

    def _wait(self, url: str):
        host = urlparse(url).netloc
        for key, th in list(self._throttles.items()):
            if key in host:
                th.wait()

    def request(self, method: str, url: str, *, ok404: bool = False, **kw) -> requests.Response:
        kw.setdefault("timeout", self.timeout)
        retries = kw.pop("retries", self.retries)
        last_err: Exception | None = None
        for attempt in range(retries + 1):
            if self.stop.is_set():
                raise InterruptedError("stopped")
            self._wait(url)
            try:
                r = self.s.request(method, url, **kw)
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as e:
                last_err = e
                time.sleep(min(30, 1.5 * (2 ** attempt)))
                continue
            if r.status_code == 404 and ok404:
                return r
            if r.status_code in (418, 429) or r.status_code >= 500:
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra else 3 * (2 ** attempt)
                except ValueError:
                    wait = 3 * (2 ** attempt)
                last_err = HttpError(r.status_code, url, r.text)
                time.sleep(min(90, wait))
                continue
            if r.status_code >= 400:
                raise HttpError(r.status_code, url, r.text)
            return r
        if isinstance(last_err, Exception):
            raise last_err
        raise HttpError(0, url, "unknown failure")

    def get(self, url, **kw):
        return self.request("GET", url, **kw)

    def post(self, url, **kw):
        return self.request("POST", url, **kw)

    def json(self, url, **kw):
        return self.get(url, **kw).json()

    def post_json(self, url, body, **kw):
        return self.post(url, json=body, **kw).json()

    def text(self, url, **kw):
        r = self.get(url, **kw)
        if r.encoding is None or r.encoding.lower() == "iso-8859-1":
            r.encoding = r.apparent_encoding or "utf-8"
        return r.text
