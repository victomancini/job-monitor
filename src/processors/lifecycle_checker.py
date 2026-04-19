"""Source-of-truth lifecycle checker.

The aggregators (Jooble / Adzuna / Indeed / JSearch-via-Google) keep listings
alive for weeks after a role is filled and drop some postings that are still
open. The company's career page (or the ATS board API we're already polling)
is the authoritative signal.

Three resolution paths, in priority order:

  A) ATS API: if `source_name` is greenhouse/lever/ashby and the caller
     supplies an `ats_snapshots` map keyed by (ats, slug) → the set of job IDs
     currently on the board, we can answer lifecycle from the fetch the
     collector already did — no extra HTTP required.

  B) Direct company URL: `apply_url` lives on a non-aggregator host. HEAD
     request with redirect following; 2xx → active, 4xx (especially 404/410)
     → likely_closed, everything else → unknown.

  C) Aggregator URL: no authoritative signal available — return "unknown" and
     let the time-based staleness pass in archiver.py handle it.

Category B checks are parallelized across distinct hosts (R5-11); same-host
checks still serialize at `delay` seconds apart. DB writes are batched onto
the caller thread because SQLite/libsql connections aren't thread-safe.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlparse

import requests

from src import db
from src.shared import AGGREGATOR_HOSTS

log = logging.getLogger(__name__)

CHECK_TIMEOUT_SEC = 10.0
CHECK_DELAY_SEC = 2.0           # per-host pacing between HEAD/GET checks
CHECK_FRESHNESS_DAYS = 2        # jobs checked within this window are skipped
CHECK_BATCH_LIMIT = 500         # cap total DB picks per run
# R4-1: HTTP budget is a separate, tighter cap. ATS-snapshot hits are free so
# they don't consume this budget; only per-URL HEAD/GET requests count.
CHECK_HTTP_BUDGET = 150
# R5-11: parallelize HEAD requests across distinct hosts. Same-host requests
# still serialize at >= delay seconds via _HostThrottle so we're as polite
# per-host as the serial version was.
DEFAULT_LIFECYCLE_WORKERS = 5
USER_AGENT = "Mozilla/5.0 (compatible; job-monitor-lifecycle/1.0)"

ATS_SOURCES = frozenset({"greenhouse", "lever", "ashby"})


def _host(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""


def _parse_ats_key(external_id: str, source_name: str) -> tuple[str, str] | None:
    """Extract (ats, slug) from an ATS external_id. Returns None if the id
    shape doesn't match. Formats follow src/sources/{greenhouse,lever,ashby}.py:

      greenhouse: gh_{slug}_{jid}   → ats=greenhouse, slug=<slug>
      lever     : lever_{slug}_{jid}
      ashby     : ashby_{slug}_{jid}
    """
    if not external_id or source_name not in ATS_SOURCES:
        return None
    prefix_map = {"greenhouse": "gh_", "lever": "lever_", "ashby": "ashby_"}
    prefix = prefix_map[source_name]
    if not external_id.startswith(prefix):
        return None
    rest = external_id[len(prefix):]
    # slug is everything up to the LAST "_"; the job id is the tail
    if "_" not in rest:
        return None
    slug, _, _ = rest.rpartition("_")
    if not slug:
        return None
    return source_name, slug


def _ats_membership_result(
    external_id: str, source_name: str,
    ats_snapshots: dict[tuple[str, str], set[str]] | None,
) -> str | None:
    """If we have a fresh board snapshot for this (ats, slug), resolve status
    by membership. Returns 'active' | 'likely_closed', or None when no snapshot
    is available (caller falls back to HTTP check)."""
    if not ats_snapshots:
        return None
    key = _parse_ats_key(external_id, source_name)
    if key is None:
        return None
    snapshot = ats_snapshots.get(key)
    if snapshot is None:
        return None
    prefix_len = {"greenhouse": len("gh_"), "lever": len("lever_"), "ashby": len("ashby_")}[source_name]
    rest = external_id[prefix_len:]
    _, _, jid = rest.rpartition("_")
    if not jid:
        return None
    return "active" if jid in snapshot else "likely_closed"


def _try_head(url: str, timeout: float):
    """Single HEAD attempt. Returns the response or None on RequestException."""
    try:
        return requests.head(
            url, timeout=timeout, allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
    except requests.RequestException:
        return None


def _try_get(url: str, timeout: float) -> int | None:
    """Single streamed GET. Returns the status code or None on RequestException."""
    try:
        resp = requests.get(
            url, timeout=timeout, allow_redirects=True, stream=True,
            headers={"User-Agent": USER_AGENT},
        )
        code = resp.status_code
        resp.close()
        return code
    except requests.RequestException:
        return None


def _head_status_code(url: str, *, timeout: float = CHECK_TIMEOUT_SEC) -> int | None:
    """Return the final status code after redirects for a HEAD request, falling
    back to a streamed GET when HEAD is rejected. None on repeated transport
    errors.

    R4-16: many CDNs (Cloudflare, AWS) answer HEAD with 403 even when GET
    succeeds — they treat HEAD as a bot signal. Fall back to GET on 403 (and
    the canonical 405/501 "method not allowed") rather than marking the job
    unknown. 404/410 stay authoritative; 5xx stays unknown (transient).

    R5-7: one retry on connection reset / timeout before giving up. A single
    flaky blip shouldn't flip the job to 'unknown' — that wastes a day of the
    freshness window.
    """
    # Attempt 1: HEAD (with one retry on RequestException).
    resp = _try_head(url, timeout)
    if resp is None:
        resp = _try_head(url, timeout)  # R5-7: one retry on connection reset
    if resp is not None and resp.status_code < 400:
        return resp.status_code
    # HEAD-hostile responses: retry as GET. 403 is included — many CDNs block
    # HEAD specifically. 404/410/5xx stay authoritative / transient.
    if resp is not None and resp.status_code in (403, 405, 501):
        resp = None
    if resp is None:
        # Attempt 2: GET (with one retry on RequestException).
        code = _try_get(url, timeout)
        if code is None:
            code = _try_get(url, timeout)
        return code
    return resp.status_code


def _classify_http_status(code: int | None) -> str:
    """Turn a status code into a lifecycle verdict."""
    if code is None:
        return "unknown"
    if 200 <= code < 300:
        return "active"
    if code in (404, 410):
        return "likely_closed"
    if 400 <= code < 500:
        # 401/403 often mean the page exists behind a login wall — ambiguous
        return "unknown"
    # 5xx → transient, treat as unknown
    return "unknown"


def check_job_status(
    job: dict[str, Any],
    *,
    ats_snapshots: dict[tuple[str, str], set[str]] | None = None,
) -> str:
    """Return 'active', 'likely_closed', or 'unknown' for one job.

    Resolution order: ATS snapshot (category A) → direct URL HEAD (category B)
    → aggregator fallback (category C) which always returns 'unknown'.
    """
    # Category A
    ats_verdict = _ats_membership_result(
        job.get("external_id") or "",
        job.get("source_name") or "",
        ats_snapshots,
    )
    if ats_verdict is not None:
        return ats_verdict

    apply_url = job.get("apply_url") or job.get("source_url") or ""
    host = _host(apply_url)

    # Category C — aggregator URL. No authoritative check available.
    if not host or host in AGGREGATOR_HOSTS:
        return "unknown"

    # Category B — direct company URL
    return _classify_http_status(_head_status_code(apply_url))


class _HostThrottle:
    """Per-host sequential throttle. Same shape as the enrichment version —
    any two requests to the same host wait at least `min_gap` seconds apart;
    distinct hosts proceed in parallel. Safe across threads."""

    def __init__(self, min_gap: float = CHECK_DELAY_SEC) -> None:
        self.min_gap = min_gap
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._last_fetch: dict[str, float] = defaultdict(lambda: 0.0)
        self._guard = threading.Lock()

    def _lock_for(self, host: str) -> threading.Lock:
        with self._guard:
            return self._locks[host]

    def acquire(self, host: str) -> None:
        lock = self._lock_for(host or "__empty__")
        lock.acquire()
        try:
            now = time.monotonic()
            last = self._last_fetch[host]
            wait = (last + self.min_gap) - now
            if wait > 0:
                time.sleep(wait)
            self._last_fetch[host] = time.monotonic()
        finally:
            lock.release()


def check_lifecycle_batch(
    conn,
    *,
    ats_snapshots: dict[tuple[str, str], set[str]] | None = None,
    stale_days: int = CHECK_FRESHNESS_DAYS,
    limit: int = CHECK_BATCH_LIMIT,
    delay: float = CHECK_DELAY_SEC,
    http_budget: int = CHECK_HTTP_BUDGET,
    max_workers: int | None = None,
) -> dict[str, int]:
    """Run a lifecycle check pass over active jobs. Skips rows checked within
    `stale_days` days. Returns a stats dict for healthcheck/ops visibility.

    R4-1 budget: `limit` caps total DB picks; `http_budget` caps per-run HEAD
    requests. Once HTTP budget is exhausted, remaining rows that would require
    an HTTP check are deferred to the next run (freshness skip ensures rotation
    so no row is permanently starved). ATS-snapshot resolutions are free and
    always run.
    """
    jobs = db.get_jobs_for_lifecycle_check(conn, stale_days=stale_days, limit=limit)
    stats = {"checked": 0, "still_active": 0, "likely_closed": 0,
             "unknown": 0, "ats_snapshot_hits": 0, "http_checks": 0,
             "http_budget_deferred": 0, "errors": 0}
    if not jobs:
        return stats

    workers = max_workers if max_workers is not None else DEFAULT_LIFECYCLE_WORKERS

    # Classify each job into one of three resolution paths so we can drive ATS
    # and aggregator verdicts locally on the main thread and only thread-pool
    # the real HTTP checks.
    verdicts: list[tuple[str, str]] = []  # (external_id, verdict) to record
    http_jobs: list[dict[str, Any]] = []

    for job in jobs:
        pre_ats = _ats_membership_result(
            job.get("external_id") or "",
            job.get("source_name") or "",
            ats_snapshots,
        )
        if pre_ats is not None:
            stats["ats_snapshot_hits"] += 1
            verdicts.append((job["external_id"], pre_ats))
            continue
        apply_url = job.get("apply_url") or job.get("source_url") or ""
        host = _host(apply_url)
        if not host or host in AGGREGATOR_HOSTS:
            # Category C: no authoritative signal available.
            verdicts.append((job["external_id"], "unknown"))
            continue
        http_jobs.append(job)

    # R4-1 budget: only the first `http_budget` HTTP-requiring jobs run this
    # pass; the rest are deferred (their last_lifecycle_check stays NULL so
    # they come back in the next run's candidate pool).
    http_to_run = http_jobs[:http_budget]
    stats["http_budget_deferred"] = len(http_jobs) - len(http_to_run)

    # R5-11: parallelize HEAD checks across distinct hosts; same-host requests
    # still serialize via _HostThrottle. Sequential path when workers<=1 for
    # deterministic tests.
    if http_to_run:
        throttle = _HostThrottle(min_gap=delay if delay > 0 else 0.0)

        def _worker(job: dict[str, Any]) -> tuple[str, str]:
            apply_url = job.get("apply_url") or job.get("source_url") or ""
            throttle.acquire(_host(apply_url))
            verdict = _classify_http_status(_head_status_code(apply_url))
            return job["external_id"], verdict

        if workers <= 1:
            for job in http_to_run:
                stats["http_checks"] += 1
                verdicts.append(_worker(job))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(_worker, j) for j in http_to_run]
                for f in as_completed(futures):
                    exc = f.exception()
                    if exc is not None:
                        stats["errors"] += 1
                        log.warning("lifecycle: worker raised: %s", exc)
                        continue
                    verdicts.append(f.result())
                    stats["http_checks"] += 1

    # SQLite/libsql connections aren't thread-safe — record verdicts here on
    # the caller thread, after all HTTP work is done.
    for ext_id, verdict in verdicts:
        stats["checked"] += 1
        if verdict == "active":
            stats["still_active"] += 1
        elif verdict == "likely_closed":
            stats["likely_closed"] += 1
        else:
            stats["unknown"] += 1
        try:
            db.record_lifecycle_check(conn, ext_id, verdict)
        except Exception as e:  # noqa: BLE001
            stats["errors"] += 1
            log.warning("lifecycle: db record failed for %s: %s", ext_id, e)

    log.info("lifecycle_checker: %s", stats)
    return stats
