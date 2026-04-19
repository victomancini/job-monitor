"""Lightweight URL enrichment: fetch a job's source/apply page, extract salary,
remote status, and location; set confidence flags on the job dict.

Skips jobs already enriched within the last 7 days. Swallows all HTTP/parse
errors and records enrichment_source='aggregator' so the pipeline keeps moving."""
from __future__ import annotations

import logging
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

import requests

from src.shared import AGGREGATOR_HOSTS, format_salary_range

log = logging.getLogger(__name__)

# R6-I1: all module-level constants live here, BEFORE any function that
# references them. Previously USER_AGENT was defined below _head_final_url,
# which referenced it — Python's late-binding made it work, but any future
# refactor that evaluated a default arg at import time would NameError.
FETCH_TIMEOUT_SEC = 10.0
RATE_LIMIT_SEC = 1.0
ENRICHMENT_FRESH_DAYS = 7
USER_AGENT = "Mozilla/5.0 (compatible; job-monitor/1.0)"
# IMP-N8: concurrent enrichment. Each host still gets serialized ≥ RATE_LIMIT_SEC
# via `_HostThrottle`, so distinct hosts fetch in parallel while a single host
# never exceeds 1 req/sec. `max_workers` caps total concurrency.
DEFAULT_ENRICHMENT_WORKERS = 5

# Meta-refresh and JS redirect patterns — aggregator pages often serve HTTP 200
# and then bounce via <meta http-equiv="refresh"> or window.location. Phase A's
# redirect following (allow_redirects=True) misses these; parsing the response
# body catches them.
_META_REFRESH_RE = re.compile(
    r"""<meta\s+http-equiv=["']refresh["']\s+content=["']\s*\d+\s*;\s*url=([^"']+)""",
    re.IGNORECASE,
)
_JS_REDIRECT_RES = [
    re.compile(r"""(?:window\.)?location(?:\.href)?\s*=\s*["']([^"']+)["']""", re.IGNORECASE),
    re.compile(r"""location\.replace\s*\(\s*["']([^"']+)["']""", re.IGNORECASE),
    re.compile(r"""location\.assign\s*\(\s*["']([^"']+)["']""", re.IGNORECASE),
]


def _host(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""


def _extract_body_redirect(html: str, base_url: str) -> str:
    """Return a redirect target URL found in an HTML body (meta-refresh or JS
    window.location), absolutized against `base_url`. Empty string if none."""
    from urllib.parse import urljoin
    if not html:
        return ""
    # R5-10: scan the first ~32kB. Redirect scripts usually live near the top
    # but some SPA-style aggregators emit 20–30kB of inline CSS before the
    # window.location bounce. Regex over 32kB is still sub-millisecond.
    scope = html[:32000]
    candidates: list[str] = []
    m = _META_REFRESH_RE.search(scope)
    if m:
        candidates.append(m.group(1).strip())
    for pat in _JS_REDIRECT_RES:
        m = pat.search(scope)
        if m:
            candidates.append(m.group(1).strip())
    for target in candidates:
        if not target or target.startswith("#"):
            continue
        absolute = urljoin(base_url, target)
        # Only treat it as a real redirect if the target is on a different host
        # — self-links and fragment anchors aren't what we're after.
        if _host(absolute) and _host(absolute) != _host(base_url):
            return absolute
    return ""


def _head_final_url(url: str, *, timeout: float = 10.0) -> str:
    """Follow a URL via HEAD request (then fall back to GET stream), returning
    the final URL after all redirects. Empty string if the request fails."""
    try:
        resp = requests.head(
            url, timeout=timeout, allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        final = getattr(resp, "url", "") or ""
        if final and final != url and _host(final) != _host(url):
            return final
    except requests.RequestException:
        pass
    # Some servers reject HEAD. Stream a GET and grab the final URL.
    try:
        resp = requests.get(
            url, timeout=timeout, allow_redirects=True, stream=True,
            headers={"User-Agent": USER_AGENT},
        )
        final = getattr(resp, "url", "") or ""
        resp.close()
        if final and final != url and _host(final) != _host(url):
            return final
    except requests.RequestException:
        pass
    return ""


_US_STATES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|"
    "MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|"
    "TX|UT|VT|VA|WA|WV|WI|WY|DC|PR"
)


# ───────────────────────── Salary extraction ──────────────────────────

_SALARY_PATTERNS = [
    # "salary/pay/compensation/range:" prefix
    re.compile(
        r"(?:salary|pay|compensation|range)[:\s]+\$?\s*([\d,]+)\s*(?:-|–|—|to)\s*\$?\s*([\d,]+)",
        re.IGNORECASE,
    ),
    # $120K - $180K (K suffix)
    re.compile(r"\$\s*(\d+)\s*[kK]\s*(?:-|–|—|to)\s*\$?\s*(\d+)\s*[kK]", re.IGNORECASE),
    # $120,000 - $180,000
    re.compile(r"\$\s*([\d,]+)\s*(?:-|–|—|to)\s*\$\s*([\d,]+)"),
    # $120,000 - 180,000 (trailing $ optional)
    re.compile(r"\$\s*([\d,]+)\s*(?:-|–|—|to)\s*([\d,]+)"),
]


def _parse_salary_value(s: str) -> float | None:
    s = (s or "").strip().replace(",", "")
    try:
        v = float(s)
    except (TypeError, ValueError):
        return None
    if v < 1000:
        v *= 1000
    if v < 10_000 or v > 1_000_000:
        return None
    return v


def _extract_salary(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    for pat in _SALARY_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        lo = _parse_salary_value(m.group(1))
        hi = _parse_salary_value(m.group(2))
        if lo is None or hi is None:
            continue
        if lo > hi:
            lo, hi = hi, lo
        return {"min": lo, "max": hi, "range_str": format_salary_range(lo, hi)}
    return None


# ───────────────────────── Remote detection ──────────────────────────

_REMOTE_PATTERNS = {
    "remote": [
        re.compile(r"\bfully\s+remote\b", re.IGNORECASE),
        re.compile(r"\b100%\s+remote\b", re.IGNORECASE),
        re.compile(r"\bremote\s+position\b", re.IGNORECASE),
        re.compile(r"\bwork\s+from\s+home\b", re.IGNORECASE),
        re.compile(r"\bremote\s+eligible\b", re.IGNORECASE),
    ],
    "hybrid": [
        re.compile(r"\bhybrid\b", re.IGNORECASE),
        re.compile(r"\b\d+\s*days?\s+(?:in\s+)?(?:the\s+)?office\b", re.IGNORECASE),
        re.compile(r"\bin-office\s+\d+\b", re.IGNORECASE),
    ],
    "onsite": [
        re.compile(r"\bon[\s-]?site\b", re.IGNORECASE),
        re.compile(r"\bin[\s-]?office\b", re.IGNORECASE),
        re.compile(r"\bin[\s-]?person\b", re.IGNORECASE),
        # "no remote work/option/policy/position" — tightened to avoid matching
        # generic phrases like "no remote info".
        re.compile(r"\bno\s+remote\s+(?:work|option|policy|position|role)\b", re.IGNORECASE),
    ],
}


def _extract_remote_status(text: str) -> str | None:
    """Return 'remote'/'hybrid'/'onsite' or None. Hybrid is the most specific signal
    and outranks both remote and onsite per spec."""
    if not text:
        return None
    is_remote = any(p.search(text) for p in _REMOTE_PATTERNS["remote"])
    is_hybrid = any(p.search(text) for p in _REMOTE_PATTERNS["hybrid"])
    is_onsite = any(p.search(text) for p in _REMOTE_PATTERNS["onsite"])
    if is_hybrid:
        return "hybrid"
    if is_remote:
        return "remote"
    if is_onsite:
        return "onsite"
    return None


# ───────────────────────── Location extraction ───────────────────────

_LOCATION_PREFIX_RE = re.compile(
    r"\blocation[:\s]+([A-Z][A-Za-z.\s]*?,\s*[A-Z]{2})\b"
)
_CITY_STATE_RE = re.compile(
    r"\b([A-Z][A-Za-z.]+(?:\s+[A-Z][A-Za-z.]+)*,\s*(?:" + _US_STATES + r"))\b"
)
_MULTI_LOC_RE = re.compile(r"\bmultiple\s+locations\b", re.IGNORECASE)


def _extract_location(text: str) -> str | None:
    if not text:
        return None
    if _MULTI_LOC_RE.search(text):
        return "Multiple Locations"
    m = _LOCATION_PREFIX_RE.search(text)
    if m:
        return m.group(1).strip()
    m = _CITY_STATE_RE.search(text)
    if m:
        return m.group(1).strip()
    return None


# ───────────────────────── HTML → text ───────────────────────────────

_HTML_SCRIPT_STYLE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_HTML_TAG = re.compile(r"<[^>]+>")


def _extract_text(html: str) -> str:
    if not html:
        return ""
    out = _HTML_SCRIPT_STYLE.sub(" ", html)
    out = _HTML_TAG.sub(" ", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


# ───────────────────────── Freshness guard ───────────────────────────

def _was_recently_enriched(job: dict[str, Any]) -> bool:
    d = job.get("enrichment_date")
    if not d:
        return False
    try:
        when = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return False
    return (datetime.now(timezone.utc) - when) < timedelta(days=ENRICHMENT_FRESH_DAYS)


# ───────────────────────── Phase J: pre-enrichment + defaults ─────────

def _apply_llm_hints(job: dict[str, Any]) -> None:
    """Phase J Pass 0: consume LLM-extracted `_llm_remote` and `_llm_salary_hint`
    as inferred values when the aggregator didn't provide them."""
    llm_remote = job.get("_llm_remote")
    if llm_remote and job.get("is_remote") in (None, "", "unknown"):
        # Normalize "on-site" / "on_site" → "onsite" to match the canonical form
        normalized = llm_remote.replace("-", "").replace("_", "")
        if normalized in ("remote", "hybrid", "onsite"):
            job["is_remote"] = normalized
            job["remote_confidence"] = "inferred"

    llm_salary = job.get("_llm_salary_hint")
    if llm_salary and not job.get("salary_min"):
        parsed = _extract_salary(llm_salary)
        if parsed:
            job["salary_min"] = parsed["min"]
            job["salary_max"] = parsed["max"]
            job["salary_range"] = parsed["range_str"]
            job["salary_confidence"] = "inferred"


def _pre_enrich_from_description(job: dict[str, Any]) -> None:
    """Phase J Pass 1: extract remote/salary/location from the aggregator's
    description text before any HTTP fetch. Fills only empty/unknown fields and
    marks them `inferred`."""
    desc = job.get("description") or ""
    if not desc:
        return

    if job.get("is_remote") in (None, "", "unknown"):
        remote = _extract_remote_status(desc)
        if remote:
            job["is_remote"] = remote
            job["remote_confidence"] = "inferred"

    if not job.get("salary_min"):
        parsed = _extract_salary(desc)
        if parsed:
            job["salary_min"] = parsed["min"]
            job["salary_max"] = parsed["max"]
            job["salary_range"] = parsed["range_str"]
            job["salary_confidence"] = "inferred"

    if not job.get("location"):
        loc = _extract_location(desc)
        if loc:
            job["location"] = loc
            job["location_confidence"] = "inferred"


def _apply_assumed_defaults(job: dict[str, Any]) -> None:
    """Phase J Pass 3: if no source mentioned remote status after everything,
    default to `onsite` with assumed confidence (most jobs are on-site)."""
    if job.get("is_remote") in (None, "", "unknown"):
        job["is_remote"] = "onsite"
        job["remote_confidence"] = "assumed"


# ───────────────────────── Public API ────────────────────────────────

def enrich_job(job: dict[str, Any]) -> dict[str, Any]:
    """Three-pass enrichment:
    Pass 0 — consume LLM hints stashed as `_llm_remote` / `_llm_salary_hint`.
    Pass 1 — regex-extract from the aggregator's description text (inferred).
    Pass 2 — HTTP fetch the source page and overlay confirmed/aggregator_only.
    Pass 3 — default `is_remote` to 'onsite' with assumed confidence if silent.

    `inferred` values set in Passes 0 and 1 are preserved; Pass 2 only downgrades
    to `aggregator_only` when the prior state was the untouched aggregator value."""
    if _was_recently_enriched(job):
        return job

    # Pass 0 + 1 run regardless of whether we can fetch
    _apply_llm_hints(job)
    _pre_enrich_from_description(job)

    url = job.get("apply_url") or job.get("source_url")
    orig_apply = job.get("apply_url")
    log.debug("enrich_job[%s] source=%s url_host=%s",
              job.get("external_id"), job.get("source_name"), _host(url or ""))
    if not url:
        job["enrichment_source"] = "aggregator"
        _apply_assumed_defaults(job)
        return job

    try:
        resp = requests.get(
            url,
            timeout=FETCH_TIMEOUT_SEC,
            headers={"User-Agent": USER_AGENT},
            allow_redirects=True,
        )
    except requests.RequestException as e:
        log.warning("enrichment: failed to fetch %s: %s", url[:80], e)
        job["enrichment_source"] = "aggregator"
        _apply_assumed_defaults(job)
        return job
    except Exception as e:  # noqa: BLE001 — any fetch error routes to aggregator fallback
        log.warning("enrichment: unexpected error fetching %s: %s", url[:80], e)
        job["enrichment_source"] = "aggregator"
        _apply_assumed_defaults(job)
        return job

    # Phase A: if we left the aggregator domain via redirect, store the final URL
    # as the canonical apply_url.
    final_url = getattr(resp, "url", None) or url
    orig_host = _host(url)
    final_host = _host(final_url)
    if final_url and final_url != url and final_host and final_host != orig_host \
            and orig_host in AGGREGATOR_HOSTS:
        job["apply_url"] = final_url
        url = final_url  # subsequent passes fetch the direct page instead
        orig_host = final_host

    if resp.status_code != 200:
        # R-audit: before giving up, try once more via HEAD — aggregators
        # sometimes 403 on GET but redirect on HEAD, or the original URL was
        # briefly flaky. If that resolves to a non-aggregator host, rewrite.
        if orig_host in AGGREGATOR_HOSTS:
            resolved = _head_final_url(job.get("apply_url") or url)
            if resolved and _host(resolved) not in AGGREGATOR_HOSTS:
                job["apply_url"] = resolved
        job["enrichment_source"] = "aggregator"
        _apply_assumed_defaults(job)
        return job

    # R-audit (Issue 1b): the aggregator returned 200 but may be bouncing via
    # meta-refresh / JS. Scan the body for a redirect target and rewrite.
    if orig_host in AGGREGATOR_HOSTS:
        body_redirect = _extract_body_redirect(resp.text or "", url)
        if body_redirect and _host(body_redirect) not in AGGREGATOR_HOSTS:
            job["apply_url"] = body_redirect
            log.debug("enrich_job[%s]: body-redirect rewrote apply_url %s -> %s",
                      job.get("external_id"), url, body_redirect)

    text = _extract_text(resp.text or "")

    # Salary — only mark confirmed when source page is the first to provide it
    # or it corroborates a description-inferred value. Aggregator-provided salary
    # gets aggregator_only (we don't trust source-page numbers to overwrite).
    salary_found = _extract_salary(text)
    if salary_found:
        if not job.get("salary_min"):
            job["salary_min"] = salary_found["min"]
            job["salary_max"] = salary_found["max"]
            job["salary_range"] = salary_found["range_str"]
            job["salary_confidence"] = "confirmed"
        elif job.get("salary_confidence") == "inferred":
            job["salary_confidence"] = "confirmed"
        else:
            job["salary_confidence"] = "aggregator_only"
    elif job.get("salary_min") and job.get("salary_confidence") != "inferred":
        job["salary_confidence"] = "aggregator_only"

    # Remote — same rule
    remote_found = _extract_remote_status(text)
    if remote_found:
        cur_remote = job.get("is_remote")
        if not cur_remote or cur_remote == "unknown":
            job["is_remote"] = remote_found
            job["remote_confidence"] = "confirmed"
        elif job.get("remote_confidence") == "inferred":
            job["remote_confidence"] = "confirmed"
        else:
            job["remote_confidence"] = "aggregator_only"
    elif (job.get("is_remote")
          and job.get("is_remote") != "unknown"
          and job.get("remote_confidence") != "inferred"):
        job["remote_confidence"] = "aggregator_only"

    # Location — same rule
    location_found = _extract_location(text)
    if location_found:
        if not job.get("location"):
            job["location"] = location_found
            job["location_confidence"] = "confirmed"
        elif job.get("location_confidence") == "inferred":
            job["location_confidence"] = "confirmed"
        else:
            job["location_confidence"] = "aggregator_only"
    elif job.get("location") and job.get("location_confidence") != "inferred":
        job["location_confidence"] = "aggregator_only"

    job["enrichment_source"] = "source_page"
    job["enrichment_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # R-audit (Issue 1c): last-chance fallback. If apply_url is still on an
    # aggregator host after body-redirect parsing, do one HEAD request — some
    # sites use multiple redirect hops that requests.get didn't fully chase.
    final_apply = job.get("apply_url") or ""
    if _host(final_apply) in AGGREGATOR_HOSTS:
        resolved = _head_final_url(final_apply)
        if resolved and _host(resolved) not in AGGREGATOR_HOSTS:
            job["apply_url"] = resolved
            log.debug("enrich_job[%s]: HEAD fallback rewrote apply_url -> %s",
                      job.get("external_id"), resolved)

    _apply_assumed_defaults(job)
    if (job.get("apply_url") or "") != (orig_apply or ""):
        log.debug("enrich_job[%s]: apply_url %s -> %s",
                  job.get("external_id"), orig_apply, job.get("apply_url"))
    return job


class _HostThrottle:
    """Per-host sequential throttle: any two fetches to the same host are
    separated by at least `min_gap` seconds. Distinct hosts proceed in parallel.
    Safe for use from multiple threads."""

    def __init__(self, min_gap: float = RATE_LIMIT_SEC) -> None:
        self.min_gap = min_gap
        self._locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._last_fetch: dict[str, float] = defaultdict(lambda: 0.0)
        self._guard = threading.Lock()

    def _lock_for(self, host: str) -> threading.Lock:
        with self._guard:
            return self._locks[host]

    def acquire(self, host: str) -> None:
        """Block until the host is available per the throttle contract."""
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


def enrich_batch(
    jobs: list[dict[str, Any]],
    *,
    delay: float = RATE_LIMIT_SEC,
    max_workers: int | None = None,
) -> list[dict[str, Any]]:
    """Enrich a list of jobs in place. By default parallelizes across hosts
    using `DEFAULT_ENRICHMENT_WORKERS` threads while still serializing same-host
    fetches to `delay` seconds apart. Pass `max_workers=1` for the old
    sequential behavior."""
    if not jobs:
        return jobs
    workers = max_workers if max_workers is not None else DEFAULT_ENRICHMENT_WORKERS
    if workers <= 1:
        for i, job in enumerate(jobs):
            enrich_job(job)
            if i < len(jobs) - 1:
                time.sleep(delay)
        return jobs

    throttle = _HostThrottle(min_gap=delay)

    def _worker(job: dict[str, Any]) -> None:
        url = job.get("apply_url") or job.get("source_url") or ""
        throttle.acquire(_host(url))
        enrich_job(job)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_worker, j) for j in jobs]
        for f in as_completed(futures):
            exc = f.exception()
            if exc is not None:
                log.warning("enrichment: worker raised: %s", exc)
    return jobs
