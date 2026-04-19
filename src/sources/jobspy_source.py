"""JobSpy-backed source — scrapes LinkedIn / Indeed / Glassdoor / ZipRecruiter.

The `python-jobspy` library is imported lazily so this module can be imported
(and unit-tested) in environments where the dependency is unavailable. At run
time, if the library is missing we return zero jobs with a single warning —
the rest of the pipeline carries on.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any

from src.shared import build_job

log = logging.getLogger(__name__)

SEARCH_TERMS: list[str] = [
    "people analytics",
    "employee listening",
    "workforce analytics",
    "HR analytics",
    "talent analytics",
    "people science",
    "employee experience analytics",
]

SITES: list[str] = ["linkedin", "indeed", "glassdoor", "zip_recruiter"]
RESULTS_PER_SITE = 20
HOURS_OLD = 48
LOCATION = "United States"


def _scrape_jobs_callable():
    """Lazy-load jobspy.scrape_jobs. Returns None if the package isn't installed."""
    try:
        from jobspy import scrape_jobs  # type: ignore
    except Exception as e:  # noqa: BLE001 — broad: tls_client bootstrap issues included
        log.info("jobspy not available (%s) — skipping", e)
        return None
    return scrape_jobs


def _hash_id(site: str, url: str) -> str:
    h = hashlib.sha256((site + "|" + (url or "")).encode("utf-8")).hexdigest()[:16]
    return f"jobspy_{site}_{h}"


def _row_to_job(row: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a single JobSpy result row (as a dict) to our standard job dict."""
    title = row.get("title") or ""
    company = row.get("company") or ""
    url = row.get("job_url") or row.get("job_url_direct") or ""
    if not title or not company or not url:
        return None
    site = row.get("site") or "unknown"
    is_remote = "remote" if row.get("is_remote") else None
    date_posted = row.get("date_posted")
    if date_posted is not None:
        date_posted = str(date_posted)[:10] or None
    return build_job(
        source_name=f"jobspy_{site}",
        external_id=_hash_id(site, url),
        title=title,
        company=company,
        location=row.get("location") or "",
        description=row.get("description") or "",
        source_url=url,
        apply_url=url,
        salary_min=row.get("min_amount") or None,
        salary_max=row.get("max_amount") or None,
        is_remote=is_remote or "unknown",
        date_posted=date_posted,
        raw_data=row,
    )


def _iter_rows(result: Any):
    """JobSpy returns a pandas DataFrame. Iterate it as dicts without requiring
    pandas at import time."""
    if result is None:
        return []
    # DataFrame: has .iterrows; convert each row to dict via ._asdict or to_dict
    iterrows = getattr(result, "iterrows", None)
    if callable(iterrows):
        rows: list[dict[str, Any]] = []
        for _, row in iterrows():
            # row is a Series; use .to_dict() when available
            to_dict = getattr(row, "to_dict", None)
            rows.append(to_dict() if callable(to_dict) else dict(row))
        return rows
    # Fallback: already a list of dicts (used in tests)
    if isinstance(result, list):
        return result
    return []


def fetch(
    *,
    search_terms: list[str] | None = None,
    results_wanted: int = RESULTS_PER_SITE,
    hours_old: int = HOURS_OLD,
    location: str = LOCATION,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Run JobSpy for each search term across all configured sites."""
    scrape = _scrape_jobs_callable()
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    if scrape is None:
        return results, errors, {"available": False, "terms_run": 0}

    terms = search_terms if search_terms is not None else SEARCH_TERMS
    terms_run = 0
    # Per-site isolation: loop over (term, site) so a Glassdoor TLS bounce
    # doesn't take out LinkedIn/Indeed/ZipRecruiter for that term. JobSpy's
    # internal per-site isolation is inconsistent across versions.
    per_site_counts: dict[str, int] = {}
    for term in terms:
        term_produced = False
        for site in SITES:
            try:
                df = scrape(
                    site_name=[site],
                    search_term=term,
                    location=location,
                    results_wanted=results_wanted,
                    hours_old=hours_old,
                    country_indeed="USA",
                )
            except Exception as e:  # noqa: BLE001 — one bad site shouldn't lose the others
                errors.append(f"jobspy[{term}/{site}]: {e}")
                continue
            term_produced = True
            for row in _iter_rows(df):
                try:
                    job = _row_to_job(row)
                    if job:
                        results.append(job)
                        per_site_counts[site] = per_site_counts.get(site, 0) + 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"jobspy[{term}/{site}]: map error: {e}")
        if term_produced:
            terms_run += 1
    return results, errors, {
        "available": True, "terms_run": terms_run,
        "per_site_counts": per_site_counts,
    }
