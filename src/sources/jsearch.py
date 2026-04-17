"""JSearch API source (Google for Jobs via RapidAPI) — PRIMARY, US-only, 200 req/month budget."""
from __future__ import annotations

import logging
import time
from typing import Any

from src.shared import build_job, load_queries
from src.sources._http import retry_request

log = logging.getLogger(__name__)

BASE_URL = "https://jsearch.p.rapidapi.com/search"


def _remote_flag(job: dict[str, Any]) -> str:
    if job.get("job_is_remote"):
        return "remote"
    return "unknown"


def _country_code(job: dict[str, Any]) -> str:
    return (job.get("job_country") or "US").upper()


def _map(raw: dict[str, Any]) -> dict[str, Any] | None:
    jid = raw.get("job_id")
    if not jid:
        return None
    title = raw.get("job_title") or ""
    company = raw.get("employer_name") or ""
    if not title or not company:
        return None
    city = raw.get("job_city") or ""
    state = raw.get("job_state") or ""
    country = _country_code(raw)
    if city and state:
        location = f"{city}, {state}"
    elif city:
        location = city
    else:
        location = raw.get("job_location") or ""
    # JSearch returns job_apply_link (direct employer URL) and job_google_link (Google
    # for Jobs redirect). Prefer the direct link for apply_url.
    direct = raw.get("job_apply_link") or ""
    google = raw.get("job_google_link") or ""
    return build_job(
        source_name="jsearch",
        external_id=f"jsearch_{jid}",
        title=title,
        company=company,
        location=location,
        location_country=country,
        description=raw.get("job_description") or "",
        salary_min=raw.get("job_min_salary"),
        salary_max=raw.get("job_max_salary"),
        source_url=direct or google,
        apply_url=direct or google,
        is_remote=_remote_flag(raw),
        work_arrangement=raw.get("job_employment_type") or "",
        date_posted=raw.get("job_posted_at_datetime_utc"),
        raw_data=raw,
    )


def fetch(api_key: str, queries: list[dict[str, Any]] | None = None) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Return (jobs, errors, meta). meta includes quota_remaining when known."""
    if not api_key:
        return [], ["jsearch: JSEARCH_API_KEY not set"], {}
    queries = queries if queries is not None else load_queries()["jsearch"]["queries"]
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    quota_remaining: int | None = None

    for i, q in enumerate(queries):
        params = {
            "query": q["query"],
            "country": "us",
            "date_posted": q.get("date_posted", "3days"),
            "num_pages": q.get("num_pages", 1),
            "page": 1,
        }
        try:
            resp = retry_request("GET", BASE_URL, headers=headers, params=params)
            qhdr = resp.headers.get("X-RapidAPI-Requests-Remaining") if hasattr(resp, "headers") else None
            if qhdr is not None:
                try:
                    quota_remaining = int(qhdr)
                except (TypeError, ValueError):
                    pass
            if resp.status_code != 200:
                errors.append(f"jsearch: HTTP {resp.status_code} on '{q['query'][:40]}'")
                continue
            data = resp.json()
            for raw in data.get("data", []) or []:
                try:
                    job = _map(raw)
                    if job:
                        results.append(job)
                except Exception as e:  # noqa: BLE001 — one bad record can't kill the batch
                    errors.append(f"jsearch: map error: {e}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"jsearch: query error '{q['query'][:40]}': {e}")
        if i < len(queries) - 1:
            time.sleep(1.0)

    if quota_remaining is not None:
        log.info("jsearch quota remaining: %d", quota_remaining)
        if quota_remaining < 40:
            log.warning("jsearch quota LOW: %d requests remaining", quota_remaining)

    return results, errors, {"quota_remaining": quota_remaining}
