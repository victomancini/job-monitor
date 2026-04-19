"""Ashby public job board API — per-company slug, no auth.

`https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true`

Notably richer than Greenhouse/Lever: structured compensation
(`compensationTierSummary.min/max/currency/period`).
"""
from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

from src import db as dbmod
from src.shared import build_job
from src.sources._http import retry_request

log = logging.getLogger(__name__)

BASE_URL = "https://api.ashbyhq.com/posting-api/job-board/{slug}"
ATS_NAME = "ashby"
SLUG_DELAY_SEC = 1.0

DEFAULT_COMPANIES: dict[str, str] = {
    "notion": "Notion",
    "linear": "Linear",
    "ramp": "Ramp",
    "plaid": "Plaid",
    "vercel": "Vercel",
    "supabase": "Supabase",
    "posthog": "PostHog",
    "opensea": "OpenSea",
    "ironclad": "Ironclad",
    "retool": "Retool",
    "benchling": "Benchling",
}


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    return _WS_RE.sub(" ", _HTML_TAG_RE.sub(" ", html)).strip()


def _load_companies_from_config() -> dict[str, str]:
    cfg_path = Path(__file__).resolve().parent.parent.parent / "config" / "ats_companies.json"
    if not cfg_path.exists():
        return {}
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("ashby: failed to load %s: %s", cfg_path, e)
        return {}
    return {slug: name for slug, name in (data.get(ATS_NAME) or {}).items()}


def _extract_compensation(item: dict[str, Any]) -> tuple[float | None, float | None]:
    comp = item.get("compensation") or {}
    summary = comp.get("compensationTierSummary") or {}
    lo = summary.get("minValue") or summary.get("min")
    hi = summary.get("maxValue") or summary.get("max")

    def _coerce(v: Any) -> float | None:
        try:
            n = float(v)
        except (TypeError, ValueError):
            return None
        if n <= 0:
            return None
        return n

    return _coerce(lo), _coerce(hi)


def _ashby_workplace_to_is_remote(workplace_type: Any) -> str:
    """R8-M3: Ashby publishes `workplaceType` as Remote / Hybrid / OnSite.
    Map to the canonical is_remote vocabulary so downstream filters see
    authoritative values instead of 'unknown' for the ATS cohort."""
    if not isinstance(workplace_type, str):
        return "unknown"
    wt = workplace_type.strip().lower()
    if wt == "remote":
        return "remote"
    if wt == "hybrid":
        return "hybrid"
    if wt in ("onsite", "on-site", "on_site", "in-person", "inperson"):
        return "onsite"
    return "unknown"


def _map(item: dict[str, Any], slug: str, company_name: str) -> dict[str, Any] | None:
    jid = item.get("id")
    title = item.get("title") or ""
    if not jid or not title:
        return None
    location = item.get("location") or ""
    if isinstance(location, dict):  # some Ashby boards return structured location
        location = location.get("name") or ""
    description = _html_to_text(item.get("descriptionHtml") or item.get("descriptionPlain") or "")
    apply_url = item.get("jobUrl") or item.get("applyUrl") or ""
    salary_min, salary_max = _extract_compensation(item)
    # R8-M3: Ashby exposes a proper remote indicator; use it for is_remote.
    # Drop the department-as-work_arrangement pattern shared with GH/Lever.
    is_remote = _ashby_workplace_to_is_remote(item.get("workplaceType"))
    return build_job(
        source_name=ATS_NAME,
        external_id=f"ashby_{slug}_{jid}",
        title=title,
        company=company_name,
        location=location,
        description=description,
        source_url=apply_url,
        apply_url=apply_url,
        salary_min=salary_min,
        salary_max=salary_max,
        is_remote=is_remote,
        date_posted=item.get("publishedAt"),
        work_arrangement="",
        raw_data=item,
    )


def fetch(
    conn=None,
    companies: dict[str, str] | None = None,
    *,
    delay: float = SLUG_DELAY_SEC,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    if companies is None:
        loaded = _load_companies_from_config()
        companies = loaded or DEFAULT_COMPANIES
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    checked = 0
    skipped = 0
    successful_slugs: set[str] = set()  # R4-4: board-failure-safe set

    items = list(companies.items())
    for i, (slug, company_name) in enumerate(items):
        if conn is not None and dbmod.should_skip_ats_slug(conn, ATS_NAME, slug):
            skipped += 1
            continue
        url = BASE_URL.format(slug=slug)
        try:
            resp = retry_request("GET", url, params={"includeCompensation": "true"},
                                 max_attempts=2, timeout=20.0)
        except Exception as e:  # noqa: BLE001
            errors.append(f"ashby[{slug}]: {e}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        checked += 1
        if resp.status_code == 404:
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "not_found")
            continue
        if resp.status_code != 200:
            errors.append(f"ashby[{slug}]: HTTP {resp.status_code}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        try:
            data = resp.json()
        except Exception as e:  # noqa: BLE001
            errors.append(f"ashby[{slug}]: non-JSON response: {e}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        raw_jobs = data.get("jobs", []) or []
        jobs_for_slug = 0
        map_errors_for_slug = 0
        for raw in raw_jobs:
            try:
                j = _map(raw, slug, company_name)
                if j:
                    results.append(j)
                    jobs_for_slug += 1
            except Exception as e:  # noqa: BLE001
                errors.append(f"ashby[{slug}]: map error: {e}")
                map_errors_for_slug += 1
        # R6-C1: see greenhouse.py — guard against parse-failure mass-closure.
        if jobs_for_slug > 0 or (not raw_jobs and map_errors_for_slug == 0):
            successful_slugs.add(slug)
        if conn is not None:
            if jobs_for_slug > 0:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "active", jobs_for_slug)
            elif map_errors_for_slug > 0:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            else:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "empty", 0)
        if i < len(items) - 1:
            time.sleep(delay)

    return results, errors, {"checked": checked, "skipped_cached": skipped,
                             "total_slugs": len(items),
                             "successful_slugs": successful_slugs}
