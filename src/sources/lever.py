"""Lever public job board API — per-company slug, no auth.

`https://api.lever.co/v0/postings/{slug}?mode=json`

Returns a flat JSON array of postings. Same cache pattern as Greenhouse.
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import db as dbmod
from src.shared import build_job
from src.sources._http import retry_request

_SALARY_NUM_RE = re.compile(r"\d{1,3}(?:[,.\s]?\d{3})*(?:\.\d+)?")

log = logging.getLogger(__name__)

BASE_URL = "https://api.lever.co/v0/postings/{slug}"
ATS_NAME = "lever"
SLUG_DELAY_SEC = 1.0

DEFAULT_COMPANIES: dict[str, str] = {
    "netflix": "Netflix",
    "figma": "Figma",
    "notion": "Notion",
    "stripe": "Stripe",
    "coinbase": "Coinbase",
    "databricks": "Databricks",
    "ramp": "Ramp",
    "plaid": "Plaid",
    "discord": "Discord",
    "anthropic": "Anthropic",
    "scaleai": "Scale AI",
    "anduril": "Anduril",
    "relativity": "Relativity",
}


def _load_companies_from_config() -> dict[str, str]:
    cfg_path = Path(__file__).resolve().parent.parent.parent / "config" / "ats_companies.json"
    if not cfg_path.exists():
        return {}
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("lever: failed to load %s: %s", cfg_path, e)
        return {}
    return {slug: name for slug, name in (data.get(ATS_NAME) or {}).items()}


def _ms_to_iso(ms: Any) -> str | None:
    try:
        ts = float(ms) / 1000.0
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return None


def _coerce_amount(v: Any) -> float | None:
    """Normalize a salary amount: numbers under 1000 are treated as thousands
    (e.g. `120` → 120k). Filters out nonsense outside [10k, 1M]."""
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    if n < 1000:
        n *= 1000
    if 10_000 <= n <= 1_000_000:
        return n
    return None


def _parse_salary(item: dict[str, Any]) -> tuple[float | None, float | None]:
    """Best-effort salary extraction from Lever's `salaryRange` (structured) or
    `salaryDescription` (text). Returns (min, max) — either may be None."""
    rng = item.get("salaryRange")
    if isinstance(rng, dict):
        lo = _coerce_amount(rng.get("min"))
        hi = _coerce_amount(rng.get("max"))
        if lo or hi:
            return lo, hi
        # Fall back to parsing the text form when present ("$120k – $180k")
        text = rng.get("text")
        if text:
            return _parse_salary_text(text)
    # Lever sometimes surfaces the band only as a description string
    for fld in ("salaryDescription", "salary"):
        text = item.get(fld)
        if isinstance(text, str) and text:
            lo, hi = _parse_salary_text(text)
            if lo or hi:
                return lo, hi
    return None, None


def _parse_salary_text(text: str) -> tuple[float | None, float | None]:
    nums: list[float] = []
    for raw in _SALARY_NUM_RE.findall(text):
        clean = raw.replace(",", "").replace(" ", "")
        # Keep only the integer portion — Lever text rarely has cents, and a
        # value like "120.5k" would confuse the <1000 "thousands" heuristic.
        clean = clean.split(".")[0]
        n = _coerce_amount(clean)
        if n is not None:
            nums.append(n)
    if not nums:
        return None, None
    return min(nums), (max(nums) if len(nums) >= 2 else None)


def _map(item: dict[str, Any], slug: str, company_name: str) -> dict[str, Any] | None:
    jid = item.get("id")
    title = item.get("text") or ""
    if not jid or not title:
        return None
    categories = item.get("categories") or {}
    location = categories.get("location") or ""
    description = item.get("descriptionPlain") or ""
    apply_url = item.get("hostedUrl") or ""
    salary_min, salary_max = _parse_salary(item)
    created_at = item.get("createdAt")
    # R8-M3: see greenhouse.py — don't jam team/department into
    # work_arrangement. Lever's `commitment` ("full-time" / "part-time") is
    # closer to the semantic field but also isn't always reliable; leaving
    # blank keeps the filter surface clean.
    return build_job(
        source_name=ATS_NAME,
        external_id=f"lever_{slug}_{jid}",
        title=title,
        company=company_name,
        location=location,
        description=description,
        source_url=apply_url,
        apply_url=apply_url,
        salary_min=salary_min,
        salary_max=salary_max,
        date_posted=_ms_to_iso(created_at),
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
            resp = retry_request("GET", url, params={"mode": "json"}, max_attempts=2, timeout=20.0)
        except Exception as e:  # noqa: BLE001
            errors.append(f"lever[{slug}]: {e}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        checked += 1
        if resp.status_code == 404:
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "not_found")
            continue
        if resp.status_code != 200:
            errors.append(f"lever[{slug}]: HTTP {resp.status_code}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        try:
            data = resp.json()
        except Exception as e:  # noqa: BLE001
            errors.append(f"lever[{slug}]: non-JSON response: {e}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        # Lever returns a top-level array
        posts = data if isinstance(data, list) else (data.get("data") or [])
        jobs_for_slug = 0
        map_errors_for_slug = 0
        for raw in posts:
            try:
                j = _map(raw, slug, company_name)
                if j:
                    results.append(j)
                    jobs_for_slug += 1
            except Exception as e:  # noqa: BLE001
                errors.append(f"lever[{slug}]: map error: {e}")
                map_errors_for_slug += 1
        # R6-C1: see greenhouse.py — don't mark a slug authoritative when
        # every entry parsed into an error. Otherwise a malformed payload
        # would make lifecycle_checker mass-close existing jobs.
        if jobs_for_slug > 0 or (not posts and map_errors_for_slug == 0):
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
