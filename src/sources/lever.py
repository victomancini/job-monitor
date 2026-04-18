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


def _map(item: dict[str, Any], slug: str, company_name: str) -> dict[str, Any] | None:
    jid = item.get("id")
    title = item.get("text") or ""
    if not jid or not title:
        return None
    categories = item.get("categories") or {}
    location = categories.get("location") or ""
    team = categories.get("team") or categories.get("department") or ""
    description = item.get("descriptionPlain") or ""
    apply_url = item.get("hostedUrl") or ""
    # Lever exposes salary as plaintext in `salaryRange` sometimes — best-effort
    salary_range = (item.get("salaryRange") or {}).get("text") if isinstance(item.get("salaryRange"), dict) else None
    created_at = item.get("createdAt")
    return build_job(
        source_name=ATS_NAME,
        external_id=f"lever_{slug}_{jid}",
        title=title,
        company=company_name,
        location=location,
        description=description,
        source_url=apply_url,
        apply_url=apply_url,
        date_posted=_ms_to_iso(created_at),
        work_arrangement=team,
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
        for raw in posts:
            try:
                j = _map(raw, slug, company_name)
                if j:
                    results.append(j)
                    jobs_for_slug += 1
            except Exception as e:  # noqa: BLE001
                errors.append(f"lever[{slug}]: map error: {e}")
        if conn is not None:
            dbmod.set_ats_status(
                conn, ATS_NAME, slug,
                "active" if jobs_for_slug else "empty",
                jobs_for_slug,
            )
        if i < len(items) - 1:
            time.sleep(delay)

    return results, errors, {"checked": checked, "skipped_cached": skipped,
                             "total_slugs": len(items)}
