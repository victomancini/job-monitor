"""Jooble API source — US-primary, bonus UK/CA/AU. POST endpoint. Returns snippets."""
from __future__ import annotations

import logging
import time
from typing import Any

from src.shared import build_job, load_queries
from src.sources._http import retry_request

log = logging.getLogger(__name__)

BASE_URL = "https://jooble.org/api"

_COUNTRY_MAP = {
    "United States": "US",
    "United Kingdom": "GB",
    "Canada": "CA",
    "Australia": "AU",
}


def _parse_salary(text: str | None) -> tuple[float | None, float | None]:
    """Best-effort numeric range parsing. Jooble's salary field is a free-text string."""
    if not text:
        return None, None
    import re
    nums = re.findall(r"\d{1,3}(?:[,.\s]?\d{3})*", text)
    vals: list[float] = []
    for n in nums:
        clean = n.replace(",", "").replace(" ", "").replace(".", "")
        try:
            v = float(clean)
        except ValueError:
            continue
        if v < 1000:
            v *= 1000  # "120" → 120K
        if 10_000 <= v <= 1_000_000:
            vals.append(v)
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], None
    return min(vals), max(vals)


def _map(raw: dict[str, Any], country_label: str) -> dict[str, Any] | None:
    jid = raw.get("id")
    if not jid:
        return None
    title = raw.get("title") or ""
    company = raw.get("company") or ""
    if not title:
        return None
    salary_min, salary_max = _parse_salary(raw.get("salary"))
    link = raw.get("link") or ""
    # Jooble only exposes its own redirect URL; apply_url == source_url for this source.
    job = build_job(
        source_name="jooble",
        external_id=f"jooble_{jid}",
        title=title,
        company=company,
        location=raw.get("location") or "",
        location_country=_COUNTRY_MAP.get(country_label, ""),
        description=raw.get("snippet") or "",
        description_is_snippet=True,
        salary_min=salary_min,
        salary_max=salary_max,
        source_url=link,
        apply_url=link,
        date_posted=raw.get("updated"),
        raw_data=raw,
    )
    # Phase A: Jooble's `link` is always an aggregator redirect; the enrichment
    # pass should prefer whatever URL it resolves to after following redirects.
    job["_apply_url_is_redirect"] = True
    return job


def fetch(api_key: str, queries: list[dict[str, Any]] | None = None) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    if not api_key:
        return [], ["jooble: JOOBLE_API_KEY not set"], {}
    queries = queries if queries is not None else load_queries()["jooble"]["queries"]
    url = f"{BASE_URL}/{api_key}"
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for i, q in enumerate(queries):
        body = {"keywords": q["keywords"], "location": q.get("location", "United States"), "page": "1"}
        if "radius" in q:
            body["radius"] = str(q["radius"])
        try:
            resp = retry_request("POST", url, json=body)
            if resp.status_code != 200:
                errors.append(f"jooble: HTTP {resp.status_code} on '{q['keywords'][:40]}'")
                continue
            data = resp.json()
            for raw in data.get("jobs", []) or []:
                try:
                    job = _map(raw, body["location"])
                    if job:
                        results.append(job)
                except Exception as e:  # noqa: BLE001
                    errors.append(f"jooble: map error: {e}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"jooble: query error '{q['keywords'][:40]}': {e}")
        if i < len(queries) - 1:
            time.sleep(1.0)
    return results, errors, {}
