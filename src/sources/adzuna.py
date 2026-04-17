"""Adzuna API source — US-primary, bonus gb/ca/au."""
from __future__ import annotations

import logging
import time
from typing import Any

from src.shared import build_job, load_queries
from src.sources._http import retry_request

log = logging.getLogger(__name__)

BASE_URL = "https://api.adzuna.com/v1/api/jobs/{country}/search/1"
_COUNTRY_TO_CODE = {"us": "US", "gb": "GB", "ca": "CA", "au": "AU"}


def _map(raw: dict[str, Any], country: str) -> dict[str, Any] | None:
    jid = raw.get("id")
    if not jid:
        return None
    title = raw.get("title") or ""
    company = (raw.get("company") or {}).get("display_name") or ""
    if not title or not company:
        return None
    location = (raw.get("location") or {}).get("display_name") or ""
    redirect = raw.get("redirect_url") or ""
    job = build_job(
        source_name="adzuna",
        external_id=f"adzuna_{jid}",
        title=title,
        company=company,
        location=location,
        location_country=_COUNTRY_TO_CODE.get(country, country.upper()),
        description=raw.get("description") or "",
        salary_min=raw.get("salary_min"),
        salary_max=raw.get("salary_max"),
        source_url=redirect,
        apply_url=redirect,
        date_posted=raw.get("created"),
        raw_data=raw,
    )
    # Phase A: Adzuna `redirect_url` always goes through its own tracker — mark
    # so enrichment prefers the final URL after following redirects.
    job["_apply_url_is_redirect"] = True
    return job


def fetch(
    app_id: str,
    app_key: str,
    queries: list[dict[str, Any]] | None = None,
    countries: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    if not app_id or not app_key:
        return [], ["adzuna: credentials not set"], {}
    cfg = load_queries()["adzuna"]
    queries = queries if queries is not None else cfg["queries"]
    if countries is None:
        countries = [cfg["primary_country"]] + list(cfg.get("bonus_countries", []))
    params_base = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": cfg.get("params", {}).get("results_per_page", 20),
        "sort_by": cfg.get("params", {}).get("sort_by", "date"),
    }
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for c_idx, country in enumerate(countries):
        url = BASE_URL.format(country=country)
        for q_idx, q in enumerate(queries):
            params = {**params_base, "what": q["what"]}
            try:
                resp = retry_request("GET", url, params=params)
                if resp.status_code != 200:
                    errors.append(f"adzuna[{country}]: HTTP {resp.status_code} on '{q['what']}'")
                    continue
                data = resp.json()
                for raw in data.get("results", []) or []:
                    try:
                        job = _map(raw, country)
                        if job:
                            results.append(job)
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"adzuna[{country}]: map error: {e}")
            except Exception as e:  # noqa: BLE001
                errors.append(f"adzuna[{country}]: query error '{q['what']}': {e}")
            # Sleep between queries, not after last overall
            is_last = (c_idx == len(countries) - 1) and (q_idx == len(queries) - 1)
            if not is_last:
                time.sleep(1.0)
    return results, errors, {}
