"""Greenhouse public job board API — per-company slug, no auth.

`https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true`

Returns all currently-posted roles for a company. We fetch per slug, apply
the downstream keyword filter like any other source, and cache 404 slugs for
30 days in `ats_company_status` so we don't re-check dead boards daily.
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

BASE_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
ATS_NAME = "greenhouse"
SLUG_DELAY_SEC = 1.0

# Built-in defaults; see `config/ats_companies.json` for the validated list.
DEFAULT_COMPANIES: dict[str, str] = {
    # PA/EL vendors
    "cultureamp": "Culture Amp",
    "lattice": "Lattice",
    "visier": "Visier",
    "peakon": "Workday (Peakon)",
    "perceptyx": "Perceptyx",
    "qualtrics": "Qualtrics",
    "medallia": "Medallia",
    "confirmit": "Forsta (Confirmit)",
    "glintinc": "Glint (LinkedIn)",
    "surveymonkey": "Momentive (SurveyMonkey)",
    "quantumworkplace": "Quantum Workplace",
    "tinypulse": "TINYpulse",
    "15five": "15Five",
    "betterworks": "BetterWorks",
    "humu": "Humu",
    "orgnostic": "Orgnostic",
    # Big tech
    "airbnb": "Airbnb",
    "pinterest": "Pinterest",
    "lyft": "Lyft",
    "stripe": "Stripe",
    "coinbase": "Coinbase",
    "doordash": "DoorDash",
    "instacart": "Instacart",
    "databricks": "Databricks",
    "figma": "Figma",
    "notion": "Notion",
    "airtable": "Airtable",
    "canva": "Canva",
    "plaid": "Plaid",
    "ramp": "Ramp",
    "brex": "Brex",
    "gusto": "Gusto",
    "rippling": "Rippling",
    "deel": "Deel",
    "remotecom": "Remote.com",
    "justworks": "Justworks",
    "duolingo": "Duolingo",
    "discord": "Discord",
    "snap": "Snap",
    "spotify": "Spotify",
    "netflix": "Netflix",
    "squarespace": "Squarespace",
    "etsy": "Etsy",
    "wayfair": "Wayfair",
    "hubspot": "HubSpot",
    "twilio": "Twilio",
    "zoom": "Zoom",
    "dropbox": "Dropbox",
    "asana": "Asana",
    "atlassian": "Atlassian",
    "elastic": "Elastic",
    "hashicorp": "HashiCorp",
    "datadog": "Datadog",
    "pagerduty": "PagerDuty",
    "newrelic": "New Relic",
    "okta": "Okta",
    "cloudflare": "Cloudflare",
    "mongodb": "MongoDB",
    "confluent": "Confluent",
    "supabase": "Supabase",
    "vercel": "Vercel",
    "anthropic": "Anthropic",
    "openai": "OpenAI",
    # Consulting
    "mckinsey": "McKinsey",
    "bcg": "BCG",
    "bain": "Bain",
    "kincentric": "Kincentric",
    # Financial
    "goldmansachs": "Goldman Sachs",
    "jpmorgan": "JPMorgan Chase",
    "blackstone": "Blackstone",
    "citadel": "Citadel",
    "twosigsigma": "Two Sigma",
    "deshaw": "D.E. Shaw",
    # Healthcare
    "regeneron": "Regeneron",
    "modernatx": "Moderna",
    # Large employers / F500
    "walmart": "Walmart",
    "target": "Target",
    "nike": "Nike",
    "starbucks": "Starbucks",
    "disney": "Disney",
    "comcast": "Comcast (NBCUniversal)",
    "verizon": "Verizon",
    "mastercard": "Mastercard",
    "visa": "Visa",
    "amex": "American Express",
    "fidelity": "Fidelity",
    "capitalone": "Capital One",
    "schwab": "Charles Schwab",
    "wellsfargo": "Wells Fargo",
    "citi": "Citi",
    "bankofamerica": "Bank of America",
    "jnj": "Johnson & Johnson",
    "pfizer": "Pfizer",
    "merck": "Merck",
    "abbvie": "AbbVie",
    "unitedhealth": "UnitedHealth Group",
    "cvs": "CVS Health",
    "kaiserpermanente": "Kaiser Permanente",
    "mayo": "Mayo Clinic",
    "ge": "GE",
    "3m": "3M",
    "boeing": "Boeing",
    "lockheedmartin": "Lockheed Martin",
    "raytheon": "Raytheon",
    "deloitte": "Deloitte",
    "ey": "EY",
    "pwc": "PwC",
    "kpmg": "KPMG",
    "accenture": "Accenture",
    "mercer": "Mercer",
    "kornferry": "Korn Ferry",
    "wtwco": "WTW",
    "aon": "Aon",
    # NYC metro
    "macys": "Macy's",
    "esteelauder": "Estee Lauder",
    "loreal": "L'Oreal",
    "marshmclennan": "Marsh McLennan",
    "bloomberg": "Bloomberg",
    "blackrock": "BlackRock",
    # PA-mature
    "microsoft": "Microsoft",
    "google": "Google",
    "meta": "Meta",
    "amazon": "Amazon",
    "apple": "Apple",
    "salesforce": "Salesforce",
    "adobe": "Adobe",
    "intuit": "Intuit",
    "workday": "Workday",
    "servicenow": "ServiceNow",
    "snowflake": "Snowflake",
    "palantir": "Palantir",
    "uber": "Uber",
}


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    return _WS_RE.sub(" ", _HTML_TAG_RE.sub(" ", html)).strip()


def _load_companies_from_config() -> dict[str, str]:
    """Load validated slugs from config/ats_companies.json if present."""
    cfg_path = Path(__file__).resolve().parent.parent.parent / "config" / "ats_companies.json"
    if not cfg_path.exists():
        return {}
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("greenhouse: failed to load %s: %s", cfg_path, e)
        return {}
    return {slug: name for slug, name in (data.get(ATS_NAME) or {}).items()}


def _map(item: dict[str, Any], slug: str, company_name: str) -> dict[str, Any] | None:
    jid = item.get("id")
    title = item.get("title") or ""
    if not jid or not title:
        return None
    loc = (item.get("location") or {}).get("name") or ""
    depts = item.get("departments") or []
    department = depts[0].get("name") if depts else ""
    description_html = item.get("content") or ""
    description = _html_to_text(description_html)
    apply_url = item.get("absolute_url") or ""
    job = build_job(
        source_name=ATS_NAME,
        external_id=f"gh_{slug}_{jid}",
        title=title,
        company=company_name,
        location=loc,
        description=description,
        source_url=apply_url,
        apply_url=apply_url,
        date_posted=item.get("updated_at"),
        work_arrangement=department,
        raw_data=item,
    )
    return job


def fetch(
    conn=None,
    companies: dict[str, str] | None = None,
    *,
    delay: float = SLUG_DELAY_SEC,
) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Fetch all Greenhouse boards for known companies.

    `conn`      — optional Turso connection; enables 30-day not_found caching.
    `companies` — {slug: display_name}. Defaults to config/ats_companies.json
                  then DEFAULT_COMPANIES.
    """
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
            resp = retry_request("GET", url, params={"content": "true"}, max_attempts=2, timeout=20.0)
        except Exception as e:  # noqa: BLE001
            errors.append(f"greenhouse[{slug}]: {e}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        checked += 1
        if resp.status_code == 404:
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "not_found")
            continue
        if resp.status_code != 200:
            errors.append(f"greenhouse[{slug}]: HTTP {resp.status_code}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        try:
            data = resp.json()
        except Exception as e:  # noqa: BLE001
            errors.append(f"greenhouse[{slug}]: non-JSON response: {e}")
            if conn is not None:
                dbmod.set_ats_status(conn, ATS_NAME, slug, "error")
            continue
        jobs_for_slug = 0
        for raw in data.get("jobs", []) or []:
            try:
                j = _map(raw, slug, company_name)
                if j:
                    results.append(j)
                    jobs_for_slug += 1
            except Exception as e:  # noqa: BLE001
                errors.append(f"greenhouse[{slug}]: map error: {e}")
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
