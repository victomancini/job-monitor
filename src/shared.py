"""Shared utilities: standardized job dict builder, YAML config loader, salary formatting."""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"

# R7: hard cap on the serialized raw_data blob stored per job. Some
# aggregators (notably Adzuna and JSearch) include full HTML descriptions
# easily exceeding 100KB. Turso row limits vary by plan and unbounded growth
# can blow them out silently. When exceeded we replace the payload with a
# marker dict so downstream consumers (shadow log, retry queue) still have
# structured metadata rather than a truncated-mid-token string.
RAW_DATA_MAX_BYTES = 50_000

# Root aggregator domains. Callers use `is_aggregator_host()` which returns
# True for the root AND every subdomain — this catches regional variants like
# `us.jooble.org`, `uk.jooble.org`, `link.adzuna.com`, `de.indeed.com`, which
# the old exact-match `AGGREGATOR_HOSTS` set missed. A job arriving on one of
# those variants previously bypassed the entire redirect-following path in
# enrichment.py because `host in AGGREGATOR_HOSTS` was False.
AGGREGATOR_ROOT_DOMAINS: frozenset[str] = frozenset({
    "jooble.org",
    # All Adzuna country TLDs. Observed live in the 2026-04-20 log:
    # www.adzuna.ca and www.adzuna.com.au were silently flowing through as
    # aggregator=False because only .com/.co.uk were listed. Adzuna operates
    # in 16 countries; list them all to stop the leak.
    "adzuna.com", "adzuna.co.uk", "adzuna.ca", "adzuna.com.au",
    "adzuna.de", "adzuna.fr", "adzuna.nl", "adzuna.it", "adzuna.pl",
    "adzuna.at", "adzuna.ch", "adzuna.ru", "adzuna.in", "adzuna.sg",
    "adzuna.co.za", "adzuna.com.br", "adzuna.com.mx",
    "indeed.com",
    "google.com",
    "linkedin.com",
    "ziprecruiter.com",
    "glassdoor.com",
    # Additional aggregator-ish forwarders observed in production
    "rapidapi.com",
})

# Kept for backward compat: any code still doing `host in AGGREGATOR_HOSTS`
# gets the same hits as the exact-match set. New code should call
# `is_aggregator_host()` instead.
AGGREGATOR_HOSTS: frozenset[str] = frozenset({
    "jooble.org", "www.jooble.org",
    "adzuna.com", "www.adzuna.com", "adzuna.co.uk",
    "indeed.com", "www.indeed.com",
    "google.com", "www.google.com", "jobs.google.com",
    "linkedin.com", "www.linkedin.com",
    "ziprecruiter.com", "www.ziprecruiter.com",
    "glassdoor.com", "www.glassdoor.com",
})


def is_aggregator_host(host: str) -> bool:
    """Return True when `host` matches any aggregator root domain OR is a
    subdomain of one. Case-insensitive. Empty/None → False.

    Examples:
      is_aggregator_host('jooble.org')      → True
      is_aggregator_host('us.jooble.org')   → True
      is_aggregator_host('link.adzuna.com') → True
      is_aggregator_host('careers.acme.com')→ False
    """
    if not host:
        return False
    h = host.lower().strip()
    for root in AGGREGATOR_ROOT_DOMAINS:
        if h == root or h.endswith("." + root):
            return True
    return False


@lru_cache(maxsize=None)
def load_yaml(name: str) -> dict[str, Any]:
    """Load config/<name>. Cached per-process."""
    path = CONFIG_DIR / name
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_keywords() -> dict[str, Any]:
    return load_yaml("keywords.yaml")


def load_queries() -> dict[str, Any]:
    return load_yaml("queries.yaml")


def load_companies() -> dict[str, Any]:
    return load_yaml("companies.yaml")


# R11 Phase 1: source reliability priors. Drives consensus voting in
# processors/deduplicator.py (R11 Phase 3). Canonical ATS APIs are high-
# confidence — they ARE the company's system of record. Aggregators vary:
# Jooble/Adzuna tag `is_remote` loosely, JSearch's remote flag fires on any
# mention of "remote" in the description. The text classifier (Phase 2) is
# deterministic but conservative. Schema.org JobPosting extraction (Phase 4)
# when available is near-canonical. Fresh calibration will come from shadow-
# log labels; these are starter priors.
SOURCE_RELIABILITY: dict[str, float] = {
    "greenhouse": 0.90,
    "lever": 0.90,
    "ashby": 0.90,
    "usajobs": 0.90,
    "siop": 0.80,
    "onemodel": 0.80,
    "included_ai": 0.75,
    "schema_org": 0.85,
    "text_classifier": 0.75,
    "jobspy_linkedin": 0.60,
    "jobspy_indeed": 0.60,
    "jobspy_glassdoor": 0.60,
    "jobspy_zip_recruiter": 0.55,
    "jsearch": 0.55,
    "adzuna": 0.55,
    "jooble": 0.50,
    "google_alerts": 0.40,
}

# Fields that carry enough signal-fusion value to merit provenance tracking.
# Pure text fields like title/company/description aren't tracked — they're
# either the same across sources (dedup handles variance) or too long to
# vote on. Focus on the ones that affect downstream filtering.
PROVENANCE_FIELDS: tuple[str, ...] = (
    "is_remote",
    "work_arrangement",
    "location",
    "location_country",
    "salary_min",
    "salary_max",
    "date_posted",
)


def source_reliability(source_name: str) -> float:
    return SOURCE_RELIABILITY.get(source_name, 0.50)


def record_field(
    job: dict[str, Any],
    field: str,
    *,
    source: str,
    confidence: float | None = None,
) -> None:
    """Attach a provenance entry for a field the job dict already carries.

    Reads the value from job[field] (so build_job's formatting / coercion
    applies once), then appends a {source, value, confidence} observation to
    job["_field_sources"][field]. Multiple sources observing the same job
    accumulate observations — consensus voting in Phase 3 reads the history.

    No-op when the field is None/empty or the sentinel 'unknown' — a source
    saying "I don't know" shouldn't count as a vote against whoever does.
    """
    value = job.get(field)
    if value is None or value == "" or value == "unknown":
        return
    if confidence is None:
        confidence = source_reliability(source)
    fs = job.setdefault("_field_sources", {})
    fs.setdefault(field, []).append({
        "source": source,
        "value": value,
        "confidence": confidence,
    })


def apply_provenance(job: dict[str, Any], source_name: str) -> None:
    """Record provenance for every meaningful field the given source set.
    Called automatically by build_job; manually by sources that construct
    job dicts outside build_job (none today, but the hook exists)."""
    for f in PROVENANCE_FIELDS:
        record_field(job, f, source=source_name)


def days_since_posted(
    date_posted: str | None,
    first_seen_date: str | None,
) -> int | None:
    """R11 Phase 0: integer days since a job was first observable.

    Prefers `date_posted` (what the company says) over `first_seen_date` (when
    we first saw it). Returns None when neither is a parseable YYYY-MM-DD.

    Moved from PHP to Python so the freshness sort in WordPress doesn't
    re-parse dates with the site's local timezone and drift off UTC. The WP
    plugin reads the integer and uses it as both data-order and display.
    """
    ref = date_posted or first_seen_date
    if not ref:
        return None
    try:
        ref_dt = datetime.strptime(ref[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    now = datetime.now(timezone.utc)
    delta = (now - ref_dt).days
    return max(0, delta)


def format_salary_range(salary_min: float | None, salary_max: float | None) -> str | None:
    """Format as '$120K-$180K', '$120K+', 'Up to $180K', or None."""
    def fmt(v: float) -> str:
        return f"${int(round(v / 1000))}K"
    if salary_min and salary_max:
        return f"{fmt(salary_min)}-{fmt(salary_max)}"
    if salary_min:
        return f"{fmt(salary_min)}+"
    if salary_max:
        return f"Up to {fmt(salary_max)}"
    return None


def build_job(
    *,
    source_name: str,
    external_id: str,
    title: str,
    company: str,
    source_url: str,
    apply_url: str = "",
    location: str = "",
    location_country: str = "",
    description: str = "",
    description_is_snippet: bool = False,
    salary_min: float | None = None,
    salary_max: float | None = None,
    is_remote: str = "unknown",
    work_arrangement: str = "",
    date_posted: str | None = None,
    raw_data: Any = None,
) -> dict[str, Any]:
    """Return a standardized job dict populated by sources.

    `apply_url` — best direct application link. Falls back to `source_url` if empty."""
    job = {
        "external_id": external_id,
        "title": (title or "").strip(),
        "company": (company or "").strip(),
        "location": location,
        "location_country": location_country,
        "description": description or "",
        "description_is_snippet": description_is_snippet,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_range": format_salary_range(salary_min, salary_max),
        "source_url": source_url,
        "apply_url": apply_url or source_url,
        "source_name": source_name,
        "is_remote": is_remote,
        "work_arrangement": work_arrangement,
        "date_posted": date_posted,
        "raw_data": _serialize_raw_data(raw_data),
    }
    # R11 Phase 1: record per-field provenance for the signal-fusion pipeline
    # (consensus voting in R11 Phase 3 reads this). No-op for fields the
    # source left at None/'unknown'.
    apply_provenance(job, source_name)
    return job


def _serialize_raw_data(raw: Any) -> str | None:
    """Serialize a source's raw payload to JSON, capping size at
    RAW_DATA_MAX_BYTES. Oversized payloads are replaced with a structured
    marker so the column still carries diagnostic info without corrupt JSON."""
    if raw is None:
        return None
    serialized = json.dumps(raw)
    size = len(serialized.encode("utf-8"))
    if size <= RAW_DATA_MAX_BYTES:
        return serialized
    # Try to keep a hint of what was in the payload for debugging. Top-level
    # keys usually include {id, title, company}; 200 chars is enough to show
    # those without risking re-exceeding the cap.
    preview: str = ""
    try:
        if isinstance(raw, dict):
            preview = json.dumps({k: str(raw.get(k))[:100] for k in list(raw.keys())[:5]})[:400]
    except Exception:  # noqa: BLE001
        preview = ""
    return json.dumps({
        "_truncated": True,
        "_original_bytes": size,
        "_preview": preview,
    })


def env(name: str, default: str = "") -> str:
    """Trimmed env var; empty string if missing."""
    return (os.environ.get(name) or default).strip()


REQUIRED_ENV = [
    "JSEARCH_API_KEY", "JOOBLE_API_KEY", "GROQ_API_KEY", "GEMINI_API_KEY",
    "TURSO_DB_URL", "TURSO_AUTH_TOKEN",
    "WP_URL", "WP_USERNAME", "WP_APP_PASSWORD",
    "HEALTHCHECK_URL",
]

OPTIONAL_ENV = [
    "ADZUNA_APP_ID", "ADZUNA_APP_KEY",
    "USAJOBS_EMAIL", "USAJOBS_API_KEY",
    "OPENAI_API_KEY",
    "BREVO_SMTP_USER", "BREVO_SMTP_PASS", "NOTIFICATION_EMAIL",
    "PUSHOVER_USER_KEY", "PUSHOVER_APP_TOKEN",
    "GOOGLE_ALERT_RSS_1", "GOOGLE_ALERT_RSS_2", "GOOGLE_ALERT_RSS_3",
    "GOOGLE_ALERT_RSS_4", "GOOGLE_ALERT_RSS_5", "GOOGLE_ALERT_SIOP",
    "TALKWALKER_RSS_1", "TALKWALKER_RSS_2", "TALKWALKER_RSS_3",
    # Optional defense-in-depth: sent as X-JM-Secret to the WP REST endpoint
    # when set. Matches JM_SHARED_SECRET defined in wp-config.php.
    "WP_SHARED_SECRET",
]


def validate_required_env() -> list[str]:
    """Return names of required env vars that are missing/empty."""
    return [v for v in REQUIRED_ENV if not env(v)]


def validate_env_scheme() -> list[str]:
    """R7-C: refuse to run when WP_URL (or HEALTHCHECK_URL) is HTTP rather than
    HTTPS. Basic Auth app-passwords and the X-JM-Secret header would otherwise
    cross the wire in cleartext every batch. Returns a list of violating var
    names so the caller can fail loudly at pre-flight."""
    violations: list[str] = []
    wp_url = env("WP_URL")
    if wp_url and not wp_url.lower().startswith("https://"):
        violations.append("WP_URL")
    # HEALTHCHECK_URL leaks run metadata (source counts, error snippets) if
    # served over HTTP. Healthchecks.io itself is HTTPS — so this mainly
    # guards against accidental self-hosted misconfig.
    hc_url = env("HEALTHCHECK_URL")
    if hc_url and not hc_url.lower().startswith("https://"):
        violations.append("HEALTHCHECK_URL")
    return violations
