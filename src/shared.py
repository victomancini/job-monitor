"""Shared utilities: standardized job dict builder, YAML config loader, salary formatting."""
from __future__ import annotations

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


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
    """Return a standardized job dict populated by sources."""
    return {
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
        "source_name": source_name,
        "is_remote": is_remote,
        "work_arrangement": work_arrangement,
        "date_posted": date_posted,
        "raw_data": json.dumps(raw_data) if raw_data is not None else None,
    }


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
]


def validate_required_env() -> list[str]:
    """Return names of required env vars that are missing/empty."""
    return [v for v in REQUIRED_ENV if not env(v)]
