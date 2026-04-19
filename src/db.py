"""Turso libSQL database layer: connection, migration, query helpers."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d")
from typing import Any, Iterable

try:
    import libsql  # type: ignore
except ImportError:  # pragma: no cover - libsql not installable locally on py3.14
    libsql = None  # type: ignore

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    company_normalized TEXT,
    location TEXT,
    location_country TEXT,
    work_arrangement TEXT,
    description TEXT,
    description_snippet TEXT,
    salary_min REAL,
    salary_max REAL,
    salary_range TEXT,
    source_url TEXT,
    apply_url TEXT,
    source_name TEXT NOT NULL,
    is_remote TEXT DEFAULT 'unknown',
    category TEXT,
    seniority TEXT,
    seniority_confidence TEXT,
    lifecycle_status TEXT DEFAULT 'active',
    keyword_score INTEGER DEFAULT 0,
    keywords_matched TEXT,
    llm_classification TEXT,
    llm_confidence INTEGER,
    llm_provider TEXT,
    llm_reasoning TEXT,
    fit_score INTEGER DEFAULT 0,
    location_confidence TEXT DEFAULT 'unverified',
    salary_confidence TEXT DEFAULT 'unverified',
    remote_confidence TEXT DEFAULT 'unverified',
    enrichment_source TEXT,
    enrichment_date TEXT,
    vendors_mentioned TEXT,
    date_posted TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1,
    archived_date TEXT,
    days_active INTEGER,
    wp_post_id INTEGER,
    raw_data TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_external_id ON jobs(external_id);
CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_normalized);
CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen_date);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    jsearch_found INTEGER DEFAULT 0,
    jooble_found INTEGER DEFAULT 0,
    adzuna_found INTEGER DEFAULT 0,
    usajobs_found INTEGER DEFAULT 0,
    alerts_found INTEGER DEFAULT 0,
    total_passed_filter INTEGER DEFAULT 0,
    total_published INTEGER DEFAULT 0,
    total_archived INTEGER DEFAULT 0,
    errors TEXT,
    llm_provider_used TEXT,
    duration_seconds REAL,
    consecutive_zero_runs INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS retry_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_json TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    last_attempt TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Phase 7 (R3): daily stats snapshot, grouped by stat_type.
CREATE TABLE IF NOT EXISTS monthly_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stat_date TEXT NOT NULL,
    stat_type TEXT NOT NULL,
    stat_key TEXT NOT NULL,
    stat_value INTEGER NOT NULL,
    UNIQUE(stat_date, stat_type, stat_key)
);
CREATE INDEX IF NOT EXISTS idx_monthly_stats ON monthly_stats(stat_date, stat_type);

-- Phase 2 (R3): ATS slug cache — avoids re-fetching known-404 slugs daily.
CREATE TABLE IF NOT EXISTS ats_company_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ats TEXT NOT NULL,
    slug TEXT NOT NULL,
    status TEXT NOT NULL,
    last_checked TEXT NOT NULL,
    jobs_found INTEGER DEFAULT 0,
    UNIQUE(ats, slug)
);
CREATE INDEX IF NOT EXISTS idx_ats_status ON ats_company_status(ats, slug);
"""

_JOB_COLUMNS = [
    "external_id", "title", "company", "company_normalized", "location",
    "location_country", "work_arrangement", "description", "description_snippet",
    "salary_min", "salary_max", "salary_range", "source_url", "apply_url",
    "source_name", "is_remote", "category", "seniority", "seniority_confidence",
    "lifecycle_status",
    "keyword_score", "keywords_matched",
    "llm_classification", "llm_confidence", "llm_provider", "llm_reasoning",
    "fit_score",
    "location_confidence", "salary_confidence", "remote_confidence",
    "enrichment_source", "enrichment_date",
    "vendors_mentioned",
    "date_posted", "first_seen_date", "last_seen_date",
    "is_active", "wp_post_id", "raw_data",
]

# Phase A: idempotent column migrations. Each tuple is (column_name, DDL fragment
# appended after ADD COLUMN). Re-running is safe — duplicate-column errors are
# caught silently.
_ADD_COLUMN_MIGRATIONS = [
    ("apply_url", "apply_url TEXT"),
    ("location_confidence", "location_confidence TEXT DEFAULT 'unverified'"),
    ("salary_confidence", "salary_confidence TEXT DEFAULT 'unverified'"),
    ("remote_confidence", "remote_confidence TEXT DEFAULT 'unverified'"),
    ("enrichment_source", "enrichment_source TEXT"),
    ("enrichment_date", "enrichment_date TEXT"),
    # Phase F (R2): seniority-level confidence (for salary-inferred seniority)
    ("seniority_confidence", "seniority_confidence TEXT"),
    # Phase 5 (R3): comma-separated vendor/tool mentions from description text
    ("vendors_mentioned", "vendors_mentioned TEXT"),
    # Phase 6 (R3): lifecycle state machine — 'active' | 'likely_closed'
    ("lifecycle_status", "lifecycle_status TEXT DEFAULT 'active'"),
]


def connect(url: str | None = None, auth_token: str | None = None):
    """Connect to Turso libSQL. Falls back to env vars if args omitted."""
    if libsql is None:
        raise RuntimeError("libsql is not installed in this environment")
    url = url or os.environ.get("TURSO_DB_URL", "")
    auth_token = auth_token or os.environ.get("TURSO_AUTH_TOKEN", "")
    if not url:
        raise ValueError("TURSO_DB_URL is required")
    return libsql.connect(url, auth_token=auth_token)


def migrate(conn) -> None:
    """Apply schema (idempotent). Runs CREATE TABLE IF NOT EXISTS for the base
    schema, then ALTER TABLE ADD COLUMN for each Phase A column so databases
    that pre-date the new columns get upgraded on next boot.

    SQLite has no `ADD COLUMN IF NOT EXISTS`, so duplicate-column errors are
    swallowed."""
    for stmt in SCHEMA.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.commit()

    for _col, ddl in _ADD_COLUMN_MIGRATIONS:
        try:
            conn.execute(f"ALTER TABLE jobs ADD COLUMN {ddl}")
            conn.commit()
        except Exception as e:  # noqa: BLE001 — SQLite/libsql raise different types
            msg = str(e).lower()
            if "duplicate column" in msg or "already exists" in msg:
                continue
            raise


def upsert_job(conn, job: dict[str, Any]) -> str:
    """Insert or update a job by external_id. Returns 'created' or 'updated'."""
    today = _today()
    row = conn.execute(
        "SELECT id FROM jobs WHERE external_id = ?", (job["external_id"],)
    ).fetchone()

    values = {col: job.get(col) for col in _JOB_COLUMNS}
    values["last_seen_date"] = today
    if values.get("is_active") is None:
        values["is_active"] = 1
    if isinstance(values.get("raw_data"), (dict, list)):
        values["raw_data"] = json.dumps(values["raw_data"])

    if row is None:
        values["first_seen_date"] = today
        # Drop None values so schema DEFAULTs apply (is_remote='unknown',
        # *_confidence='unverified', etc.).
        insert_vals = {k: v for k, v in values.items() if v is not None}
        cols = ", ".join(insert_vals.keys())
        placeholders = ", ".join(["?"] * len(insert_vals))
        conn.execute(
            f"INSERT INTO jobs ({cols}) VALUES ({placeholders})",
            tuple(insert_vals.values()),
        )
        conn.commit()
        return "created"

    # Phase 6 (R3): re-seeing a job proves it's still active, even if the source
    # adapter didn't set lifecycle_status explicitly. Force it back to 'active'
    # so 'likely_closed' rows recover on re-appearance.
    values["lifecycle_status"] = "active"
    update_cols = [c for c in values.keys() if c != "first_seen_date"]
    set_clause = ", ".join(f"{c} = ?" for c in update_cols) + ", updated_at = datetime('now')"
    params = [values[c] for c in update_cols] + [job["external_id"]]
    conn.execute(f"UPDATE jobs SET {set_clause} WHERE external_id = ?", params)
    conn.commit()
    return "updated"


def set_wp_post_id(conn, external_id: str, wp_post_id: int) -> None:
    conn.execute(
        "UPDATE jobs SET wp_post_id = ?, updated_at = datetime('now') WHERE external_id = ?",
        (wp_post_id, external_id),
    )
    conn.commit()


def get_active_jobs_for_dedup(conn) -> list[dict[str, Any]]:
    """Return active jobs as dicts for deduplication comparison."""
    cur = conn.execute(
        "SELECT external_id, title, company, company_normalized, location "
        "FROM jobs WHERE is_active = 1"
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_jobs_to_archive(conn, days: int = 7) -> list[dict[str, Any]]:
    """Active jobs unseen for `days` days — candidates for full archival
    (is_active=0). Caller is archiver.archive_stale()."""
    cutoff = _days_ago(days)
    cur = conn.execute(
        "SELECT id, external_id, wp_post_id, first_seen_date, last_seen_date "
        "FROM jobs WHERE is_active = 1 AND last_seen_date < ? LIMIT 100",
        (cutoff,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def mark_job_likely_closed(conn, external_id: str) -> None:
    """Phase 6 (R3): intermediate lifecycle state before full archive."""
    conn.execute(
        "UPDATE jobs SET lifecycle_status='likely_closed', "
        "updated_at = datetime('now') WHERE external_id = ?",
        (external_id,),
    )
    conn.commit()


def get_jobs_to_mark_likely_closed(conn, days: int = 7) -> list[dict[str, Any]]:
    """Active jobs unseen for `days` days that are still lifecycle_status='active'.
    Used by the Phase 6 staleness pass — a step before full archive. Rows whose
    lifecycle_status is already 'likely_closed' are intentionally excluded so
    we don't re-mark them on every run."""
    cutoff = _days_ago(days)
    cur = conn.execute(
        "SELECT id, external_id, wp_post_id, first_seen_date, last_seen_date "
        "FROM jobs WHERE is_active = 1 AND "
        "(lifecycle_status = 'active' OR lifecycle_status IS NULL) "
        "AND last_seen_date < ? LIMIT 100",
        (cutoff,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def archive_job(conn, external_id: str, days_active: int) -> None:
    today = _today()
    conn.execute(
        "UPDATE jobs SET is_active = 0, archived_date = ?, days_active = ?, "
        "updated_at = datetime('now') WHERE external_id = ?",
        (today, days_active, external_id),
    )
    conn.commit()


def enqueue_retry(conn, job: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO retry_queue (job_json) VALUES (?)",
        (json.dumps(job),),
    )
    conn.commit()


def fetch_retry_queue(conn, max_attempts: int = 3) -> list[tuple[int, dict[str, Any]]]:
    cur = conn.execute(
        "SELECT id, job_json FROM retry_queue WHERE attempts < ?",
        (max_attempts,),
    )
    return [(row[0], json.loads(row[1])) for row in cur.fetchall()]


def mark_retry_success(conn, retry_id: int) -> None:
    conn.execute("DELETE FROM retry_queue WHERE id = ?", (retry_id,))
    conn.commit()


def mark_retry_failure(conn, retry_id: int) -> None:
    conn.execute(
        "UPDATE retry_queue SET attempts = attempts + 1, last_attempt = datetime('now') WHERE id = ?",
        (retry_id,),
    )
    conn.commit()


def drop_exhausted_retries(conn, max_attempts: int = 3) -> int:
    cur = conn.execute(
        "DELETE FROM retry_queue WHERE attempts >= ?", (max_attempts,)
    )
    conn.commit()
    return cur.rowcount if hasattr(cur, "rowcount") else 0


def log_run(conn, stats: dict[str, Any]) -> None:
    """Insert a row into run_log. `stats` keys map to columns."""
    cols = [
        "run_date", "jsearch_found", "jooble_found", "adzuna_found",
        "usajobs_found", "alerts_found", "total_passed_filter",
        "total_published", "total_archived", "errors", "llm_provider_used",
        "duration_seconds", "consecutive_zero_runs",
    ]
    values = [stats.get(c) for c in cols]
    placeholders = ", ".join(["?"] * len(cols))
    conn.execute(
        f"INSERT INTO run_log ({', '.join(cols)}) VALUES ({placeholders})",
        values,
    )
    conn.commit()


# ────────────── Phase 2 (R3): ATS slug cache helpers ──────────────

def get_ats_status(conn, ats: str, slug: str) -> dict[str, Any] | None:
    """Return {'status', 'last_checked', 'jobs_found'} for an ATS/slug, or None."""
    row = conn.execute(
        "SELECT status, last_checked, jobs_found FROM ats_company_status WHERE ats=? AND slug=?",
        (ats, slug),
    ).fetchone()
    if not row:
        return None
    return {"status": row[0], "last_checked": row[1], "jobs_found": row[2] or 0}


def set_ats_status(conn, ats: str, slug: str, status: str, jobs_found: int = 0) -> None:
    """Upsert the cache row for (ats, slug) with today's date."""
    today = _today()
    conn.execute(
        "INSERT INTO ats_company_status (ats, slug, status, last_checked, jobs_found) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(ats, slug) DO UPDATE SET "
        "status=excluded.status, last_checked=excluded.last_checked, jobs_found=excluded.jobs_found",
        (ats, slug, status, today, jobs_found),
    )
    conn.commit()


def should_skip_ats_slug(conn, ats: str, slug: str, not_found_ttl_days: int = 30) -> bool:
    """Return True if a slug was marked 'not_found' within the last `not_found_ttl_days`.
    Other statuses (active/empty/error) never skip — we re-check every run."""
    info = get_ats_status(conn, ats, slug)
    if info is None or info["status"] != "not_found":
        return False
    try:
        when = datetime.strptime(info["last_checked"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return False
    return (datetime.now(timezone.utc) - when) < timedelta(days=not_found_ttl_days)


def get_consecutive_zero_runs(conn) -> int:
    """Look back at run_log. Returns count of consecutive most-recent zero runs."""
    cur = conn.execute(
        "SELECT jsearch_found, jooble_found, adzuna_found, usajobs_found, alerts_found "
        "FROM run_log ORDER BY id DESC LIMIT 10"
    )
    count = 0
    for row in cur.fetchall():
        if sum(v or 0 for v in row) == 0:
            count += 1
        else:
            break
    return count
