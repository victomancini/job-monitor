"""Turso libSQL database layer: connection, migration, query helpers."""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d")

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
    last_lifecycle_check TEXT,
    last_lifecycle_verdict TEXT,
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
    # R-audit: source-of-truth lifecycle check stamp. Distinct from
    # last_seen_date (aggregator visibility) — records when we last verified
    # the job via ATS API membership or company career page HEAD request.
    ("last_lifecycle_check", "last_lifecycle_check TEXT"),
    # R7-A: the most recent lifecycle_checker verdict — 'active', 'likely_closed',
    # or 'unknown'. Disambiguates a Track B time-based likely_closed (no
    # authoritative signal) from a lifecycle_checker-confirmed closure (ATS API
    # 404 or source-page HEAD 404). Fast-path archival requires this column to
    # equal 'likely_closed' so an 'unknown' HEAD verdict on an already-stale
    # job can't short-circuit the 21-day grace window.
    ("last_lifecycle_verdict", "last_lifecycle_verdict TEXT"),
]


def _is_stream_error(e: BaseException) -> bool:
    """Detect Turso/Hrana stream-expiration errors. These happen when the
    server evicts a long-idle stream — the connection object itself is still
    alive but every execute on it fails with 'stream not found'. Pattern
    observed in GH Actions logs 2026-04-20:

        ValueError: Hrana: `api error: `status=404 Not Found,
        body={"error":"stream not found: ..."}``

    The only remedy is to establish a fresh connection.
    """
    s = str(e).lower()
    return ("stream not found" in s
            or "stream closed" in s
            or "stream expired" in s)


def _raw_libsql_connect(url: str, auth_token: str, *, max_attempts: int = 3):
    """Low-level libsql.connect with the same backoff behavior we use for
    initial connections. Pulled out so the auto-reconnect wrapper can call it
    without recursing through the public `connect()`."""
    backoffs = [2, 4]
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return libsql.connect(url, auth_token=auth_token)
        except Exception as e:  # noqa: BLE001 — libsql raises various types
            last_exc = e
            if attempt < max_attempts - 1:
                wait = backoffs[min(attempt, len(backoffs) - 1)]
                log.warning(
                    "db.connect attempt %d/%d failed (%s) — retrying in %ds",
                    attempt + 1, max_attempts, e, wait,
                )
                time.sleep(wait)
    assert last_exc is not None
    raise last_exc


class _AutoReconnectConnection:
    """Wrapper around a libsql Connection that catches stream-expiration
    errors on `execute()` / `commit()` and transparently reconnects +
    retries once.

    Rationale: long-running phases (enrichment with 1 req/host throttling,
    lifecycle checker with HTTP budget) can idle the Turso stream for 10+
    minutes. The server evicts the stream but the client holds on to it.
    Next write fails. Without auto-reconnect, the first post-enrichment
    call (fetch_retry_queue in Phase 5) crashes the whole pipeline — which
    is exactly what happened in the 2026-04-20 run.

    Tests using sqlite3.Connection directly bypass this wrapper entirely.
    Only libsql connections created via `db.connect()` get wrapped.
    """

    def __init__(self, url: str, auth_token: str, *, max_attempts: int = 3) -> None:
        self._url = url
        self._auth = auth_token
        self._conn = _raw_libsql_connect(url, auth_token, max_attempts=max_attempts)

    def _reconnect(self) -> None:
        log.warning("db: Turso stream expired — reconnecting")
        self._conn = _raw_libsql_connect(self._url, self._auth)

    def execute(self, *args, **kwargs):
        try:
            return self._conn.execute(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            if _is_stream_error(e):
                self._reconnect()
                return self._conn.execute(*args, **kwargs)
            raise

    def commit(self):
        try:
            return self._conn.commit()
        except Exception as e:  # noqa: BLE001
            if _is_stream_error(e):
                self._reconnect()
                return self._conn.commit()
            raise

    def close(self):
        try:
            return self._conn.close()
        except Exception:  # noqa: BLE001 — best-effort close
            return None

    def __getattr__(self, name):
        # Delegate any other attribute access (cursor, rowcount, etc.) to the
        # underlying connection. Any call path not wrapped above loses the
        # auto-reconnect protection — that's by design; those are rare.
        return getattr(self._conn, name)


def connect(
    url: str | None = None,
    auth_token: str | None = None,
    *,
    max_attempts: int = 3,
):
    """Connect to Turso libSQL. Falls back to env vars if args omitted.

    R7: retry with exponential backoff (2s, 4s) on transient connect errors.
    Turso occasionally returns 5xx during regional failover; a single retry
    bucket prevents a transient blip from killing the entire run.

    R10: returns an `_AutoReconnectConnection` that self-heals when the
    Turso stream expires mid-pipeline (enrichment can idle the conn 10+
    minutes). `max_attempts` applies to the INITIAL connect only; runtime
    stream reconnects always get fresh backoff via `_raw_libsql_connect`.
    """
    if libsql is None:
        raise RuntimeError("libsql is not installed in this environment")
    url = url or os.environ.get("TURSO_DB_URL", "")
    auth_token = auth_token or os.environ.get("TURSO_AUTH_TOKEN", "")
    if not url:
        raise ValueError("TURSO_DB_URL is required")
    return _AutoReconnectConnection(url, auth_token, max_attempts=max_attempts)


def _execute_with_retry(conn, sql: str, params: tuple | list = ()) -> Any:
    """R8-M10: run a single `conn.execute` with one retry on any libsql
    exception. A transient Turso blip during an upsert would otherwise skip
    that job silently; one retry with a 1-second sleep covers the common
    failure mode (brief regional failover) without burning wall time on
    permanently-dead connections.

    Not used for SELECTs — those are cheap to replay from the caller if they
    fail. Reserved for the writes that matter (upsert_job, log_run, etc.).
    """
    try:
        return conn.execute(sql, params)
    except Exception as e:  # noqa: BLE001 — libsql raises multiple concrete types
        log.warning("db.execute failed once, retrying in 1s: %s", e)
        time.sleep(1.0)
        return conn.execute(sql, params)


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
    # Derive company_normalized if the caller didn't. Lazy import so db.py has
    # no hard dependency on processors.
    if not values.get("company_normalized") and values.get("company"):
        from src.processors.deduplicator import normalize_company
        values["company_normalized"] = normalize_company(values["company"])

    if row is None:
        values["first_seen_date"] = today
        # Drop None values so schema DEFAULTs apply (is_remote='unknown',
        # *_confidence='unverified', etc.).
        insert_vals = {k: v for k, v in values.items() if v is not None}
        cols = ", ".join(insert_vals.keys())
        placeholders = ", ".join(["?"] * len(insert_vals))
        _execute_with_retry(
            conn,
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
    # Resurrection: if a previously-archived row (is_active=0 with archived_date
    # and days_active set) reappears in a fresh batch, clear the archival state
    # so the dashboard/WP don't show stale "archived X days ago" on a live job.
    # Only clears when the incoming row is marking itself active (which upsert
    # always does — is_active defaulted to 1 above).
    if values.get("is_active") == 1:
        set_clause += ", archived_date = NULL, days_active = NULL"
    params = [values[c] for c in update_cols] + [job["external_id"]]
    _execute_with_retry(
        conn, f"UPDATE jobs SET {set_clause} WHERE external_id = ?", params,
    )
    conn.commit()
    return "updated"


def set_wp_post_id(conn, external_id: str, wp_post_id: int) -> None:
    conn.execute(
        "UPDATE jobs SET wp_post_id = ?, updated_at = datetime('now') WHERE external_id = ?",
        (wp_post_id, external_id),
    )
    conn.commit()


def upgrade_apply_url(conn, external_id: str, new_apply_url: str) -> None:
    """Replace the apply_url on an existing row (used when the dedup pipeline
    sees a direct-company URL for a role that's already stored with an
    aggregator URL). No-op if the URL is empty."""
    if not new_apply_url:
        return
    conn.execute(
        "UPDATE jobs SET apply_url = ?, updated_at = datetime('now') WHERE external_id = ?",
        (new_apply_url, external_id),
    )
    conn.commit()


def get_row_for_wp_push(conn, external_id: str) -> dict[str, Any] | None:
    """Return the minimal payload needed to push a targeted WP meta update for
    an existing row (R4-7 apply_url upgrades). None if the row doesn't exist."""
    row = conn.execute(
        "SELECT external_id, title, apply_url, source_url FROM jobs WHERE external_id = ?",
        (external_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "external_id": row[0],
        "title": row[1],
        "apply_url": row[2],
        "source_url": row[3],
    }


def get_active_jobs_for_dedup(conn, *, include_recent_archived_days: int = 30) -> list[dict[str, Any]]:
    """Return jobs for deduplication comparison. By default includes active rows
    plus archived rows from the last `include_recent_archived_days` days so that
    a re-emerging posting (same company+title, different external_id from a
    different aggregator) dedups against its archived sibling instead of
    creating a second row. Set `include_recent_archived_days=0` to limit to
    active only."""
    # apply_url is selected so the deduplicator can compare URL quality when an
    # incoming direct-company URL matches an existing DB row carrying an
    # aggregator URL — and promote the DB row's apply_url on upsert.
    if include_recent_archived_days > 0:
        cutoff = _days_ago(include_recent_archived_days)
        cur = conn.execute(
            "SELECT external_id, title, company, company_normalized, location, apply_url "
            "FROM jobs WHERE is_active = 1 "
            "OR (is_active = 0 AND archived_date IS NOT NULL AND archived_date >= ?)",
            (cutoff,),
        )
    else:
        cur = conn.execute(
            "SELECT external_id, title, company, company_normalized, location, apply_url "
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


# ────────────── R-audit: lifecycle checker helpers ──────────────

def get_jobs_for_lifecycle_check(conn, stale_days: int = 2, limit: int = 500) -> list[dict[str, Any]]:
    """Return active jobs due for a lifecycle check. Excludes rows already
    checked within `stale_days`. `limit` caps per-run volume so the GH Actions
    budget isn't eaten by a single run."""
    cutoff = _days_ago(stale_days)
    cur = conn.execute(
        "SELECT external_id, source_name, apply_url, source_url, last_lifecycle_check "
        "FROM jobs WHERE is_active = 1 "
        "AND (last_lifecycle_check IS NULL OR last_lifecycle_check < ?) "
        "ORDER BY last_lifecycle_check IS NULL DESC, last_lifecycle_check ASC "
        "LIMIT ?",
        (cutoff, limit),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def record_lifecycle_check(conn, external_id: str, status: str) -> str | None:
    """Stamp `last_lifecycle_check` and `last_lifecycle_verdict` to today's
    verdict, transitioning `lifecycle_status` based on the checker result.

    Returns the NEW lifecycle_status IF it transitioned this call, otherwise
    None. The caller (check_lifecycle_batch) collects transitions so the
    collector can push a targeted WP meta update — without that, the WP table
    would show the stale lifecycle_status until the next full publish cycle
    (up to 24h).

    Verdict semantics:
      - status='active'        → lifecycle_status='active'
      - status='likely_closed' → lifecycle_status='likely_closed'
      - status='unknown'       → lifecycle_status unchanged; timestamp + verdict
                                 updated so the freshness-skip window applies
                                 on subsequent runs.

    R7-A: the verdict is stored separately from lifecycle_status so the
    archiver fast-path can distinguish "lifecycle_checker said likely_closed"
    (authoritative) from "Track B time-marked + HEAD later returned unknown"
    (NOT authoritative — wait 21-day window).
    """
    today = _today()
    # Read prior state so we can detect transitions and signal the caller.
    prior = conn.execute(
        "SELECT lifecycle_status FROM jobs WHERE external_id = ?",
        (external_id,),
    ).fetchone()
    old_status = prior[0] if prior else None

    if status == "unknown":
        _execute_with_retry(
            conn,
            "UPDATE jobs SET last_lifecycle_check = ?, last_lifecycle_verdict = ?, "
            "updated_at = datetime('now') WHERE external_id = ?",
            (today, status, external_id),
        )
        conn.commit()
        return None  # lifecycle_status never changes on unknown
    new_status = "active" if status == "active" else "likely_closed"
    _execute_with_retry(
        conn,
        "UPDATE jobs SET lifecycle_status = ?, last_lifecycle_check = ?, "
        "last_lifecycle_verdict = ?, updated_at = datetime('now') "
        "WHERE external_id = ?",
        (new_status, today, status, external_id),
    )
    conn.commit()
    return new_status if new_status != old_status else None


def get_jobs_to_archive_confirmed_closed(conn, limit: int = 100) -> list[dict[str, Any]]:
    """R-audit: fast-path for jobs confirmed-closed by a source-of-truth check.

    R7-A: requires `last_lifecycle_verdict='likely_closed'` — an authoritative
    verdict from lifecycle_checker (ATS API absence or source-page 404/410).
    Track B time-based `likely_closed` rows have last_lifecycle_verdict NULL
    (or 'unknown' after a later indecisive HEAD check) and must continue to
    wait the 21-day window via the time-based path.
    """
    cur = conn.execute(
        "SELECT id, external_id, wp_post_id, first_seen_date, last_seen_date "
        "FROM jobs WHERE is_active = 1 "
        "AND lifecycle_status = 'likely_closed' "
        "AND last_lifecycle_verdict = 'likely_closed' "
        "LIMIT ?",
        (limit,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


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
    """R8-M9: dedup by external_id. If WP is down for several days, the
    collector would enqueue the same job every run, piling up duplicate
    retry_queue rows. Before inserting, check whether the job already has a
    pending row and — if so — update it in place (reset attempts, refresh
    payload) rather than adding a duplicate.

    `retry_queue.job_json` is opaque TEXT; there's no indexed external_id
    column, so we do a LIKE scan. At the sizes we operate at (tens of rows
    at most) this is fine. If retry_queue ever grows to thousands, promote
    external_id to a real indexed column.
    """
    ext_id = job.get("external_id") or ""
    new_json = json.dumps(job)
    if ext_id:
        # Look for an existing row carrying this external_id. We match on the
        # JSON-escaped substring since json.dumps always quotes the value.
        needle = '"external_id": "' + ext_id.replace('"', '\\"') + '"'
        existing = conn.execute(
            "SELECT id FROM retry_queue WHERE job_json LIKE ? LIMIT 1",
            (f"%{needle}%",),
        ).fetchone()
        if existing is not None:
            conn.execute(
                "UPDATE retry_queue SET job_json = ?, attempts = 0, "
                "last_attempt = NULL WHERE id = ?",
                (new_json, existing[0]),
            )
            conn.commit()
            return
    conn.execute(
        "INSERT INTO retry_queue (job_json) VALUES (?)",
        (new_json,),
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
    """Insert a row into run_log. `stats` keys map to columns.

    R8-M10: the run_log row is the only source of truth for the
    consecutive-zero-run canary — losing it to a transient write failure
    would mask an outage. Route through `_execute_with_retry`.
    """
    cols = [
        "run_date", "jsearch_found", "jooble_found", "adzuna_found",
        "usajobs_found", "alerts_found", "total_passed_filter",
        "total_published", "total_archived", "errors", "llm_provider_used",
        "duration_seconds", "consecutive_zero_runs",
    ]
    values = [stats.get(c) for c in cols]
    placeholders = ", ".join(["?"] * len(cols))
    _execute_with_retry(
        conn,
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
