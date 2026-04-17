"""Tests for src/db.py — uses in-memory sqlite3 since libsql is sqlite3-compatible."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

import pytest

from src import db


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    db.migrate(c)
    yield c
    c.close()


def sample_job(ext_id="test_1", title="People Analytics Manager", company="Netflix"):
    return {
        "external_id": ext_id,
        "title": title,
        "company": company,
        "company_normalized": company.lower(),
        "location": "Los Gatos, CA",
        "location_country": "US",
        "source_url": "https://example.com/job/1",
        "source_name": "jsearch",
        "description": "Full description here.",
        "description_snippet": "Full description here.",
        "salary_min": 150000.0,
        "salary_max": 200000.0,
        "salary_range": "$150K-$200K",
        "is_remote": "hybrid",
        "work_arrangement": "Hybrid",
        "keyword_score": 80,
        "keywords_matched": "people analytics,employee listening",
        "llm_classification": "RELEVANT",
        "llm_confidence": 95,
        "llm_provider": "groq",
        "llm_reasoning": "Core people analytics role.",
        "fit_score": 80,
        "date_posted": "2026-04-15",
        "raw_data": {"id": "abc"},
    }


def test_migrate_creates_tables(conn):
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert {"jobs", "run_log", "retry_queue"}.issubset(tables)


def test_migrate_is_idempotent(conn):
    db.migrate(conn)  # second run
    db.migrate(conn)  # third run
    # still only one copy of each table
    cur = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='jobs'")
    assert cur.fetchone()[0] == 1


def test_upsert_job_create_then_update(conn):
    result = db.upsert_job(conn, sample_job())
    assert result == "created"
    row = conn.execute(
        "SELECT title, first_seen_date, raw_data FROM jobs WHERE external_id=?",
        ("test_1",),
    ).fetchone()
    assert row[0] == "People Analytics Manager"
    assert row[1] == _today()
    assert json.loads(row[2]) == {"id": "abc"}

    job = sample_job(title="Senior People Analytics Manager")
    result = db.upsert_job(conn, job)
    assert result == "updated"
    row = conn.execute(
        "SELECT title FROM jobs WHERE external_id=?", ("test_1",)
    ).fetchone()
    assert row[0] == "Senior People Analytics Manager"


def test_set_wp_post_id(conn):
    db.upsert_job(conn, sample_job())
    db.set_wp_post_id(conn, "test_1", 42)
    row = conn.execute(
        "SELECT wp_post_id FROM jobs WHERE external_id=?", ("test_1",)
    ).fetchone()
    assert row[0] == 42


def test_get_active_jobs_for_dedup(conn):
    db.upsert_job(conn, sample_job("a", "People Analytics Manager", "Netflix"))
    db.upsert_job(conn, sample_job("b", "Employee Listening Lead", "Google"))
    active = db.get_active_jobs_for_dedup(conn)
    assert len(active) == 2
    assert {j["external_id"] for j in active} == {"a", "b"}


def test_get_stale_and_archive(conn):
    db.upsert_job(conn, sample_job("stale_1"))
    old = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")
    conn.execute(
        "UPDATE jobs SET last_seen_date=? WHERE external_id=?",
        (old, "stale_1"),
    )
    conn.commit()

    stale = db.get_stale_active_jobs(conn, days=7)
    assert len(stale) == 1
    assert stale[0]["external_id"] == "stale_1"

    db.archive_job(conn, "stale_1", days_active=10)
    row = conn.execute(
        "SELECT is_active, archived_date, days_active FROM jobs WHERE external_id=?",
        ("stale_1",),
    ).fetchone()
    assert row[0] == 0
    assert row[1] == _today()
    assert row[2] == 10


def test_retry_queue_lifecycle(conn):
    db.enqueue_retry(conn, {"external_id": "rq1", "title": "X"})
    items = db.fetch_retry_queue(conn)
    assert len(items) == 1
    retry_id, job = items[0]
    assert job["external_id"] == "rq1"

    db.mark_retry_failure(conn, retry_id)
    db.mark_retry_failure(conn, retry_id)
    assert len(db.fetch_retry_queue(conn)) == 1

    db.mark_retry_failure(conn, retry_id)  # 3 attempts → now exhausted
    assert db.fetch_retry_queue(conn, max_attempts=3) == []
    dropped = db.drop_exhausted_retries(conn)
    assert dropped == 1


def test_retry_queue_success_removes(conn):
    db.enqueue_retry(conn, {"external_id": "rq2"})
    items = db.fetch_retry_queue(conn)
    db.mark_retry_success(conn, items[0][0])
    assert db.fetch_retry_queue(conn) == []


# ───────────── Phase A: enrichment schema columns ─────────────────────────

PHASE_A_COLUMNS = [
    "apply_url",
    "location_confidence",
    "salary_confidence",
    "remote_confidence",
    "enrichment_source",
    "enrichment_date",
]


def _column_names(conn, table="jobs"):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def test_phase_a_columns_exist(conn):
    cols = _column_names(conn)
    for c in PHASE_A_COLUMNS:
        assert c in cols, f"missing column: {c}"


def test_phase_a_confidence_defaults_unverified(conn):
    db.upsert_job(conn, sample_job())
    row = conn.execute(
        "SELECT location_confidence, salary_confidence, remote_confidence, "
        "apply_url, enrichment_source, enrichment_date "
        "FROM jobs WHERE external_id=?",
        ("test_1",),
    ).fetchone()
    assert row[0] == "unverified"
    assert row[1] == "unverified"
    assert row[2] == "unverified"
    # Optional columns default to NULL
    assert row[3] is None
    assert row[4] is None
    assert row[5] is None


def test_phase_a_enrichment_fields_upsert():
    c = sqlite3.connect(":memory:")
    db.migrate(c)
    job = sample_job()
    job["apply_url"] = "https://careers.netflix.com/apply/123"
    job["location_confidence"] = "confirmed"
    job["salary_confidence"] = "aggregator_only"
    job["remote_confidence"] = "confirmed"
    job["enrichment_source"] = "source_page"
    job["enrichment_date"] = "2026-04-17"
    db.upsert_job(c, job)
    row = c.execute(
        "SELECT apply_url, location_confidence, salary_confidence, "
        "remote_confidence, enrichment_source, enrichment_date "
        "FROM jobs WHERE external_id=?",
        ("test_1",),
    ).fetchone()
    assert row == (
        "https://careers.netflix.com/apply/123",
        "confirmed", "aggregator_only", "confirmed",
        "source_page", "2026-04-17",
    )
    c.close()


def test_phase_a_migration_adds_columns_to_pre_migration_db():
    """Simulate a database that pre-dates Phase A and verify migrate() adds all
    the new columns without raising duplicate-column errors."""
    c = sqlite3.connect(":memory:")
    # Pre-Phase-A schema (has is_active, etc., just missing the new columns)
    c.execute(
        "CREATE TABLE jobs ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  external_id TEXT NOT NULL UNIQUE,"
        "  title TEXT NOT NULL,"
        "  company TEXT NOT NULL,"
        "  source_name TEXT NOT NULL,"
        "  is_active INTEGER DEFAULT 1,"
        "  company_normalized TEXT,"
        "  last_seen_date TEXT NOT NULL,"
        "  first_seen_date TEXT NOT NULL"
        ")"
    )
    c.commit()
    cols_before = _column_names(c)
    for col in PHASE_A_COLUMNS:
        assert col not in cols_before, f"sanity: {col} should not exist yet"

    # Running migrate on an existing, partially-populated schema must succeed
    db.migrate(c)
    cols = _column_names(c)
    for col in PHASE_A_COLUMNS:
        assert col in cols, f"migration did not add: {col}"

    # And must be idempotent — second run with the columns already present
    db.migrate(c)
    c.close()


def test_log_run_and_consecutive_zeros(conn):
    db.log_run(conn, {
        "run_date": "2026-04-15",
        "jsearch_found": 5, "jooble_found": 2, "adzuna_found": 0,
        "usajobs_found": 0, "alerts_found": 1,
        "total_passed_filter": 3, "total_published": 2, "total_archived": 0,
        "errors": "", "llm_provider_used": "groq", "duration_seconds": 12.3,
        "consecutive_zero_runs": 0,
    })
    assert db.get_consecutive_zero_runs(conn) == 0

    for _ in range(3):
        db.log_run(conn, {
            "run_date": "2026-04-16",
            "jsearch_found": 0, "jooble_found": 0, "adzuna_found": 0,
            "usajobs_found": 0, "alerts_found": 0,
            "total_passed_filter": 0, "total_published": 0, "total_archived": 0,
            "errors": "", "llm_provider_used": "keyword_only",
            "duration_seconds": 1.0, "consecutive_zero_runs": 0,
        })
    assert db.get_consecutive_zero_runs(conn) == 3
