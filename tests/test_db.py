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

    stale = db.get_jobs_to_archive(conn, days=7)
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


# Regression: C2 — upsert must populate company_normalized so the index is
# actually used and dedup's DB-side comparisons aren't working against NULL.
def test_upsert_populates_company_normalized(conn):
    j = sample_job("cn1", company="Netflix, Inc.")
    j.pop("company_normalized", None)  # simulate a source that didn't set it
    db.upsert_job(conn, j)
    row = conn.execute(
        "SELECT company_normalized FROM jobs WHERE external_id=?", ("cn1",)
    ).fetchone()
    assert row[0] == "netflix"


def test_upsert_respects_caller_provided_company_normalized(conn):
    j = sample_job("cn2", company="Acme Corporation")
    j["company_normalized"] = "acme-forced"
    db.upsert_job(conn, j)
    row = conn.execute(
        "SELECT company_normalized FROM jobs WHERE external_id=?", ("cn2",)
    ).fetchone()
    assert row[0] == "acme-forced"


# Regression: C3 — when a previously-archived job reappears, upsert must clear
# archived_date + days_active so the row isn't shown as "archived N days ago"
# while is_active=1.
# Regression: N3 — dedup pool must include recently-archived rows so a job
# re-emerging from a different aggregator dedups against its archived sibling.
# ───── R7-1: db.connect retry ─────────────────────────────────

def test_db_connect_retries_on_transient_error(monkeypatch):
    """R7-1: a single transient error should be retried; the second attempt
    succeeding returns a usable connection.

    R10: `connect()` now returns an _AutoReconnectConnection wrapper instead
    of the raw libsql connection. Assert the wrapper was produced and that
    the underlying libsql connect was retried."""
    from src import db as _db
    from unittest.mock import MagicMock
    attempts = {"count": 0}

    class FakeLibsql:
        @staticmethod
        def connect(url, auth_token=None):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("transient turso outage")
            return MagicMock(name="raw_conn")

    monkeypatch.setattr(_db, "libsql", FakeLibsql)
    monkeypatch.setenv("TURSO_DB_URL", "libsql://x")
    monkeypatch.setattr(_db.time, "sleep", lambda *a, **kw: None)
    result = _db.connect()
    assert isinstance(result, _db._AutoReconnectConnection)
    assert attempts["count"] == 2


def test_db_connect_raises_after_exhausted_retries(monkeypatch):
    """All attempts exhausted → the final exception bubbles up."""
    from src import db as _db

    class FakeLibsql:
        @staticmethod
        def connect(url, auth_token=None):
            raise RuntimeError("permanently down")

    monkeypatch.setattr(_db, "libsql", FakeLibsql)
    monkeypatch.setenv("TURSO_DB_URL", "libsql://x")
    monkeypatch.setattr(_db.time, "sleep", lambda *a, **kw: None)
    with pytest.raises(RuntimeError, match="permanently down"):
        _db.connect(max_attempts=3)


# ───── R10: AutoReconnectConnection stream-expiration handling ─────────

def test_is_stream_error_detects_turso_patterns():
    """R10: regex for the exact error shape seen in 2026-04-20 GH Actions run."""
    from src import db as _db
    # The literal error string from the failing run, plus other variants
    for msg in [
        "Hrana: `api error: `status=404 Not Found, body={\"error\":\"stream not found: abc\"}`",
        "stream closed",
        "Stream Expired after long idle",  # case-insensitive
    ]:
        assert _db._is_stream_error(Exception(msg)), f"should detect: {msg!r}"


def test_is_stream_error_rejects_normal_exceptions():
    from src import db as _db
    assert not _db._is_stream_error(Exception("connection refused"))
    assert not _db._is_stream_error(Exception("syntax error"))
    assert not _db._is_stream_error(RuntimeError("random"))


def test_auto_reconnect_wrapper_catches_stream_error_on_execute(monkeypatch):
    """R10: the exact bug from 2026-04-20. A stale stream fails once, the
    wrapper reconnects, and the retry succeeds."""
    from src import db as _db
    from unittest.mock import MagicMock

    call_log: list[str] = []

    class FakeConn:
        def __init__(self, name):
            self.name = name
        def execute(self, *a, **kw):
            call_log.append(f"{self.name}.execute")
            if self.name == "stale":
                raise ValueError(
                    'Hrana: `api error: `status=404 Not Found, body={"error":"stream not found: xyz"}``'
                )
            return MagicMock(name=f"{self.name}_cursor")
        def commit(self):
            call_log.append(f"{self.name}.commit")
        def close(self):
            pass

    conns = iter([FakeConn("stale"), FakeConn("fresh")])

    class FakeLibsql:
        @staticmethod
        def connect(url, auth_token=None):
            return next(conns)

    monkeypatch.setattr(_db, "libsql", FakeLibsql)
    monkeypatch.setenv("TURSO_DB_URL", "libsql://x")
    monkeypatch.setattr(_db.time, "sleep", lambda *a, **kw: None)

    wrapper = _db.connect()
    # First execute on the wrapper: underlying 'stale' conn raises stream
    # error → wrapper reconnects to 'fresh' and retries.
    result = wrapper.execute("SELECT 1")
    assert result is not None
    # Verify the call order: stale.execute (raised), fresh.execute (returned)
    assert call_log == ["stale.execute", "fresh.execute"]


def test_auto_reconnect_wrapper_only_reconnects_on_stream_errors(monkeypatch):
    """Non-stream errors (bad SQL, type mismatch) must propagate unchanged —
    we don't want the wrapper to hide real bugs behind a silent reconnect."""
    from src import db as _db
    from unittest.mock import MagicMock

    class FakeConn:
        def __init__(self):
            self.execute_calls = 0
        def execute(self, *a, **kw):
            self.execute_calls += 1
            raise ValueError("syntax error near 'SELEC'")
        def commit(self):
            pass
        def close(self):
            pass

    fake = FakeConn()

    class FakeLibsql:
        @staticmethod
        def connect(url, auth_token=None):
            return fake

    monkeypatch.setattr(_db, "libsql", FakeLibsql)
    monkeypatch.setenv("TURSO_DB_URL", "libsql://x")
    monkeypatch.setattr(_db.time, "sleep", lambda *a, **kw: None)

    wrapper = _db.connect()
    with pytest.raises(ValueError, match="syntax error"):
        wrapper.execute("SELEC * FROM jobs")
    # Only one attempt; no reconnect fired
    assert fake.execute_calls == 1


def test_auto_reconnect_wrapper_commit_recovers_from_stream_error(monkeypatch):
    """commit() has the same stream-expiration recovery as execute()."""
    from src import db as _db

    class FakeConn:
        def __init__(self, name, raise_on_commit):
            self.name = name
            self.raise_on_commit = raise_on_commit
            self.commit_calls = 0
        def execute(self, *a, **kw):
            pass
        def commit(self):
            self.commit_calls += 1
            if self.raise_on_commit:
                raise ValueError("stream not found: xyz")
        def close(self):
            pass

    conns = iter([FakeConn("stale", True), FakeConn("fresh", False)])

    class FakeLibsql:
        @staticmethod
        def connect(url, auth_token=None):
            return next(conns)

    monkeypatch.setattr(_db, "libsql", FakeLibsql)
    monkeypatch.setenv("TURSO_DB_URL", "libsql://x")
    monkeypatch.setattr(_db.time, "sleep", lambda *a, **kw: None)
    wrapper = _db.connect()
    wrapper.commit()  # stale commit raises → reconnect → fresh commit succeeds


def test_db_connect_sleeps_backoff_between_attempts(monkeypatch):
    """Backoff waits 2s after first failure, 4s after second."""
    from src import db as _db
    sleeps: list[float] = []

    class FakeLibsql:
        @staticmethod
        def connect(url, auth_token=None):
            raise RuntimeError("down")

    monkeypatch.setattr(_db, "libsql", FakeLibsql)
    monkeypatch.setenv("TURSO_DB_URL", "libsql://x")
    monkeypatch.setattr(_db.time, "sleep", lambda s: sleeps.append(s))
    with pytest.raises(RuntimeError):
        _db.connect(max_attempts=3)
    assert sleeps == [2, 4]


def test_get_active_jobs_for_dedup_includes_recent_archives(conn):
    db.upsert_job(conn, sample_job("active_one"))
    db.upsert_job(conn, sample_job("recently_archived"))
    db.upsert_job(conn, sample_job("long_archived"))
    # Archive two — one recently, one ancient
    db.archive_job(conn, "recently_archived", days_active=3)
    conn.execute(
        "UPDATE jobs SET archived_date=? WHERE external_id=?",
        ((datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d"), "recently_archived"),
    )
    db.archive_job(conn, "long_archived", days_active=60)
    conn.execute(
        "UPDATE jobs SET archived_date=? WHERE external_id=?",
        ((datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d"), "long_archived"),
    )
    conn.commit()
    pool = db.get_active_jobs_for_dedup(conn, include_recent_archived_days=30)
    ids = {j["external_id"] for j in pool}
    assert "active_one" in ids
    assert "recently_archived" in ids  # within 30-day window
    assert "long_archived" not in ids


def test_get_active_jobs_for_dedup_legacy_active_only(conn):
    """Backward-compat: passing 0 returns active rows only."""
    db.upsert_job(conn, sample_job("a"))
    db.upsert_job(conn, sample_job("b"))
    db.archive_job(conn, "b", days_active=5)
    pool = db.get_active_jobs_for_dedup(conn, include_recent_archived_days=0)
    assert {j["external_id"] for j in pool} == {"a"}


def test_upsert_resurrection_clears_archived_fields(conn):
    db.upsert_job(conn, sample_job("res1"))
    db.archive_job(conn, "res1", days_active=12)
    # Sanity: archived_date + days_active are set
    row = conn.execute(
        "SELECT is_active, archived_date, days_active FROM jobs WHERE external_id=?",
        ("res1",),
    ).fetchone()
    assert row == (0, _today(), 12)
    # Reappear in a fresh batch
    db.upsert_job(conn, sample_job("res1"))
    row = conn.execute(
        "SELECT is_active, archived_date, days_active FROM jobs WHERE external_id=?",
        ("res1",),
    ).fetchone()
    assert row[0] == 1
    assert row[1] is None
    assert row[2] is None


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


# ───── R11 Phase 0: first_seen_date + date_posted preservation ─────────

def test_upsert_preserves_first_seen_date_on_update(conn):
    """The NEW-today bug root cause: Turso's first_seen_date must stay fixed
    once set. The update path on line 378 is supposed to filter it out; this
    regression locks that in and also verifies the job dict is populated with
    the authoritative value for the WP publisher to ship."""
    j1 = sample_job("fsd_1")
    db.upsert_job(conn, j1)
    first = conn.execute(
        "SELECT first_seen_date FROM jobs WHERE external_id=?", ("fsd_1",)
    ).fetchone()[0]
    assert j1["first_seen_date"] == first
    assert j1["_is_brand_new"] is True

    # Manually backdate so we can detect accidental reset
    conn.execute(
        "UPDATE jobs SET first_seen_date='2026-01-01' WHERE external_id=?", ("fsd_1",)
    )
    conn.commit()

    j2 = sample_job("fsd_1", title="Senior People Analytics Manager")
    db.upsert_job(conn, j2)
    preserved = conn.execute(
        "SELECT first_seen_date FROM jobs WHERE external_id=?", ("fsd_1",)
    ).fetchone()[0]
    assert preserved == "2026-01-01"
    assert j2["first_seen_date"] == "2026-01-01"  # publisher reads this
    assert j2["_is_brand_new"] is False


def test_upsert_update_does_not_wipe_existing_values_with_none(conn):
    """A source re-seeing a job without a field it doesn't emit must not wipe
    data another source already provided. Secondary R11 bug: the update path
    formerly SET every column including None-valued ones, erasing salary from
    Greenhouse the moment Jooble re-observed the same role without salary."""
    j_full = sample_job("preserve_1")
    db.upsert_job(conn, j_full)
    row = conn.execute(
        "SELECT salary_min, salary_max, llm_reasoning FROM jobs WHERE external_id=?",
        ("preserve_1",),
    ).fetchone()
    assert row[0] == 150000.0
    assert row[1] == 200000.0
    assert row[2] == "Core people analytics role."

    # Second source re-sees the job with NO salary and NO llm_reasoning
    j_sparse = {
        "external_id": "preserve_1",
        "title": "People Analytics Manager",
        "company": "Netflix",
        "source_url": "https://jooble.org/job/1",
        "source_name": "jooble",
    }
    db.upsert_job(conn, j_sparse)
    row = conn.execute(
        "SELECT salary_min, salary_max, llm_reasoning FROM jobs WHERE external_id=?",
        ("preserve_1",),
    ).fetchone()
    assert row[0] == 150000.0, "salary_min must not be wiped by a later None"
    assert row[1] == 200000.0, "salary_max must not be wiped"
    assert row[2] == "Core people analytics role.", "llm_reasoning must not be wiped"


def test_upsert_date_posted_keeps_earliest(conn):
    """date_posted is the posting date, not the scrape date. Jooble's
    `updated=today` for a Greenhouse posting first seen a week ago must not
    overwrite the earlier, more accurate date."""
    j1 = sample_job("dp_1")
    j1["date_posted"] = "2026-04-01"
    db.upsert_job(conn, j1)

    # Later source reports today as date_posted (Jooble pathology)
    j2 = sample_job("dp_1")
    j2["date_posted"] = "2026-04-20"
    db.upsert_job(conn, j2)

    dp = conn.execute(
        "SELECT date_posted FROM jobs WHERE external_id=?", ("dp_1",)
    ).fetchone()[0]
    assert dp == "2026-04-01", "earlier date_posted must win"


def test_upsert_date_posted_preserved_when_incoming_none(conn):
    j1 = sample_job("dp_2")
    j1["date_posted"] = "2026-04-10"
    db.upsert_job(conn, j1)

    j2 = sample_job("dp_2")
    j2["date_posted"] = None
    db.upsert_job(conn, j2)

    dp = conn.execute(
        "SELECT date_posted FROM jobs WHERE external_id=?", ("dp_2",)
    ).fetchone()[0]
    assert dp == "2026-04-10"


def test_upsert_is_brand_new_flag_transitions(conn):
    """NEW badge trigger: _is_brand_new must be True only on the run that
    actually inserts the Turso row, False on every subsequent update."""
    j1 = sample_job("bn_1")
    db.upsert_job(conn, j1)
    assert j1["_is_brand_new"] is True

    j2 = sample_job("bn_1")
    db.upsert_job(conn, j2)
    assert j2["_is_brand_new"] is False

    j3 = sample_job("bn_1")
    db.upsert_job(conn, j3)
    assert j3["_is_brand_new"] is False


# ───── R11 Phase 1: field_sources column + provenance round-trip ────

def test_upsert_persists_field_sources_json(conn):
    """Provenance dict populated by build_job must round-trip through the
    field_sources column so consensus voting in a later batch can read
    the full history across runs."""
    j = sample_job("fs_1")
    j["_field_sources"] = {
        "is_remote": [
            {"source": "jsearch", "value": "remote", "confidence": 0.55},
            {"source": "greenhouse", "value": "hybrid", "confidence": 0.90},
        ],
        "salary_min": [
            {"source": "greenhouse", "value": 150000, "confidence": 0.90},
        ],
    }
    db.upsert_job(conn, j)
    row = conn.execute(
        "SELECT field_sources FROM jobs WHERE external_id=?", ("fs_1",)
    ).fetchone()
    assert row[0] is not None
    loaded = json.loads(row[0])
    assert len(loaded["is_remote"]) == 2
    assert loaded["is_remote"][1]["source"] == "greenhouse"
    assert loaded["salary_min"][0]["value"] == 150000


def test_upsert_field_sources_column_null_when_absent(conn):
    """Jobs without provenance (legacy code paths, bypass sources) don't
    crash — the column stays NULL rather than serializing None or {}."""
    j = sample_job("fs_nullable")
    # No _field_sources key at all
    assert "_field_sources" not in j
    db.upsert_job(conn, j)
    row = conn.execute(
        "SELECT field_sources FROM jobs WHERE external_id=?", ("fs_nullable",)
    ).fetchone()
    assert row[0] is None
