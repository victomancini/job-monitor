"""Tests for Phase 6 (R3) lifecycle tracking: active → likely_closed → archived."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from src import db as dbmod
from src.publishers import archiver


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    dbmod.migrate(c)
    yield c
    c.close()


def _sample(ext_id: str, **overrides):
    base = {
        "external_id": ext_id,
        "title": "People Analytics Manager",
        "company": "Netflix",
        "source_name": "jsearch",
    }
    base.update(overrides)
    return base


def _set_last_seen(conn, ext_id: str, days_ago: int) -> None:
    when = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    conn.execute("UPDATE jobs SET last_seen_date=? WHERE external_id=?", (when, ext_id))
    conn.commit()


# ──────────────────────── Schema ────────────────────────────

def test_lifecycle_status_defaults_to_active(conn):
    dbmod.upsert_job(conn, _sample("j1"))
    row = conn.execute(
        "SELECT lifecycle_status FROM jobs WHERE external_id=?", ("j1",)
    ).fetchone()
    assert row[0] == "active"


def test_upsert_resets_lifecycle_to_active_on_reseen(conn):
    dbmod.upsert_job(conn, _sample("j1"))
    # Simulate the staleness pass having flagged this job
    dbmod.mark_job_likely_closed(conn, "j1")
    row = conn.execute(
        "SELECT lifecycle_status FROM jobs WHERE external_id=?", ("j1",)
    ).fetchone()
    assert row[0] == "likely_closed"
    # Source re-reports the job → upsert must reset lifecycle
    dbmod.upsert_job(conn, _sample("j1"))
    row = conn.execute(
        "SELECT lifecycle_status FROM jobs WHERE external_id=?", ("j1",)
    ).fetchone()
    assert row[0] == "active"


# ──────────────────────── Staleness pass (step 1) ───────────

def test_mark_likely_closed_at_7_days(conn):
    dbmod.upsert_job(conn, _sample("fresh"))
    dbmod.upsert_job(conn, _sample("stale1"))
    dbmod.upsert_job(conn, _sample("stale2"))
    _set_last_seen(conn, "stale1", days_ago=8)
    _set_last_seen(conn, "stale2", days_ago=10)

    result = archiver.mark_likely_closed(conn)
    assert result == {"marked_likely_closed": 2}

    rows = {
        r[0]: r[1]
        for r in conn.execute("SELECT external_id, lifecycle_status FROM jobs").fetchall()
    }
    assert rows["fresh"] == "active"
    assert rows["stale1"] == "likely_closed"
    assert rows["stale2"] == "likely_closed"


def test_mark_likely_closed_skips_already_marked(conn):
    """Once a job is likely_closed, mark_likely_closed shouldn't rewrite it."""
    dbmod.upsert_job(conn, _sample("x"))
    _set_last_seen(conn, "x", days_ago=8)
    archiver.mark_likely_closed(conn)
    # Second call finds no fresh candidates (all already marked)
    result = archiver.mark_likely_closed(conn)
    assert result == {"marked_likely_closed": 0}


# ──────────────────────── Archive (step 2) ──────────────────

def test_archive_stale_marks_21_day_old_as_inactive(conn):
    dbmod.upsert_job(conn, _sample("oldies"))
    _set_last_seen(conn, "oldies", days_ago=22)
    result = archiver.archive_stale(conn)
    assert result.get("archived") == 1
    row = conn.execute(
        "SELECT is_active, archived_date FROM jobs WHERE external_id=?", ("oldies",)
    ).fetchone()
    assert row[0] == 0
    assert row[1]  # archived_date is set


def test_archive_stale_runs_step1_then_step2(conn):
    """Single call to archive_stale should both flag 7-day jobs and archive 21-day jobs."""
    dbmod.upsert_job(conn, _sample("recent"))
    dbmod.upsert_job(conn, _sample("stale"))
    dbmod.upsert_job(conn, _sample("ancient"))
    _set_last_seen(conn, "stale", days_ago=10)
    _set_last_seen(conn, "ancient", days_ago=25)

    result = archiver.archive_stale(conn)
    assert result["marked_likely_closed"] == 2  # "stale" + "ancient" both pass step 1
    assert result["archived"] == 1  # only "ancient" passes step 2

    rows = {
        r[0]: (r[1], r[2])
        for r in conn.execute(
            "SELECT external_id, lifecycle_status, is_active FROM jobs"
        ).fetchall()
    }
    assert rows["recent"] == ("active", 1)
    assert rows["stale"] == ("likely_closed", 1)
    assert rows["ancient"][1] == 0  # archived


def test_archive_stale_skips_fresh(conn):
    dbmod.upsert_job(conn, _sample("recent"))
    # No date mutation — default last_seen_date is today
    result = archiver.archive_stale(conn)
    assert result["archived"] == 0
    assert result["marked_likely_closed"] == 0


# ──────────────────────── Publisher propagation ─────────────

def test_wp_fields_includes_lifecycle_status():
    from src.publishers.wordpress import _WP_FIELDS
    assert "lifecycle_status" in _WP_FIELDS
