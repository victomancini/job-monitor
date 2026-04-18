"""Tests for Phase 7 (R3) stats aggregator."""
from __future__ import annotations

import sqlite3

import pytest

from src import db as dbmod
from src.processors import stats_aggregator


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    dbmod.migrate(c)
    yield c
    c.close()


def _upsert(conn, **fields):
    base = {
        "external_id": fields.pop("ext_id", "x"),
        "title": "T",
        "company": "Netflix",
        "source_name": "jsearch",
    }
    base.update(fields)
    dbmod.upsert_job(conn, base)


# ──────────────────────── schema ────────────────────────

def test_monthly_stats_table_exists(conn):
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "monthly_stats" in tables


def test_monthly_stats_unique_constraint(conn):
    stats_aggregator._upsert_stat(conn, "2026-04-18", "test", "key", 5)
    stats_aggregator._upsert_stat(conn, "2026-04-18", "test", "key", 99)  # overwrite
    conn.commit()
    rows = conn.execute(
        "SELECT stat_value FROM monthly_stats WHERE stat_date=? AND stat_type=? AND stat_key=?",
        ("2026-04-18", "test", "key"),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 99


# ──────────────────────── aggregations ────────────────────

def test_category_and_seniority_counts(conn):
    _upsert(conn, ext_id="a", category="Employee Listening", seniority="Manager")
    _upsert(conn, ext_id="b", category="Employee Listening", seniority="Director")
    _upsert(conn, ext_id="c", category="People Analytics", seniority="Manager")
    result = stats_aggregator.aggregate_daily_stats(conn, today="2026-04-18")
    assert result["total_active"] == 3
    # Category slice
    rows = {r[0]: r[1] for r in conn.execute(
        "SELECT stat_key, stat_value FROM monthly_stats "
        "WHERE stat_date='2026-04-18' AND stat_type='category_count'"
    ).fetchall()}
    assert rows["Employee Listening"] == 2
    assert rows["People Analytics"] == 1
    # Seniority slice
    rows = {r[0]: r[1] for r in conn.execute(
        "SELECT stat_key, stat_value FROM monthly_stats "
        "WHERE stat_date='2026-04-18' AND stat_type='seniority_count'"
    ).fetchall()}
    assert rows["Manager"] == 2
    assert rows["Director"] == 1


def test_vendor_counts_from_csv(conn):
    _upsert(conn, ext_id="a", vendors_mentioned="Qualtrics,Python,SQL")
    _upsert(conn, ext_id="b", vendors_mentioned="Qualtrics,Tableau")
    _upsert(conn, ext_id="c", vendors_mentioned="")
    stats_aggregator.aggregate_daily_stats(conn, today="2026-04-18")
    rows = {r[0]: r[1] for r in conn.execute(
        "SELECT stat_key, stat_value FROM monthly_stats "
        "WHERE stat_date='2026-04-18' AND stat_type='vendor_count'"
    ).fetchall()}
    assert rows["Qualtrics"] == 2
    assert rows["Python"] == 1
    assert rows["Tableau"] == 1


def test_salary_percentiles_require_three_samples(conn):
    # 2 samples at same seniority — not enough for quantiles
    _upsert(conn, ext_id="a", seniority="Manager", salary_min=120000)
    _upsert(conn, ext_id="b", seniority="Manager", salary_min=140000)
    # 3 samples at Director — enough
    _upsert(conn, ext_id="c", seniority="Director", salary_min=200000)
    _upsert(conn, ext_id="d", seniority="Director", salary_min=240000)
    _upsert(conn, ext_id="e", seniority="Director", salary_min=280000)
    stats_aggregator.aggregate_daily_stats(conn, today="2026-04-18")

    manager_rows = conn.execute(
        "SELECT stat_type FROM monthly_stats "
        "WHERE stat_date='2026-04-18' AND stat_key='Manager' "
        "AND stat_type LIKE 'salary_%'"
    ).fetchall()
    director_rows = conn.execute(
        "SELECT stat_type FROM monthly_stats "
        "WHERE stat_date='2026-04-18' AND stat_key='Director' "
        "AND stat_type LIKE 'salary_%'"
    ).fetchall()
    assert manager_rows == []  # only 2 samples, no percentiles
    assert {r[0] for r in director_rows} == {"salary_p25", "salary_p50", "salary_p75"}


def test_total_active_count_written(conn):
    _upsert(conn, ext_id="a")
    _upsert(conn, ext_id="b")
    _upsert(conn, ext_id="c")
    # Archive one to make sure inactive rows are excluded
    conn.execute("UPDATE jobs SET is_active=0 WHERE external_id='c'")
    conn.commit()
    stats_aggregator.aggregate_daily_stats(conn, today="2026-04-18")
    row = conn.execute(
        "SELECT stat_value FROM monthly_stats "
        "WHERE stat_date='2026-04-18' AND stat_type='total_active' AND stat_key='all'"
    ).fetchone()
    assert row[0] == 2


def test_rerun_same_day_upserts_not_duplicates(conn):
    _upsert(conn, ext_id="a", category="X")
    stats_aggregator.aggregate_daily_stats(conn, today="2026-04-18")
    # Run a second time — should upsert without error
    stats_aggregator.aggregate_daily_stats(conn, today="2026-04-18")
    count = conn.execute(
        "SELECT COUNT(*) FROM monthly_stats "
        "WHERE stat_date='2026-04-18' AND stat_type='total_active'"
    ).fetchone()[0]
    assert count == 1
