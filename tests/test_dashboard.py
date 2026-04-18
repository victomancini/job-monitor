"""Tests for Phase 8 (R3) dashboard publisher + payload builder."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src import db as dbmod
from src.processors import stats_aggregator
from src.publishers import wordpress


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


# ──────────────────────── Payload builder ──────────────────

def test_build_dashboard_payload_shape(conn):
    _upsert(conn, ext_id="a", category="People Analytics", seniority="Manager",
            is_remote="remote", vendors_mentioned="Qualtrics,SQL", salary_min=120000)
    _upsert(conn, ext_id="b", category="Employee Listening", seniority="Director",
            is_remote="hybrid", vendors_mentioned="Medallia,Python", salary_min=180000)
    stats_aggregator.aggregate_daily_stats(conn, today="2026-04-18")

    payload = stats_aggregator.build_dashboard_payload(conn, today="2026-04-18")
    assert payload["snapshot_date"] == "2026-04-18"
    assert "category_count" in payload
    assert "seniority_count" in payload
    assert "remote_count" in payload
    assert "company_count" in payload
    assert isinstance(payload["company_count"], list)
    assert "vendor_count" in payload
    assert isinstance(payload["vendor_count"], list)
    assert "total_active_trend" in payload
    assert payload["total_active_trend"][-1]["count"] == 2  # today's value


def test_build_dashboard_payload_trend_chronological(conn):
    # Seed 3 days of total_active stats
    for date, val in [("2026-04-16", 10), ("2026-04-17", 15), ("2026-04-18", 20)]:
        stats_aggregator._upsert_stat(conn, date, "total_active", "all", val)
    conn.commit()
    payload = stats_aggregator.build_dashboard_payload(conn, today="2026-04-18")
    trend = payload["total_active_trend"]
    assert [p["date"] for p in trend] == ["2026-04-16", "2026-04-17", "2026-04-18"]
    assert [p["count"] for p in trend] == [10, 15, 20]


def test_build_dashboard_payload_limits_top_lists(conn):
    # 15 distinct companies, payload should limit company_count to top 10
    for i in range(15):
        stats_aggregator._upsert_stat(conn, "2026-04-18", "company_count", f"Co{i}", 20 - i)
    conn.commit()
    payload = stats_aggregator.build_dashboard_payload(conn, today="2026-04-18")
    assert len(payload["company_count"]) == 10


# ──────────────────────── Publisher ────────────────────────

def _mock_resp(body, status=200):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = body
    m.text = str(body)
    return m


def test_publish_dashboard_stats_posts_json():
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured["url"] = url
        captured["json"] = json
        return _mock_resp({"ok": True})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        result = wordpress.publish_dashboard_stats(
            {"snapshot_date": "2026-04-18", "category_count": {"X": 1}},
            wp_url="https://site.com",
            username="u",
            app_password="p",
        )
    assert result["ok"] is True
    assert result["status"] == 200
    assert captured["url"].endswith("/wp-json/jobmonitor/v1/dashboard-stats")
    assert captured["json"]["snapshot_date"] == "2026-04-18"


def test_publish_dashboard_stats_handles_missing_creds():
    result = wordpress.publish_dashboard_stats(
        {"snapshot_date": "x"},
        wp_url="", username="", app_password="",
    )
    assert result["ok"] is False


def test_publish_dashboard_stats_handles_transport_error():
    with patch("src.publishers.wordpress.retry_request", side_effect=Exception("boom")):
        result = wordpress.publish_dashboard_stats(
            {"snapshot_date": "x"},
            wp_url="https://s", username="u", app_password="p",
        )
    assert result["ok"] is False
    assert result["status"] is None
