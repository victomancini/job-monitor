"""Tests for src/collector.py — full pipeline with mocks. No real API calls."""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src import collector, db


REQUIRED = [
    "JSEARCH_API_KEY", "JOOBLE_API_KEY", "GROQ_API_KEY", "GEMINI_API_KEY",
    "TURSO_DB_URL", "TURSO_AUTH_TOKEN",
    "WP_URL", "WP_USERNAME", "WP_APP_PASSWORD",
    "HEALTHCHECK_URL",
]


@pytest.fixture
def env_ok(monkeypatch, tmp_path):
    for v in REQUIRED:
        monkeypatch.setenv(v, "x")
    # Send shadow_log to tmp
    monkeypatch.setattr(collector, "SHADOW_LOG_PATH", tmp_path / "shadow.jsonl")


@pytest.fixture
def conn(monkeypatch):
    c = sqlite3.connect(":memory:")
    db.migrate(c)
    monkeypatch.setattr(collector.db, "connect", lambda *a, **kw: c)
    yield c
    c.close()


def _sample_job(ext_id="jsearch_1", title="People Analytics Manager", company="Netflix",
                source="jsearch"):
    return {
        "external_id": ext_id, "title": title, "company": company,
        "location": "Los Gatos, CA", "location_country": "US",
        "source_url": "https://example.com/1", "source_name": source,
        "description": "Lead people analytics team", "description_is_snippet": False,
        "salary_min": 150000, "salary_max": 200000, "salary_range": "$150K-$200K",
        "is_remote": "hybrid", "work_arrangement": "Hybrid",
        "date_posted": "2026-04-14", "raw_data": None,
    }


# ──────────────── Pre-flight validation ────────────────

def test_missing_env_exits_1(monkeypatch):
    for v in REQUIRED:
        monkeypatch.delenv(v, raising=False)
    assert collector.run() == 1


def test_monday_detection():
    with patch("src.collector.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 4, 13, tzinfo=timezone.utc)  # Monday
        assert collector._is_monday() is True
    with patch("src.collector.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 4, 14, tzinfo=timezone.utc)  # Tuesday
        assert collector._is_monday() is False


# ──────────────── Source collection ────────────────

def test_collect_sources_aggregates_all(env_ok, monkeypatch):
    monkeypatch.setattr(collector.jsearch, "fetch",
                        lambda k: ([_sample_job("jsearch_1")], [], {"quota_remaining": 150}))
    monkeypatch.setattr(collector.jooble, "fetch",
                        lambda k: ([_sample_job("jooble_1", source="jooble")], [], {}))
    monkeypatch.setattr(collector.adzuna, "fetch",
                        lambda a, b: ([_sample_job("adzuna_1", source="adzuna")], [], {}))
    monkeypatch.setattr(collector.google_alerts, "fetch",
                        lambda: ([], [], {"stale_feeds": ["https://x"]}))
    # Not monday — usajobs skipped
    monkeypatch.setattr(collector, "_is_monday", lambda: False)

    jobs, counts, errors, meta = collector.collect_sources()
    assert len(jobs) == 3
    assert counts == {"jsearch_found": 1, "jooble_found": 1, "adzuna_found": 1,
                      "usajobs_found": 0, "alerts_found": 0}
    assert meta["jsearch_quota_remaining"] == 150
    assert meta["usajobs_skipped_not_monday"] is True
    assert meta["stale_feeds"] == ["https://x"]


def test_collect_sources_runs_usajobs_on_monday(env_ok, monkeypatch):
    monkeypatch.setattr(collector.jsearch, "fetch", lambda k: ([], [], {}))
    monkeypatch.setattr(collector.jooble, "fetch", lambda k: ([], [], {}))
    monkeypatch.setattr(collector.adzuna, "fetch", lambda a, b: ([], [], {}))
    monkeypatch.setattr(collector.google_alerts, "fetch", lambda: ([], [], {}))
    usajobs_mock = MagicMock(return_value=([_sample_job("usajobs_1", source="usajobs")], [], {}))
    monkeypatch.setattr(collector.usajobs, "fetch", usajobs_mock)
    monkeypatch.setattr(collector, "_is_monday", lambda: True)

    jobs, counts, errors, meta = collector.collect_sources()
    usajobs_mock.assert_called_once()
    assert counts["usajobs_found"] == 1


# ──────────────── Keyword filter stage ────────────────

def test_apply_keyword_filter_routes(env_ok):
    good = _sample_job("g", title="People Analytics Manager")
    bad = {
        "external_id": "b", "title": "Customer Service Rep",
        "company": "Unknown Company LLC", "source_name": "jsearch",
        "description": "Handle customer calls. Requires active listening.",
        "location": "Remote", "source_url": "https://x",
    }
    candidates, rejects = collector.apply_keyword_filter([good, bad])
    assert len(candidates) == 1 and candidates[0]["external_id"] == "g"
    assert len(rejects) == 1


# ──────────────── LLM stage ────────────────

def test_apply_llm_classifies_and_filters(env_ok, monkeypatch):
    j1 = _sample_job("a", title="People Analytics Manager")
    j2 = _sample_job("b", title="People Analytics Manager")
    # classify_batch mutates jobs with llm_* fields
    def fake_batch(jobs, **kw):
        jobs[0]["llm_classification"] = "RELEVANT"
        jobs[0]["llm_confidence"] = 90
        jobs[0]["llm_provider"] = "groq"
        jobs[1]["llm_classification"] = "NOT_RELEVANT"
        jobs[1]["llm_confidence"] = 85
        jobs[1]["llm_provider"] = "groq"
        return [], {"groq": 2}
    monkeypatch.setattr(collector.llm_classifier, "classify_batch", fake_batch)
    to_publish, counts, errors = collector.apply_llm([j1, j2])
    assert len(to_publish) == 1
    assert to_publish[0]["external_id"] == "a"
    assert counts == {"groq": 2}


def test_apply_llm_empty_candidates(env_ok):
    result = collector.apply_llm([])
    assert result == ([], {}, [])


# ──────────────── Full pipeline (dry-run) ────────────────

def test_full_pipeline_dry_run(env_ok, conn, monkeypatch):
    monkeypatch.setattr(collector, "collect_sources", lambda: (
        [_sample_job("j1"), _sample_job("j2", title="Customer Service Rep")],
        {"jsearch_found": 2, "jooble_found": 0, "adzuna_found": 0, "usajobs_found": 0, "alerts_found": 0},
        [], {},
    ))

    def fake_batch(jobs, **kw):
        for j in jobs:
            j["llm_classification"] = "RELEVANT"
            j["llm_confidence"] = 90
            j["llm_provider"] = "groq"
        return [], {"groq": len(jobs)}
    monkeypatch.setattr(collector.llm_classifier, "classify_batch", fake_batch)

    wp_mock = MagicMock()
    monkeypatch.setattr(collector.wordpress, "publish", wp_mock)
    monkeypatch.setattr(collector.wordpress, "process_retry_queue", wp_mock)
    notify_mock = MagicMock()
    monkeypatch.setattr(collector.notifier, "notify", notify_mock)
    hc_mock = MagicMock()
    monkeypatch.setattr(collector, "ping_healthcheck", hc_mock)

    rc = collector.run(dry_run=True)
    assert rc == 0
    # DRY-RUN: wordpress + notifier must NOT be called
    wp_mock.assert_not_called()
    notify_mock.assert_not_called()
    # Archiver + healthcheck SHOULD be called
    hc_mock.assert_called_once()


def test_full_pipeline_publishes_when_not_dry(env_ok, conn, monkeypatch):
    monkeypatch.setattr(collector, "collect_sources", lambda: (
        [_sample_job("j1")],
        {"jsearch_found": 1, "jooble_found": 0, "adzuna_found": 0, "usajobs_found": 0, "alerts_found": 0},
        [], {},
    ))

    def fake_batch(jobs, **kw):
        for j in jobs:
            j["llm_classification"] = "RELEVANT"
            j["llm_confidence"] = 90
            j["llm_provider"] = "groq"
        return [], {"groq": len(jobs)}
    monkeypatch.setattr(collector.llm_classifier, "classify_batch", fake_batch)

    wp_publish = MagicMock(return_value={"created": 1, "updated": 0, "errors": 0, "queued": 0, "batches": 1})
    wp_retry = MagicMock(return_value={"attempted": 0, "succeeded": 0, "failed": 0, "dropped": 0})
    monkeypatch.setattr(collector.wordpress, "publish", wp_publish)
    monkeypatch.setattr(collector.wordpress, "process_retry_queue", wp_retry)
    monkeypatch.setattr(collector.notifier, "notify",
                        MagicMock(return_value={"qualifying": 1, "pushes_sent": 1, "email_sent": 1}))
    monkeypatch.setattr(collector, "ping_healthcheck", MagicMock())

    rc = collector.run(dry_run=False)
    assert rc == 0
    wp_publish.assert_called_once()
    wp_retry.assert_called_once()


# ──────────────── Zero-results canary ────────────────

def test_zero_results_triggers_after_2_consecutive(env_ok, conn, monkeypatch):
    # Pre-seed run_log with one prior zero run
    db.log_run(conn, {
        "run_date": "2026-04-15",
        "jsearch_found": 0, "jooble_found": 0, "adzuna_found": 0,
        "usajobs_found": 0, "alerts_found": 0,
        "total_passed_filter": 0, "total_published": 0, "total_archived": 0,
        "errors": "", "llm_provider_used": "none", "duration_seconds": 1.0,
        "consecutive_zero_runs": 1,
    })
    monkeypatch.setattr(collector, "collect_sources", lambda: (
        [],
        {"jsearch_found": 0, "jooble_found": 0, "adzuna_found": 0, "usajobs_found": 0, "alerts_found": 0},
        [], {},
    ))
    alert_mock = MagicMock()
    monkeypatch.setattr(collector, "_alert_zero_results", alert_mock)
    monkeypatch.setattr(collector, "ping_healthcheck", MagicMock())

    rc = collector.run(dry_run=True)
    assert rc == 1  # canary tripped → non-zero so workflow-level ping also fails
    alert_mock.assert_called_once_with(2)


def test_single_zero_run_does_not_alert(env_ok, conn, monkeypatch):
    monkeypatch.setattr(collector, "collect_sources", lambda: (
        [],
        {"jsearch_found": 0, "jooble_found": 0, "adzuna_found": 0, "usajobs_found": 0, "alerts_found": 0},
        [], {},
    ))
    alert_mock = MagicMock()
    monkeypatch.setattr(collector, "_alert_zero_results", alert_mock)
    monkeypatch.setattr(collector, "ping_healthcheck", MagicMock())

    collector.run(dry_run=True)
    alert_mock.assert_not_called()


# ──────────────── Healthcheck ping body ────────────────

def test_healthcheck_ping_posts_rich_body():
    with patch("src.collector.retry_request") as mock_req:
        collector.ping_healthcheck(
            "https://hc-ping.com/abc",
            success=True,
            counts={"jsearch_found": 5, "jooble_found": 3, "adzuna_found": 0,
                    "usajobs_found": 0, "alerts_found": 1},
            errors=["jsearch: minor glitch"],
            published=4, archived=1, duration_s=12.5,
            provider_counts={"groq": 4},
            meta={"jsearch_quota_remaining": 120},
        )
    mock_req.assert_called_once()
    kwargs = mock_req.call_args.kwargs
    body = kwargs["json"]
    assert body["jsearch_found"] == 5
    assert body["total_published"] == 4
    assert body["llm_providers"] == {"groq": 4}
    assert body["jsearch_quota_remaining"] == 120
    # Success endpoint — no /fail suffix
    assert mock_req.call_args.args[1] == "https://hc-ping.com/abc"


def test_healthcheck_ping_fail_endpoint_on_failure():
    with patch("src.collector.retry_request") as mock_req:
        collector.ping_healthcheck(
            "https://hc-ping.com/abc",
            success=False, counts={}, errors=[], published=0, archived=0,
            duration_s=1.0, provider_counts={}, meta={},
        )
    assert mock_req.call_args.args[1] == "https://hc-ping.com/abc/fail"


def test_healthcheck_ping_no_url_is_noop():
    with patch("src.collector.retry_request") as mock_req:
        collector.ping_healthcheck(
            "", success=True, counts={}, errors=[], published=0, archived=0,
            duration_s=1.0, provider_counts={}, meta={},
        )
    mock_req.assert_not_called()


# ──────────────── Shadow log ────────────────

def test_shadow_log_writes_jsonl(env_ok, tmp_path, monkeypatch):
    path = tmp_path / "shadow.jsonl"
    monkeypatch.setattr(collector, "SHADOW_LOG_PATH", path)
    collector._shadow_log({"stage": "test", "external_id": "x"})
    assert path.exists()
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 1
    import json
    record = json.loads(lines[0])
    assert record["stage"] == "test"
    assert record["external_id"] == "x"
    assert "t" in record


def test_shadow_log_never_raises(env_ok, monkeypatch):
    """Shadow log write failure must not bubble up."""
    monkeypatch.setattr(collector, "SHADOW_LOG_PATH", "/nonexistent_path_xxx/shadow.jsonl")
    # Should not raise
    collector._shadow_log({"stage": "test"})
