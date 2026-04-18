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
    # Phase 2 (R3) — ATS sources are direct-HTTP per slug; tests must not hit network.
    monkeypatch.setattr(collector.greenhouse, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.lever, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.ashby, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.jobspy_source, "fetch", lambda **kw: ([], [], {"available": False}))
    monkeypatch.setattr(collector.onemodel, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.included_ai, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.siop, "fetch", lambda **kw: ([], [], {}))
    # Not monday — usajobs skipped
    monkeypatch.setattr(collector, "_is_monday", lambda: False)

    jobs, counts, errors, meta = collector.collect_sources()
    assert len(jobs) == 3
    assert counts == {
        "jsearch_found": 1, "jooble_found": 1, "adzuna_found": 1,
        "usajobs_found": 0, "alerts_found": 0,
        "greenhouse_found": 0, "lever_found": 0, "ashby_found": 0,
        "jobspy_found": 0,
        "onemodel_found": 0, "included_ai_found": 0, "siop_found": 0,
    }
    assert meta["jsearch_quota_remaining"] == 150
    assert meta["usajobs_skipped_not_monday"] is True
    assert meta["stale_feeds"] == ["https://x"]


def test_collect_sources_runs_usajobs_on_monday(env_ok, monkeypatch):
    monkeypatch.setattr(collector.jsearch, "fetch", lambda k: ([], [], {}))
    monkeypatch.setattr(collector.jooble, "fetch", lambda k: ([], [], {}))
    monkeypatch.setattr(collector.adzuna, "fetch", lambda a, b: ([], [], {}))
    monkeypatch.setattr(collector.google_alerts, "fetch", lambda: ([], [], {}))
    monkeypatch.setattr(collector.greenhouse, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.lever, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.ashby, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.jobspy_source, "fetch", lambda **kw: ([], [], {"available": False}))
    monkeypatch.setattr(collector.onemodel, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.included_ai, "fetch", lambda **kw: ([], [], {}))
    monkeypatch.setattr(collector.siop, "fetch", lambda **kw: ([], [], {}))
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
    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
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

    # Patch enrichment so tests don't hit real URLs
    monkeypatch.setattr(collector.enrichment, "enrich_batch",
                        lambda jobs, **kw: jobs)

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
    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
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

    monkeypatch.setattr(collector.enrichment, "enrich_batch",
                        lambda jobs, **kw: jobs)
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
    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
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
    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
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


# ──────────────── Phase E: seniority + enrichment integration ────────

def test_apply_seniority_regex_only():
    jobs = [
        {"title": "Senior Manager, People Analytics"},
        {"title": "Principal People Scientist"},
        {"title": "Random Role"},
    ]
    collector.apply_seniority(jobs)
    assert jobs[0]["seniority"] == "Senior Manager"
    assert jobs[1]["seniority"] == "Senior IC"
    assert jobs[2]["seniority"] == "Unknown"


def test_apply_seniority_llm_override_wins():
    jobs = [
        {"title": "People Analytics Manager", "_llm_seniority": "Director"},
    ]
    collector.apply_seniority(jobs)
    assert jobs[0]["seniority"] == "Director"  # LLM wins over regex "Manager"


def test_apply_seniority_salary_fallback_when_title_unknown():
    """Phase F (R2): regex returns Unknown, no LLM hint, but salary_min is $175K →
    infer Senior Manager and mark seniority_confidence='inferred'."""
    jobs = [{"title": "Opaque Job Title", "salary_min": 175_000}]
    collector.apply_seniority(jobs)
    assert jobs[0]["seniority"] == "Senior Manager"
    assert jobs[0]["seniority_confidence"] == "inferred"


def test_apply_seniority_salary_fallback_skipped_when_title_wins():
    """Title regex wins; salary fallback doesn't fire."""
    jobs = [{"title": "Senior Manager, People Analytics", "salary_min": 60_000}]
    collector.apply_seniority(jobs)
    assert jobs[0]["seniority"] == "Senior Manager"
    assert "seniority_confidence" not in jobs[0]  # regex match, not inferred


def test_apply_seniority_intern_from_title():
    jobs = [{"title": "People Analytics Intern"}]
    collector.apply_seniority(jobs)
    assert jobs[0]["seniority"] == "Intern"


def test_apply_vendor_mentions_extracts_from_description():
    jobs = [
        {"description": "Experience with Qualtrics and Medallia required. Python, SQL."},
        {"description": ""},
        {},  # no description
    ]
    collector.apply_vendor_mentions(jobs)
    assert "Qualtrics" in jobs[0]["vendors_mentioned"]
    assert "Medallia" in jobs[0]["vendors_mentioned"]
    assert "Python" in jobs[0]["vendors_mentioned"]
    assert "SQL" in jobs[0]["vendors_mentioned"]
    assert jobs[1]["vendors_mentioned"] == ""
    assert jobs[2]["vendors_mentioned"] == ""


def test_apply_category_assigns_per_job():
    jobs = [
        {"title": "Employee Listening Manager", "company": "Netflix", "description": ""},
        {"title": "HRIS Analyst", "company": "Deloitte", "description": ""},
        {"title": "Senior Associate", "company": "Deloitte", "description": ""},
        {"title": "Random Role", "company": "Random Corp", "description": ""},
    ]
    collector.apply_category(jobs)
    assert jobs[0]["category"] == "Employee Listening"
    assert jobs[1]["category"] == "HRIS & Systems"
    assert jobs[2]["category"] == "Consulting"
    assert jobs[3]["category"] == "General PA"


def test_apply_enrichment_returns_stats(monkeypatch):
    def fake_enrich_batch(jobs, **kw):
        jobs[0]["enrichment_source"] = "source_page"
        jobs[1]["enrichment_source"] = "aggregator"
        return jobs
    monkeypatch.setattr(collector.enrichment, "enrich_batch", fake_enrich_batch)
    jobs = [{"external_id": "a"}, {"external_id": "b"}]
    stats = collector.apply_enrichment(jobs)
    assert stats == {"enriched_from_source": 1, "aggregator_only": 1}


def test_apply_defaults_sets_onsite_assumed():
    """Phase 1 (R3): unknown is_remote gets onsite/assumed after all other passes."""
    jobs = [
        {"external_id": "a", "is_remote": "unknown"},
        {"external_id": "b", "is_remote": ""},
        {"external_id": "c"},
        {"external_id": "d", "is_remote": "remote"},  # already set — no change
    ]
    collector.apply_defaults(jobs)
    assert jobs[0]["is_remote"] == "onsite"
    assert jobs[0]["remote_confidence"] == "assumed"
    assert jobs[1]["is_remote"] == "onsite"
    assert jobs[2]["is_remote"] == "onsite"
    assert jobs[3]["is_remote"] == "remote"
    assert "remote_confidence" not in jobs[3]


def test_apply_enrichment_empty_short_circuits(monkeypatch):
    called = MagicMock()
    monkeypatch.setattr(collector.enrichment, "enrich_batch", called)
    stats = collector.apply_enrichment([])
    assert stats == {"enriched_from_source": 0, "aggregator_only": 0}
    called.assert_not_called()


def test_pipeline_enrichment_runs_between_dedup_and_publish(env_ok, conn, monkeypatch):
    """Order check: deduplicator.deduplicate must be called BEFORE enrichment.enrich_batch,
    and enrichment BEFORE wordpress.publish."""
    call_order: list[str] = []

    def spy(name, real):
        def wrapper(*a, **kw):
            call_order.append(name)
            return real(*a, **kw)
        return wrapper

    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
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

    def fake_dedupe(jobs, active_db_rows=None):
        call_order.append("dedupe")
        return jobs, []
    monkeypatch.setattr(collector.deduplicator, "deduplicate", fake_dedupe)

    def fake_enrich_batch(jobs, **kw):
        call_order.append("enrich")
        return jobs
    monkeypatch.setattr(collector.enrichment, "enrich_batch", fake_enrich_batch)

    wp_publish = MagicMock(return_value={"created": 1, "updated": 0, "errors": 0, "queued": 0, "batches": 1},
                           side_effect=lambda *a, **kw: (
                               call_order.append("publish"),
                               {"created": 1, "updated": 0, "errors": 0, "queued": 0, "batches": 1},
                           )[1])
    monkeypatch.setattr(collector.wordpress, "publish", wp_publish)
    monkeypatch.setattr(collector.wordpress, "process_retry_queue",
                        MagicMock(return_value={"attempted": 0, "succeeded": 0, "failed": 0, "dropped": 0}))
    monkeypatch.setattr(collector.notifier, "notify",
                        MagicMock(return_value={"qualifying": 0, "pushes_sent": 0, "email_sent": 0}))
    monkeypatch.setattr(collector, "ping_healthcheck", MagicMock())

    collector.run(dry_run=False)

    assert call_order.index("dedupe") < call_order.index("enrich")
    assert call_order.index("enrich") < call_order.index("publish")


def test_pipeline_seniority_populated_on_output(env_ok, conn, monkeypatch):
    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
        [_sample_job("j1", title="Senior Manager, People Analytics")],
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
    monkeypatch.setattr(collector.enrichment, "enrich_batch",
                        lambda jobs, **kw: jobs)
    monkeypatch.setattr(collector, "ping_healthcheck", MagicMock())

    collector.run(dry_run=True)

    # Verify seniority landed in DB for the published job
    row = conn.execute("SELECT seniority FROM jobs WHERE external_id=?", ("j1",)).fetchone()
    assert row[0] == "Senior Manager"


def test_pipeline_enrichment_stats_in_healthcheck_meta(env_ok, conn, monkeypatch):
    # Two distinct jobs so the deduplicator keeps both
    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
        [
            _sample_job("j1", title="People Analytics Manager", company="Netflix"),
            _sample_job("j2", title="Employee Listening Director", company="Atlassian"),
        ],
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

    def fake_enrich_batch(jobs, **kw):
        if jobs:
            jobs[0]["enrichment_source"] = "source_page"
        if len(jobs) > 1:
            jobs[1]["enrichment_source"] = "aggregator"
        return jobs
    monkeypatch.setattr(collector.enrichment, "enrich_batch", fake_enrich_batch)

    hc_mock = MagicMock()
    monkeypatch.setattr(collector, "ping_healthcheck", hc_mock)

    collector.run(dry_run=True)

    meta = hc_mock.call_args.kwargs["meta"]
    assert meta["enriched_from_source"] == 1
    assert meta["aggregator_only"] == 1


# ──────────────── Phase G: end-to-end integration ────────────────

def test_pipeline_r2_fields_persist_to_db_and_wp_payload(env_ok, conn, monkeypatch):
    """Phase G (R2) integration: verify Phase A-F fields flow collector → Turso → WP.
    Covers: apply_url upgrade by dedup, location merge, seniority salary fallback,
    date_posted, Relevance (llm_classification)."""
    # Two same-company duplicates across cities (Phase E) + aggregator vs direct URL (Phase A)
    job1 = _sample_job("a", title="Opaque Role", company="Deloitte",
                       source="jsearch")
    job1["apply_url"] = "https://jooble.org/desc/a"  # aggregator URL
    job1["location"] = "New York, NY"
    job1["salary_min"] = 175_000
    job1["salary_max"] = 225_000
    job1["date_posted"] = "2026-04-14"
    job2 = _sample_job("b", title="Opaque Role", company="Deloitte",
                       source="adzuna")
    job2["apply_url"] = "https://careers.deloitte.com/jobs/b"  # direct
    job2["location"] = "Chicago, IL"
    job2["salary_min"] = 175_000
    job2["salary_max"] = 225_000
    job2["date_posted"] = "2026-04-14"

    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
        [job1, job2],
        {"jsearch_found": 1, "jooble_found": 0, "adzuna_found": 1,
         "usajobs_found": 0, "alerts_found": 0},
        [], {},
    ))

    def fake_batch(jobs, **kw):
        for j in jobs:
            j["llm_classification"] = "RELEVANT"
            j["llm_confidence"] = 90
            j["llm_provider"] = "groq"
        return [], {"groq": len(jobs)}
    monkeypatch.setattr(collector.llm_classifier, "classify_batch", fake_batch)

    monkeypatch.setattr(collector.enrichment, "enrich_batch",
                        lambda jobs, **kw: jobs)

    captured: list[dict] = []
    def fake_publish(jobs, **kw):
        from src.publishers.wordpress import _payload
        captured.extend(_payload(j) for j in jobs)
        return {"created": len(jobs), "updated": 0, "errors": 0, "queued": 0, "batches": 1}
    monkeypatch.setattr(collector.wordpress, "publish", fake_publish)
    monkeypatch.setattr(collector.wordpress, "process_retry_queue",
                        MagicMock(return_value={"attempted": 0, "succeeded": 0, "failed": 0, "dropped": 0}))
    monkeypatch.setattr(collector.notifier, "notify",
                        MagicMock(return_value={"qualifying": 0, "pushes_sent": 0, "email_sent": 0}))
    monkeypatch.setattr(collector, "ping_healthcheck", MagicMock())

    collector.run(dry_run=False)

    # Only one row in the DB (Deloitte dupe collapsed) and it's the one with the direct URL
    rows = conn.execute(
        "SELECT external_id, apply_url, location, seniority, seniority_confidence, "
        "date_posted, llm_classification FROM jobs"
    ).fetchall()
    assert len(rows) == 1
    ext_id, apply_url, loc, seniority, sen_conf, date_posted, llm_cls = rows[0]
    assert ext_id == "b"  # direct URL wins
    assert apply_url == "https://careers.deloitte.com/jobs/b"
    assert "New York, NY" in loc and "Chicago, IL" in loc
    assert seniority == "Senior Manager"  # inferred from $175K salary_min
    assert sen_conf == "inferred"
    assert date_posted == "2026-04-14"
    assert llm_cls == "RELEVANT"

    # WP payload side
    assert len(captured) == 1
    p = captured[0]
    assert p["apply_url"] == "https://careers.deloitte.com/jobs/b"
    assert p["seniority"] == "Senior Manager"
    assert p["seniority_confidence"] == "inferred"
    assert p["date_posted"] == "2026-04-14"
    assert p["llm_classification"] == "RELEVANT"


def test_pipeline_confidence_fields_persist_to_db_and_wp_payload(env_ok, conn, monkeypatch):
    """Walks a single job through: collect → filter → LLM → seniority → dedup → enrichment → upsert → WP.
    Verifies apply_url + confidence fields survive every hop."""
    source_job = _sample_job("j1", title="Senior Manager, People Analytics", company="Netflix")
    source_job["apply_url"] = "https://careers.netflix.com/job/1"
    # Aggregator had these, but not confirmed from source page yet:
    source_job["is_remote"] = "hybrid"
    monkeypatch.setattr(collector, "collect_sources", lambda conn=None: (
        [source_job],
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

    def fake_enrich_batch(jobs, **kw):
        for j in jobs:
            j["salary_confidence"] = "confirmed"
            j["remote_confidence"] = "confirmed"
            j["location_confidence"] = "aggregator_only"
            j["enrichment_source"] = "source_page"
            j["enrichment_date"] = "2026-04-17"
        return jobs
    monkeypatch.setattr(collector.enrichment, "enrich_batch", fake_enrich_batch)

    captured_payloads: list[dict] = []
    def fake_publish(jobs, **kw):
        # Replay the publisher's _payload construction so we see what would be posted to WP
        from src.publishers.wordpress import _payload
        captured_payloads.extend(_payload(j) for j in jobs)
        return {"created": len(jobs), "updated": 0, "errors": 0, "queued": 0, "batches": 1}

    monkeypatch.setattr(collector.wordpress, "publish", fake_publish)
    monkeypatch.setattr(collector.wordpress, "process_retry_queue",
                        MagicMock(return_value={"attempted": 0, "succeeded": 0, "failed": 0, "dropped": 0}))
    monkeypatch.setattr(collector.notifier, "notify",
                        MagicMock(return_value={"qualifying": 0, "pushes_sent": 0, "email_sent": 0}))
    monkeypatch.setattr(collector, "ping_healthcheck", MagicMock())

    collector.run(dry_run=False)

    # DB side
    row = conn.execute(
        "SELECT apply_url, seniority, salary_confidence, remote_confidence, "
        "location_confidence, enrichment_source FROM jobs WHERE external_id=?",
        ("j1",),
    ).fetchone()
    assert row[0] == "https://careers.netflix.com/job/1"
    assert row[1] == "Senior Manager"
    assert row[2] == "confirmed"
    assert row[3] == "confirmed"
    assert row[4] == "aggregator_only"
    assert row[5] == "source_page"

    # WP payload side
    assert len(captured_payloads) == 1
    p = captured_payloads[0]
    assert p["apply_url"] == "https://careers.netflix.com/job/1"
    assert p["seniority"] == "Senior Manager"
    assert p["salary_confidence"] == "confirmed"
    assert p["remote_confidence"] == "confirmed"
    assert p["location_confidence"] == "aggregator_only"
    assert p["enrichment_source"] == "source_page"
