"""R11 Phase 5: guardrails on enrichment HTTP — circuit breaker, fetch
budget cap, and priority ordering. Prevents one misbehaving host from
eating enrichment time and a big batch from dragging past Turso's
~15-minute stream idle timeout.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.processors import enrichment as E


@pytest.fixture(autouse=True)
def _reset_state():
    """Each test starts with fresh guardrail state."""
    E._reset_guardrails()
    yield
    E._reset_guardrails()


# ─── _CircuitBreaker ───────────────────────────────────────

def test_breaker_allows_by_default():
    cb = E._CircuitBreaker(threshold=3)
    assert cb.allow("jooble.org") is True


def test_breaker_trips_after_threshold_failures():
    cb = E._CircuitBreaker(threshold=3)
    for _ in range(3):
        cb.record_failure("jooble.org")
    assert cb.allow("jooble.org") is False


def test_breaker_success_resets_failure_counter():
    """Within the threshold window a success resets the counter so
    intermittent failures don't accumulate forever."""
    cb = E._CircuitBreaker(threshold=3)
    for _ in range(2):
        cb.record_failure("jooble.org")
    cb.record_success("jooble.org")
    for _ in range(2):
        cb.record_failure("jooble.org")
    # 2 failures after reset is still below threshold
    assert cb.allow("jooble.org") is True


def test_breaker_other_hosts_unaffected():
    cb = E._CircuitBreaker(threshold=3)
    for _ in range(5):
        cb.record_failure("jooble.org")
    assert cb.allow("greenhouse.io") is True


def test_breaker_empty_host_always_allows():
    cb = E._CircuitBreaker(threshold=1)
    cb.record_failure("")
    assert cb.allow("") is True


def test_breaker_snapshot_reports_tripped_hosts():
    cb = E._CircuitBreaker(threshold=2)
    for _ in range(2):
        cb.record_failure("jooble.org")
    snap = cb.snapshot()
    assert "jooble.org" in snap["tripped_hosts"]
    assert snap["total_failures"] >= 2


# ─── _FetchBudget ───────────────────────────────────────

def test_budget_allows_up_to_limit():
    b = E._FetchBudget(limit=3)
    assert b.take() is True
    assert b.take() is True
    assert b.take() is True
    assert b.take() is False


def test_budget_snapshot_reports_usage():
    b = E._FetchBudget(limit=5)
    b.take()
    b.take()
    assert b.snapshot() == {"used": 2, "limit": 5}


# ─── _priority_key ordering ───────────────────────────

def test_priority_sort_puts_relevant_first():
    jobs = [
        {"llm_classification": "NOT_RELEVANT", "keyword_score": 80, "source_name": "jsearch"},
        {"llm_classification": "RELEVANT", "keyword_score": 30, "source_name": "jsearch"},
        {"llm_classification": "PARTIALLY_RELEVANT", "keyword_score": 50, "source_name": "jsearch"},
    ]
    jobs.sort(key=E._priority_key)
    assert jobs[0]["llm_classification"] == "RELEVANT"
    assert jobs[1]["llm_classification"] == "PARTIALLY_RELEVANT"


def test_priority_sort_prefers_aggregator_jobs_within_same_tier():
    """Aggregator-sourced jobs benefit most from canonical fetch; canonical
    ATS sources already have the truth — so aggregators go FIRST when LLM
    tier and keyword score are equal."""
    jobs = [
        {"llm_classification": "RELEVANT", "keyword_score": 60, "source_name": "greenhouse"},
        {"llm_classification": "RELEVANT", "keyword_score": 60, "source_name": "jooble"},
    ]
    jobs.sort(key=E._priority_key)
    # Aggregator first (more benefit), canonical ATS second
    assert jobs[0]["source_name"] == "jooble"


def test_priority_sort_keyword_score_tiebreak():
    jobs = [
        {"llm_classification": "RELEVANT", "keyword_score": 15, "source_name": "jsearch"},
        {"llm_classification": "RELEVANT", "keyword_score": 75, "source_name": "jsearch"},
    ]
    jobs.sort(key=E._priority_key)
    assert jobs[0]["keyword_score"] == 75


# ─── enrich_job integration ────────────────────────────

def test_enrich_job_skipped_when_host_circuit_broken():
    """When a host is tripped, enrich_job must short-circuit without
    issuing HTTP — keeps aggregator_only semantics intact."""
    E._circuit_breaker._tripped.add("jooble.org")
    job = {
        "external_id": "t1",
        "apply_url": "https://jooble.org/jdp/1",
        "source_name": "jooble",
        "description": "x",
    }
    with patch("src.processors.enrichment.requests.get") as m:
        E.enrich_job(job)
    m.assert_not_called()
    assert job["enrichment_source"] == "aggregator"


def test_enrich_job_skipped_when_budget_exhausted():
    """Budget exhaustion routes to the aggregator fallback. Tests the
    scenario where a large batch of lower-priority jobs runs out of HTTP
    budget; they still return with sane defaults."""
    # Exhaust the global budget
    while E._fetch_budget.take():
        pass
    assert E._fetch_budget.take() is False
    job = {
        "external_id": "t2",
        "apply_url": "https://example.com/jobs/1",
        "source_name": "greenhouse",
        "description": "x",
    }
    with patch("src.processors.enrichment.requests.get") as m:
        E.enrich_job(job)
    m.assert_not_called()
    assert job["enrichment_source"] == "aggregator"


def test_enrich_job_records_failure_on_http_exception():
    job = {
        "external_id": "t3",
        "apply_url": "https://flaky.example.com/job/1",
        "source_name": "jsearch",
        "description": "x",
    }
    import requests as _requests
    with patch("src.processors.enrichment.requests.get",
               side_effect=_requests.RequestException("conn refused")):
        E.enrich_job(job)
    # Failure recorded against the host
    assert E._circuit_breaker._failures["flaky.example.com"] == 1
    assert job["enrichment_source"] == "aggregator"


def test_enrich_batch_resets_guardrails_each_call():
    """Each batch starts with fresh state — a previous run's tripped hosts
    don't leak into the next run."""
    E._circuit_breaker._tripped.add("jooble.org")
    E.enrich_batch([])  # empty batch: no-op but should still reset... actually no.
    # Empty batch short-circuits BEFORE reset. Test with a minimal job.
    E._circuit_breaker._tripped.add("jooble.org")
    jobs = [{"external_id": "x", "apply_url": "", "source_name": "jsearch",
             "description": ""}]
    E.enrich_batch(jobs)
    # After enrich_batch, circuit breaker should have been reset at entry
    assert "jooble.org" not in E._circuit_breaker._tripped
