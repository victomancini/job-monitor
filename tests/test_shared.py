"""Tests for src/shared.py."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from src import shared


def test_format_salary_range_both():
    assert shared.format_salary_range(120_000, 180_000) == "$120K-$180K"


def test_format_salary_range_min_only():
    assert shared.format_salary_range(120_000, None) == "$120K+"


def test_format_salary_range_max_only():
    assert shared.format_salary_range(None, 180_000) == "Up to $180K"


def test_format_salary_range_none():
    assert shared.format_salary_range(None, None) is None


def test_format_salary_range_zero_is_none():
    assert shared.format_salary_range(0, 0) is None


def test_build_job_minimal():
    j = shared.build_job(
        source_name="jsearch",
        external_id="jsearch_1",
        title="  People Analytics Lead  ",
        company="  Netflix ",
        source_url="https://example.com/1",
    )
    assert j["external_id"] == "jsearch_1"
    assert j["title"] == "People Analytics Lead"
    assert j["company"] == "Netflix"
    assert j["salary_range"] is None
    assert j["is_remote"] == "unknown"
    assert j["description_is_snippet"] is False


def test_build_job_with_salary():
    j = shared.build_job(
        source_name="adzuna",
        external_id="adzuna_1",
        title="X",
        company="Y",
        source_url="https://example.com",
        salary_min=100_000,
        salary_max=150_000,
    )
    assert j["salary_range"] == "$100K-$150K"


def test_load_keywords_and_queries():
    kw = shared.load_keywords()
    assert "tier1_title" in kw
    assert "thresholds" in kw
    assert "active listening" in kw["negative_auto_reject"]["terms"]
    q = shared.load_queries()
    assert "jsearch" in q
    assert "jooble" in q


def test_load_companies():
    c = shared.load_companies()
    assert "tier1" in c
    # Check known EL vendors
    names = {entry["name"] for entry in c["tier1"]}
    assert "Perceptyx" in names
    assert "Netflix" in names


def test_validate_required_env_missing(monkeypatch):
    for v in shared.REQUIRED_ENV:
        monkeypatch.delenv(v, raising=False)
    missing = shared.validate_required_env()
    assert set(missing) == set(shared.REQUIRED_ENV)


def test_validate_required_env_present(monkeypatch):
    for v in shared.REQUIRED_ENV:
        monkeypatch.setenv(v, "x")
    assert shared.validate_required_env() == []


def test_env_trims_whitespace(monkeypatch):
    monkeypatch.setenv("FOO", "  value  ")
    assert shared.env("FOO") == "value"


# ───── R7-C: HTTPS scheme enforcement ─────────────────────────

def test_validate_env_scheme_accepts_https(monkeypatch):
    monkeypatch.setenv("WP_URL", "https://site.example/")
    monkeypatch.setenv("HEALTHCHECK_URL", "https://hc-ping.com/abc")
    assert shared.validate_env_scheme() == []


def test_validate_env_scheme_rejects_http_wp_url(monkeypatch):
    monkeypatch.setenv("WP_URL", "http://site.example/")
    monkeypatch.setenv("HEALTHCHECK_URL", "https://hc-ping.com/abc")
    assert shared.validate_env_scheme() == ["WP_URL"]


def test_validate_env_scheme_rejects_http_healthcheck(monkeypatch):
    monkeypatch.setenv("WP_URL", "https://site.example/")
    monkeypatch.setenv("HEALTHCHECK_URL", "http://my-hc.example/")
    assert shared.validate_env_scheme() == ["HEALTHCHECK_URL"]


def test_validate_env_scheme_reports_both_violations(monkeypatch):
    monkeypatch.setenv("WP_URL", "http://site/")
    monkeypatch.setenv("HEALTHCHECK_URL", "http://hc/")
    assert set(shared.validate_env_scheme()) == {"WP_URL", "HEALTHCHECK_URL"}


def test_validate_env_scheme_empty_urls_skip(monkeypatch):
    """Missing URLs are caught by validate_required_env; scheme check should
    be a no-op for empty values rather than false-flagging them."""
    monkeypatch.delenv("WP_URL", raising=False)
    monkeypatch.delenv("HEALTHCHECK_URL", raising=False)
    assert shared.validate_env_scheme() == []


# ───── R9-Part-2: is_aggregator_host subdomain match ────────────

def test_is_aggregator_host_matches_root():
    assert shared.is_aggregator_host("jooble.org")
    assert shared.is_aggregator_host("adzuna.com")
    assert shared.is_aggregator_host("indeed.com")


def test_is_aggregator_host_matches_regional_subdomains():
    """R9-Part-2: this is the fix. Prior exact-match AGGREGATOR_HOSTS missed
    us.jooble.org, uk.jooble.org, link.adzuna.com, de.indeed.com, etc. so
    jobs arriving on those variants bypassed redirect following entirely."""
    for h in [
        "us.jooble.org", "uk.jooble.org", "de.jooble.org",
        "www.adzuna.co.uk", "link.adzuna.com",
        "uk.indeed.com", "www.indeed.com",
        "jobs.google.com",
    ]:
        assert shared.is_aggregator_host(h), f"should match: {h}"


def test_is_aggregator_host_matches_adzuna_all_country_tlds():
    """R10: the 2026-04-20 log showed www.adzuna.ca and www.adzuna.com.au
    flowing through as aggregator=False. Adzuna operates in 16+ countries;
    every TLD must match."""
    for h in [
        "www.adzuna.ca", "adzuna.ca",
        "www.adzuna.com.au", "adzuna.com.au",
        "www.adzuna.de", "www.adzuna.fr",
        "www.adzuna.nl", "www.adzuna.it",
        "www.adzuna.in", "www.adzuna.sg",
        "www.adzuna.co.za", "www.adzuna.com.br",
    ]:
        assert shared.is_aggregator_host(h), f"should match adzuna TLD: {h}"


def test_is_aggregator_host_rejects_company_domains():
    for h in [
        "careers.netflix.com", "boards.greenhouse.io",
        "jobs.lever.co", "jobs.ashbyhq.com",
        "acme-engineering.com", "careers.acme.com",
    ]:
        assert not shared.is_aggregator_host(h), f"should not match: {h}"


def test_is_aggregator_host_case_insensitive_and_empty_safe():
    assert shared.is_aggregator_host("US.JOOBLE.ORG")
    assert not shared.is_aggregator_host("")
    assert not shared.is_aggregator_host(None)


def test_is_aggregator_host_doesnt_false_match_substring():
    """A company named 'adzunalike.com' shouldn't match 'adzuna.com'."""
    assert not shared.is_aggregator_host("adzunalike.com")
    assert not shared.is_aggregator_host("notjooble.org")


def test_validate_env_scheme_case_insensitive(monkeypatch):
    """HTTPS scheme match is case-insensitive per RFC 3986."""
    monkeypatch.setenv("WP_URL", "HTTPS://site.example/")
    monkeypatch.setenv("HEALTHCHECK_URL", "Https://hc.example/")
    assert shared.validate_env_scheme() == []


# ───── R7-2: raw_data size cap ────────────────────────────────

def test_build_job_stores_small_raw_data_verbatim():
    small = {"id": "abc", "title": "X", "company": "Y"}
    j = shared.build_job(
        source_name="jsearch",
        external_id="jsearch_abc",
        title="X",
        company="Y",
        source_url="https://example.com",
        raw_data=small,
    )
    import json as _json
    parsed = _json.loads(j["raw_data"])
    assert parsed == small
    assert "_truncated" not in parsed


def test_build_job_truncates_oversized_raw_data():
    """R7-2: >50KB payload gets replaced with a marker dict so row-size limits
    on Turso aren't blown silently."""
    huge_desc = "A" * (shared.RAW_DATA_MAX_BYTES + 10_000)
    raw = {"id": "abc", "title": "X", "company": "Y", "description_raw": huge_desc}
    j = shared.build_job(
        source_name="jsearch",
        external_id="jsearch_abc",
        title="X",
        company="Y",
        source_url="https://example.com",
        raw_data=raw,
    )
    import json as _json
    parsed = _json.loads(j["raw_data"])
    assert parsed["_truncated"] is True
    assert parsed["_original_bytes"] > shared.RAW_DATA_MAX_BYTES
    # Stored size well below the cap
    assert len(j["raw_data"].encode("utf-8")) < 1_000


def test_build_job_none_raw_data_stays_none():
    j = shared.build_job(
        source_name="jsearch",
        external_id="jsearch_abc",
        title="X",
        company="Y",
        source_url="https://example.com",
        raw_data=None,
    )
    assert j["raw_data"] is None


# ───── R11 Phase 0: days_since_posted helper ──────────────────

def _utc_minus(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")


def test_days_since_posted_prefers_date_posted():
    assert shared.days_since_posted(_utc_minus(5), _utc_minus(10)) == 5


def test_days_since_posted_falls_back_to_first_seen():
    assert shared.days_since_posted(None, _utc_minus(7)) == 7


def test_days_since_posted_none_when_both_missing():
    assert shared.days_since_posted(None, None) is None


def test_days_since_posted_returns_zero_for_today():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert shared.days_since_posted(today, None) == 0


def test_days_since_posted_accepts_iso_datetime_prefix():
    """Some sources emit full ISO timestamps; helper must tolerate the T suffix."""
    iso = _utc_minus(3) + "T12:34:56Z"
    assert shared.days_since_posted(iso, None) == 3


def test_days_since_posted_invalid_input_is_none():
    assert shared.days_since_posted("not-a-date", None) is None
    assert shared.days_since_posted("", "") is None


def test_days_since_posted_never_negative():
    """Future date (clock skew, bad source data) clamps to 0, not negative."""
    future = (datetime.now(timezone.utc) + timedelta(days=5)).strftime("%Y-%m-%d")
    assert shared.days_since_posted(future, None) == 0


# ───── R11 Phase 1: field provenance ────────────────────────

def test_source_reliability_known_source():
    assert shared.source_reliability("greenhouse") == 0.90
    assert shared.source_reliability("jsearch") == 0.55
    assert shared.source_reliability("jooble") == 0.50


def test_source_reliability_unknown_source_defaults_to_half():
    assert shared.source_reliability("some_new_source") == 0.50


def test_record_field_attaches_provenance():
    job = {"is_remote": "remote"}
    shared.record_field(job, "is_remote", source="jsearch")
    fs = job["_field_sources"]
    assert fs["is_remote"] == [{
        "source": "jsearch",
        "value": "remote",
        "confidence": 0.55,
    }]


def test_record_field_accumulates_multiple_observations():
    """Two sources observing the same field attach two entries — consensus
    voting (Phase 3) reads the history to adjudicate."""
    job = {"is_remote": "hybrid"}
    shared.record_field(job, "is_remote", source="jsearch")
    shared.record_field(job, "is_remote", source="greenhouse")
    observations = job["_field_sources"]["is_remote"]
    assert len(observations) == 2
    assert observations[0]["source"] == "jsearch"
    assert observations[1]["source"] == "greenhouse"
    assert observations[1]["confidence"] == 0.90


def test_record_field_skips_empty_and_unknown():
    """A source saying 'I don't know' must not count as a vote."""
    job = {"is_remote": "unknown", "location": "", "salary_min": None}
    shared.record_field(job, "is_remote", source="jsearch")
    shared.record_field(job, "location", source="jsearch")
    shared.record_field(job, "salary_min", source="jsearch")
    assert job.get("_field_sources") is None or job["_field_sources"] == {}


def test_record_field_explicit_confidence_override():
    job = {"is_remote": "remote"}
    shared.record_field(job, "is_remote", source="jsearch", confidence=0.99)
    assert job["_field_sources"]["is_remote"][0]["confidence"] == 0.99


def test_build_job_auto_records_provenance():
    """Every source that uses build_job gets provenance for free — no
    per-source retrofit needed when the field carries a real value."""
    j = shared.build_job(
        source_name="greenhouse",
        external_id="gh_1",
        title="People Analytics Manager",
        company="Netflix",
        source_url="https://example.com/1",
        location="Los Gatos, CA",
        location_country="US",
        is_remote="hybrid",
        salary_min=150000,
        salary_max=200000,
        date_posted="2026-04-15",
    )
    fs = j["_field_sources"]
    assert "is_remote" in fs and fs["is_remote"][0]["source"] == "greenhouse"
    assert "location" in fs
    assert "salary_min" in fs and fs["salary_min"][0]["value"] == 150000
    assert "date_posted" in fs
    # Unknown/empty fields skipped
    assert "work_arrangement" not in fs


def test_build_job_skips_provenance_on_unknown_remote():
    """Default is_remote='unknown' should NOT generate a provenance entry —
    it's a non-vote, not an observation."""
    j = shared.build_job(
        source_name="jooble",
        external_id="jooble_1",
        title="X",
        company="Y",
        source_url="https://example.com",
    )
    fs = j.get("_field_sources", {})
    assert "is_remote" not in fs
