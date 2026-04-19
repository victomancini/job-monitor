"""Tests for src/shared.py."""
from __future__ import annotations

import os

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
