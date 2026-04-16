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
