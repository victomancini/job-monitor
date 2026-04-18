"""Tests for Phase 3 (R3) JobSpy-backed source. No real HTTP calls."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.sources import jobspy_source


def _row(title="People Analytics Manager", company="Netflix",
         url="https://example.com/job/1", site="linkedin",
         min_amount=150000, max_amount=200000, is_remote=True,
         date_posted="2026-04-14"):
    return {
        "title": title,
        "company": company,
        "job_url": url,
        "site": site,
        "location": "Los Gatos, CA",
        "description": "Lead our people analytics team.",
        "min_amount": min_amount,
        "max_amount": max_amount,
        "is_remote": is_remote,
        "date_posted": date_posted,
    }


def test_fetch_returns_empty_when_jobspy_missing():
    """If the python-jobspy library isn't available, fetch() must not raise."""
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=None):
        jobs, errors, meta = jobspy_source.fetch()
    assert jobs == []
    assert errors == []
    assert meta == {"available": False, "terms_run": 0}


def test_fetch_maps_scrape_jobs_result():
    fake_scrape = MagicMock(return_value=[_row()])
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, errors, meta = jobspy_source.fetch(search_terms=["people analytics"])
    assert len(jobs) == 1
    j = jobs[0]
    assert j["title"] == "People Analytics Manager"
    assert j["company"] == "Netflix"
    assert j["source_name"] == "jobspy_linkedin"
    assert j["external_id"].startswith("jobspy_linkedin_")
    assert j["apply_url"] == "https://example.com/job/1"
    assert j["salary_min"] == 150000
    assert j["salary_max"] == 200000
    assert j["is_remote"] == "remote"
    assert meta["available"] is True
    assert meta["terms_run"] == 1


def test_fetch_continues_past_per_term_exception():
    def fake_scrape(**kw):
        if kw["search_term"] == "break":
            raise RuntimeError("network flake")
        return [_row(url=f"https://x/{kw['search_term']}")]
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, errors, meta = jobspy_source.fetch(
            search_terms=["ok1", "break", "ok2"],
        )
    assert len(jobs) == 2  # two successful terms
    assert len(errors) == 1
    assert "network flake" in errors[0]
    assert meta["terms_run"] == 2


def test_fetch_skips_rows_without_required_fields():
    bad_row = {"title": "", "company": "X", "job_url": "https://x"}  # empty title
    fake_scrape = MagicMock(return_value=[bad_row, _row()])
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, _, _ = jobspy_source.fetch(search_terms=["x"])
    assert len(jobs) == 1


def test_fetch_dedups_via_hashed_external_id():
    """Same URL posted twice (different terms) → both appear in batch; the dedup
    happens downstream. But the external_id is deterministic, so duplicate URLs
    across terms will collide if the site is the same."""
    same_row = _row(url="https://same.example/1")
    fake_scrape = MagicMock(return_value=[same_row])
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, _, _ = jobspy_source.fetch(search_terms=["a", "b"])
    # Both terms return the same row → both get the same external_id; batch dedup is downstream.
    assert {j["external_id"] for j in jobs} == {jobs[0]["external_id"]}
