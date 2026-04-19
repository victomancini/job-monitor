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
    # Per-site loop: return the row only when the mocked site is LinkedIn,
    # empty otherwise. Keeps the assertion count small and decoupled from SITES.
    def fake_scrape(**kw):
        if kw["site_name"] == ["linkedin"]:
            return [_row(site="linkedin")]
        return []
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
    assert meta["per_site_counts"] == {"linkedin": 1}


def test_fetch_continues_past_per_term_exception():
    """Per-site iso: a single term can have some sites succeed + some raise.
    Terms where at least one site produced a row count toward terms_run."""
    def fake_scrape(**kw):
        if kw["search_term"] == "break":
            raise RuntimeError("network flake")
        # One row per site; enough for this test to see non-zero output per term
        return [_row(url=f"https://x/{kw['search_term']}/{kw['site_name'][0]}",
                     site=kw["site_name"][0])]
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, errors, meta = jobspy_source.fetch(
            search_terms=["ok1", "break", "ok2"],
        )
    # 2 successful terms × N sites
    assert len(jobs) == 2 * len(jobspy_source.SITES)
    # 1 error per site for the broken term
    assert len(errors) == len(jobspy_source.SITES)
    assert all("network flake" in e for e in errors)
    assert meta["terms_run"] == 2


def test_fetch_isolates_single_site_failure():
    """N11: one site raising must not drop the other three for the same term."""
    def fake_scrape(**kw):
        if kw["site_name"] == ["glassdoor"]:
            raise RuntimeError("glassdoor TLS bounce")
        return [_row(site=kw["site_name"][0],
                     url=f"https://x/{kw['site_name'][0]}")]
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, errors, meta = jobspy_source.fetch(search_terms=["x"])
    # 3 successful sites × 1 term
    assert len(jobs) == len(jobspy_source.SITES) - 1
    assert len(errors) == 1
    assert "glassdoor" in errors[0]
    assert "glassdoor" not in meta["per_site_counts"]


def test_fetch_skips_rows_without_required_fields():
    bad_row = {"title": "", "company": "X", "job_url": "https://x"}  # empty title
    # Emit [bad_row, valid] on every site so the total good count matches SITES.
    fake_scrape = MagicMock(return_value=[bad_row, _row()])
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, _, _ = jobspy_source.fetch(search_terms=["x"])
    # 1 valid row × N sites (bad rows filtered in _row_to_job)
    assert len(jobs) == len(jobspy_source.SITES)


def test_fetch_dedups_via_hashed_external_id():
    """Same URL on the same site across two terms → stable external_id. The
    per-site loop now emits rows tagged with their site, so the dedup guarantee
    is 'same site + same URL = same external_id'."""
    def fake_scrape(**kw):
        # Only LinkedIn returns the duplicate row; other sites are empty.
        if kw["site_name"] == ["linkedin"]:
            return [_row(url="https://same.example/1", site="linkedin")]
        return []
    with patch("src.sources.jobspy_source._scrape_jobs_callable", return_value=fake_scrape):
        jobs, _, _ = jobspy_source.fetch(search_terms=["a", "b"])
    # Two terms × one site yielding the same URL → two rows, one external_id.
    assert len(jobs) == 2
    assert {j["external_id"] for j in jobs} == {jobs[0]["external_id"]}
