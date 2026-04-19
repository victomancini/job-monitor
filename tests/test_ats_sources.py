"""Tests for Phase 2 (R3) direct-ATS source adapters (Greenhouse, Lever, Ashby)."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src import db as dbmod
from src.sources import ashby, greenhouse, lever


def _mock_resp(body=None, status=200):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = body if body is not None else {}
    m.text = str(body)
    return m


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    dbmod.migrate(c)
    yield c
    c.close()


# ──────────────────────────── Greenhouse ───────────────────────────

def test_greenhouse_maps_basic_posting():
    body = {"jobs": [{
        "id": 42,
        "title": "Senior People Analytics Manager",
        "location": {"name": "Remote - US"},
        "content": "<p>Lead our <b>people analytics</b> team.</p>",
        "departments": [{"name": "People"}],
        "updated_at": "2026-04-14T10:00:00Z",
        "absolute_url": "https://boards.greenhouse.io/cultureamp/jobs/42",
    }]}
    with patch("src.sources.greenhouse.retry_request", return_value=_mock_resp(body)), \
         patch("src.sources.greenhouse.time.sleep"):
        jobs, errors, meta = greenhouse.fetch(
            companies={"cultureamp": "Culture Amp"},
        )
    assert len(jobs) == 1
    j = jobs[0]
    assert j["external_id"] == "gh_cultureamp_42"
    assert j["title"] == "Senior People Analytics Manager"
    assert j["company"] == "Culture Amp"
    assert j["source_name"] == "greenhouse"
    assert j["apply_url"] == "https://boards.greenhouse.io/cultureamp/jobs/42"
    # HTML stripped from description
    assert "<p>" not in j["description"]
    assert "people analytics" in j["description"]
    assert meta["checked"] == 1


def test_greenhouse_404_marks_cache_not_found(conn):
    with patch("src.sources.greenhouse.retry_request", return_value=_mock_resp({}, status=404)):
        jobs, errors, _ = greenhouse.fetch(conn=conn, companies={"ghost": "Ghost Co"})
    assert jobs == []
    assert errors == []  # 404 is not an error — company just doesn't use Greenhouse
    info = dbmod.get_ats_status(conn, "greenhouse", "ghost")
    assert info["status"] == "not_found"


def test_greenhouse_cache_skips_recent_404(conn):
    dbmod.set_ats_status(conn, "greenhouse", "ghost", "not_found")
    # No patch — if skip works, no HTTP call is made
    jobs, errors, meta = greenhouse.fetch(conn=conn, companies={"ghost": "Ghost Co"})
    assert jobs == []
    assert meta["skipped_cached"] == 1
    assert meta["checked"] == 0


def test_greenhouse_continues_past_individual_errors():
    body_ok = {"jobs": [{"id": 1, "title": "T", "absolute_url": "https://x.com/1"}]}
    responses = [_mock_resp({}, 500), _mock_resp(body_ok)]
    with patch("src.sources.greenhouse.retry_request", side_effect=responses), \
         patch("src.sources.greenhouse.time.sleep"):
        jobs, errors, _ = greenhouse.fetch(
            companies={"bad": "Bad Co", "good": "Good Co"},
        )
    assert len(jobs) == 1
    assert len(errors) == 1


def test_greenhouse_marks_active_in_cache(conn):
    body = {"jobs": [{"id": 1, "title": "T", "absolute_url": "https://x/1"}]}
    with patch("src.sources.greenhouse.retry_request", return_value=_mock_resp(body)):
        greenhouse.fetch(conn=conn, companies={"slug1": "Co"})
    info = dbmod.get_ats_status(conn, "greenhouse", "slug1")
    assert info["status"] == "active"
    assert info["jobs_found"] == 1


# ──────────────────────────── Lever ────────────────────────────────

def test_lever_maps_basic_posting():
    body = [{
        "id": "abc-123",
        "text": "Staff People Scientist",
        "categories": {"location": "Remote", "team": "People"},
        "descriptionPlain": "Research role",
        "hostedUrl": "https://jobs.lever.co/figma/abc-123",
        "createdAt": 1745000000000,  # ms timestamp
    }]
    with patch("src.sources.lever.retry_request", return_value=_mock_resp(body)):
        jobs, errors, _ = lever.fetch(companies={"figma": "Figma"})
    assert len(jobs) == 1
    j = jobs[0]
    assert j["external_id"] == "lever_figma_abc-123"
    assert j["title"] == "Staff People Scientist"
    assert j["apply_url"] == "https://jobs.lever.co/figma/abc-123"
    assert j["source_name"] == "lever"
    assert j["date_posted"]  # ms converted to ISO date


def test_lever_parses_structured_salary_range():
    """Regression for H3: Lever salary was previously computed then dropped."""
    body = [{
        "id": "sal-1",
        "text": "People Analytics Manager",
        "hostedUrl": "https://jobs.lever.co/x/sal-1",
        "salaryRange": {"min": 150000, "max": 210000, "currency": "USD"},
    }]
    with patch("src.sources.lever.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = lever.fetch(companies={"x": "X"})
    j = jobs[0]
    assert j["salary_min"] == 150000
    assert j["salary_max"] == 210000
    assert j["salary_range"] == "$150K-$210K"


def test_lever_parses_text_salary_when_structured_absent():
    body = [{
        "id": "sal-2",
        "text": "Senior Analyst",
        "hostedUrl": "https://jobs.lever.co/x/sal-2",
        "salaryRange": {"text": "$120k – $160k"},
    }]
    with patch("src.sources.lever.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = lever.fetch(companies={"x": "X"})
    j = jobs[0]
    assert j["salary_min"] == 120000
    assert j["salary_max"] == 160000


def test_lever_missing_salary_leaves_fields_null():
    body = [{"id": "no-sal", "text": "T", "hostedUrl": "https://x/ns"}]
    with patch("src.sources.lever.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = lever.fetch(companies={"x": "X"})
    assert jobs[0]["salary_min"] is None
    assert jobs[0]["salary_max"] is None


def test_lever_404_cached(conn):
    with patch("src.sources.lever.retry_request", return_value=_mock_resp({}, status=404)):
        jobs, _, _ = lever.fetch(conn=conn, companies={"nope": "Nope"})
    assert jobs == []
    assert dbmod.get_ats_status(conn, "lever", "nope")["status"] == "not_found"


# ──────────────────────────── Ashby ────────────────────────────────

def test_ashby_maps_structured_compensation():
    body = {"jobs": [{
        "id": "ash-1",
        "title": "Head of People Analytics",
        "location": "San Francisco",
        "department": "People",
        "compensation": {
            "compensationTierSummary": {
                "minValue": 180000, "maxValue": 240000,
                "currency": "USD", "period": "YEAR",
            },
        },
        "descriptionHtml": "<p>Lead PA.</p>",
        "jobUrl": "https://jobs.ashbyhq.com/notion/ash-1",
        "publishedAt": "2026-04-10",
    }]}
    with patch("src.sources.ashby.retry_request", return_value=_mock_resp(body)):
        jobs, errors, _ = ashby.fetch(companies={"notion": "Notion"})
    assert len(jobs) == 1
    j = jobs[0]
    assert j["external_id"] == "ashby_notion_ash-1"
    assert j["company"] == "Notion"
    assert j["salary_min"] == 180000
    assert j["salary_max"] == 240000
    assert j["salary_range"] == "$180K-$240K"
    assert j["apply_url"] == "https://jobs.ashbyhq.com/notion/ash-1"


def test_ashby_missing_compensation_leaves_salary_null():
    body = {"jobs": [{
        "id": "ash-2", "title": "Role", "jobUrl": "https://x/2",
    }]}
    with patch("src.sources.ashby.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = ashby.fetch(companies={"slug": "Co"})
    assert jobs[0]["salary_min"] is None
    assert jobs[0]["salary_max"] is None


# ──────────────────────────── Cache helpers (db.py) ────────────────

def test_should_skip_ats_slug_only_skips_not_found(conn):
    dbmod.set_ats_status(conn, "greenhouse", "slug1", "not_found")
    dbmod.set_ats_status(conn, "greenhouse", "slug2", "empty")
    dbmod.set_ats_status(conn, "greenhouse", "slug3", "active")
    dbmod.set_ats_status(conn, "greenhouse", "slug4", "error")
    assert dbmod.should_skip_ats_slug(conn, "greenhouse", "slug1") is True
    assert dbmod.should_skip_ats_slug(conn, "greenhouse", "slug2") is False
    assert dbmod.should_skip_ats_slug(conn, "greenhouse", "slug3") is False
    assert dbmod.should_skip_ats_slug(conn, "greenhouse", "slug4") is False
    assert dbmod.should_skip_ats_slug(conn, "greenhouse", "unseen") is False


def test_set_ats_status_upserts(conn):
    dbmod.set_ats_status(conn, "lever", "slug", "not_found")
    dbmod.set_ats_status(conn, "lever", "slug", "active", jobs_found=5)
    info = dbmod.get_ats_status(conn, "lever", "slug")
    assert info["status"] == "active"
    assert info["jobs_found"] == 5
