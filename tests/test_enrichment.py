"""Tests for src/processors/enrichment.py — no real HTTP calls."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.processors import enrichment as en


def _mock_resp(body: str, status: int = 200):
    m = MagicMock()
    m.status_code = status
    m.text = body
    return m


def _job(**overrides):
    base = {
        "external_id": "test_1",
        "title": "People Analytics Manager",
        "company": "Netflix",
        "source_url": "https://careers.netflix.com/job/1",
        "apply_url": "https://careers.netflix.com/job/1",
        "location": "",
        "is_remote": "unknown",
    }
    base.update(overrides)
    return base


# ──────────────────────── Salary extraction ──────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("$120,000 - $180,000 per year", (120000.0, 180000.0)),
    ("Salary: $95,000 to $130,000", (95000.0, 130000.0)),
    ("$120K - $180K", (120000.0, 180000.0)),
    ("$120k-$180k", (120000.0, 180000.0)),
    ("pay range: 120000 to 180000", (120000.0, 180000.0)),
    ("$120,000 – $180,000", (120000.0, 180000.0)),  # en-dash
    ("$180,000 - $120,000", (120000.0, 180000.0)),  # reversed → still sorted
])
def test_extract_salary_formats(text, expected):
    r = en._extract_salary(text)
    assert r is not None
    assert (r["min"], r["max"]) == expected


def test_extract_salary_ignores_small_numbers():
    assert en._extract_salary("employees aged 18 - 65 welcome") is None


def test_extract_salary_returns_none_without_match():
    assert en._extract_salary("no compensation info here") is None


def test_extract_salary_range_string_formatted():
    r = en._extract_salary("$120,000 - $180,000")
    assert r["range_str"] == "$120K-$180K"


# ──────────────────────── Remote detection ───────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("This is a fully remote position", "remote"),
    ("100% remote work allowed", "remote"),
    ("Work from home — no office required", "remote"),
    ("This role is remote eligible", "remote"),
    ("Hybrid - 3 days in office per week", "hybrid"),
    ("We offer a hybrid schedule", "hybrid"),
    ("In-office 2 days weekly", "hybrid"),
    ("On-site position in New York", "onsite"),
    ("Must work in-office daily", "onsite"),
    ("In-person role, no remote", "onsite"),
])
def test_extract_remote_status(text, expected):
    assert en._extract_remote_status(text) == expected


def test_extract_remote_status_hybrid_outranks_remote():
    """Spec: if both 'remote' and 'hybrid' appear, prefer 'hybrid'."""
    text = "Remote eligible; hybrid schedule with 2 days in the office"
    assert en._extract_remote_status(text) == "hybrid"


def test_extract_remote_status_hybrid_outranks_onsite():
    text = "On-site expectation; hybrid arrangement possible"
    assert en._extract_remote_status(text) == "hybrid"


def test_extract_remote_none_when_silent():
    assert en._extract_remote_status("Generic job description with no remote info.") is None


# ──────────────────────── Location extraction ────────────────────────

def test_extract_location_prefixed():
    assert en._extract_location("Location: Austin, TX") == "Austin, TX"


def test_extract_location_city_state():
    text = "We are headquartered in San Francisco, CA and hiring."
    assert en._extract_location(text) == "San Francisco, CA"


def test_extract_location_multiple_locations():
    text = "This role is available in Multiple Locations across the US."
    assert en._extract_location(text) == "Multiple Locations"


def test_extract_location_none_when_missing():
    assert en._extract_location("No location given anywhere.") is None


# ──────────────────────── HTML → text ────────────────────────────────

def test_extract_text_strips_tags_and_scripts():
    html = """
    <html><head><script>alert('x')</script><style>body{}</style></head>
    <body><h1>Job Title</h1><p>$120,000 - $180,000</p></body></html>
    """
    text = en._extract_text(html)
    assert "alert" not in text
    assert "body{}" not in text
    assert "Job Title" in text
    assert "$120,000 - $180,000" in text


# ──────────────────────── enrich_job end-to-end ──────────────────────

def test_enrich_job_full_happy_path():
    page = """
    <html><body>
    <h1>Employee Listening Manager</h1>
    <p>Location: Austin, TX</p>
    <p>Compensation: $120,000 to $180,000</p>
    <p>This role is fully remote.</p>
    </body></html>
    """
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["salary_min"] == 120000.0
    assert j["salary_max"] == 180000.0
    assert j["salary_range"] == "$120K-$180K"
    assert j["salary_confidence"] == "confirmed"
    assert j["is_remote"] == "remote"
    assert j["remote_confidence"] == "confirmed"
    assert j["location"] == "Austin, TX"
    assert j["location_confidence"] == "confirmed"
    assert j["enrichment_source"] == "source_page"
    assert j["enrichment_date"] == datetime.now(timezone.utc).strftime("%Y-%m-%d")


def test_enrich_job_preserves_existing_salary():
    """If aggregator already has a salary, enrichment should mark it 'aggregator_only'
    rather than overwrite."""
    page = "<p>Location: Austin, TX</p><p>$50,000 - $60,000</p>"
    j = _job(salary_min=150000.0, salary_max=200000.0, salary_range="$150K-$200K")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["salary_min"] == 150000.0  # preserved
    assert j["salary_confidence"] == "aggregator_only"


def test_enrich_job_preserves_existing_is_remote_when_page_silent():
    page = "<p>Just a description with no remote hints.</p>"
    j = _job(is_remote="hybrid")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["is_remote"] == "hybrid"
    assert j["remote_confidence"] == "aggregator_only"


def test_enrich_job_updates_is_remote_when_aggregator_unknown():
    page = "<p>This is a fully remote position.</p>"
    j = _job(is_remote="unknown")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["is_remote"] == "remote"
    assert j["remote_confidence"] == "confirmed"


def test_enrich_job_timeout_falls_back_to_aggregator():
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               side_effect=requests.Timeout("too slow")):
        en.enrich_job(j)
    assert j["enrichment_source"] == "aggregator"
    assert "enrichment_date" not in j


def test_enrich_job_connection_error_falls_back_to_aggregator():
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               side_effect=requests.ConnectionError("boom")):
        en.enrich_job(j)
    assert j["enrichment_source"] == "aggregator"


def test_enrich_job_404_falls_back_to_aggregator():
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp("Not found", 404)):
        en.enrich_job(j)
    assert j["enrichment_source"] == "aggregator"


def test_enrich_job_empty_url_falls_back():
    j = _job(source_url="", apply_url="")
    en.enrich_job(j)  # no mock needed — should not fetch
    assert j["enrichment_source"] == "aggregator"


def test_enrich_job_skips_recently_enriched():
    """Already-enriched jobs (within 7 days) should not re-fetch."""
    recent = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    j = _job(enrichment_date=recent, enrichment_source="source_page")
    with patch("src.processors.enrichment.requests.get") as m:
        en.enrich_job(j)
    assert m.call_count == 0


def test_enrich_job_re_enriches_stale():
    """Enrichment older than 7 days → re-fetch."""
    stale = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    j = _job(enrichment_date=stale)
    page = "<p>Fully remote.</p>"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)) as m:
        en.enrich_job(j)
    assert m.call_count == 1
    assert j["is_remote"] == "remote"


def test_enrich_batch_processes_each_and_sleeps_between():
    jobs = [_job(external_id=f"t{i}") for i in range(3)]
    page = "<p>Fully remote.</p>"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)) as m, \
         patch("src.processors.enrichment.time.sleep") as ms:
        en.enrich_batch(jobs, delay=1.0)
    assert m.call_count == 3
    # 3 jobs → 2 sleeps (no trailing sleep)
    assert ms.call_count == 2


def test_enrich_batch_continues_past_individual_failures():
    jobs = [_job(external_id=f"t{i}") for i in range(3)]
    responses = [
        _mock_resp("<p>$120K-$180K</p>", 200),
        requests.Timeout("boom"),  # will be raised via side_effect
        _mock_resp("<p>Hybrid schedule</p>", 200),
    ]
    with patch("src.processors.enrichment.requests.get",
               side_effect=responses), \
         patch("src.processors.enrichment.time.sleep"):
        en.enrich_batch(jobs)
    assert jobs[0]["enrichment_source"] == "source_page"
    assert jobs[1]["enrichment_source"] == "aggregator"
    assert jobs[2]["enrichment_source"] == "source_page"
