"""Tests for all 5 data sources using mocked HTTP responses. No real API calls."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.sources import adzuna, google_alerts, jooble, jsearch, usajobs


def _mock_resp(json_body, status=200, headers=None):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = json_body
    m.headers = headers or {}
    return m


# ──────────────────────────── JSearch ────────────────────────────

def test_jsearch_maps_full_response():
    body = {
        "data": [{
            "job_id": "abc123",
            "job_title": "People Analytics Manager",
            "employer_name": "Netflix",
            "job_city": "Los Gatos",
            "job_state": "CA",
            "job_country": "US",
            "job_description": "Lead people analytics team...",
            "job_min_salary": 150000,
            "job_max_salary": 200000,
            "job_apply_link": "https://netflix.com/jobs/1",
            "job_is_remote": False,
            "job_employment_type": "FULLTIME",
            "job_posted_at_datetime_utc": "2026-04-14T10:00:00Z",
        }]
    }
    with patch("src.sources.jsearch.retry_request", return_value=_mock_resp(body, headers={"X-RapidAPI-Requests-Remaining": "180"})):
        jobs, errors, meta = jsearch.fetch("fake_key", queries=[{"query": "people analytics", "date_posted": "3days", "num_pages": 1}])
    assert len(jobs) == 1
    assert errors == []
    j = jobs[0]
    assert j["external_id"] == "jsearch_abc123"
    assert j["title"] == "People Analytics Manager"
    assert j["company"] == "Netflix"
    assert j["location"] == "Los Gatos, CA"
    assert j["location_country"] == "US"
    assert j["salary_range"] == "$150K-$200K"
    assert j["source_name"] == "jsearch"
    assert meta["quota_remaining"] == 180


def test_jsearch_missing_key_returns_error():
    jobs, errors, meta = jsearch.fetch("")
    assert jobs == []
    assert any("JSEARCH_API_KEY" in e for e in errors)


def test_jsearch_partial_failure_continues():
    good = {"data": [{"job_id": "1", "job_title": "PA Lead", "employer_name": "X", "job_apply_link": "http://x.com"}]}
    responses = [_mock_resp({}, status=500), _mock_resp(good)]
    with patch("src.sources.jsearch.retry_request", side_effect=responses), \
         patch("src.sources.jsearch.time.sleep"):
        jobs, errors, meta = jsearch.fetch("k", queries=[{"query": "q1"}, {"query": "q2"}])
    assert len(jobs) == 1
    assert len(errors) == 1


def test_jsearch_skips_malformed_item():
    body = {"data": [{"job_id": "good", "job_title": "T", "employer_name": "C", "job_apply_link": "http://x"}, {"job_title": "no id"}]}
    with patch("src.sources.jsearch.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = jsearch.fetch("k", queries=[{"query": "q"}])
    assert len(jobs) == 1


# ──────────────────────────── Jooble ─────────────────────────────

def test_jooble_maps_snippet():
    body = {"jobs": [{
        "id": 999, "title": "Employee Listening Analyst",
        "company": "Acme Corp", "location": "Boston, MA",
        "snippet": "Short snippet text here",
        "salary": "$120,000 - $160,000 per year",
        "link": "https://jooble.org/desc/999", "updated": "2026-04-14",
    }]}
    with patch("src.sources.jooble.retry_request", return_value=_mock_resp(body)), \
         patch("src.sources.jooble.time.sleep"):
        jobs, errors, _ = jooble.fetch("k", queries=[{"keywords": "employee listening", "location": "United States"}])
    assert len(jobs) == 1
    j = jobs[0]
    assert j["external_id"] == "jooble_999"
    assert j["description_is_snippet"] is True
    assert j["location_country"] == "US"
    assert j["salary_range"] == "$120K-$160K"


def test_jooble_no_key():
    jobs, errors, _ = jooble.fetch("")
    assert jobs == []
    assert errors


def test_jooble_handles_empty_response():
    with patch("src.sources.jooble.retry_request", return_value=_mock_resp({"jobs": []})):
        jobs, errors, _ = jooble.fetch("k", queries=[{"keywords": "x", "location": "United States"}])
    assert jobs == []
    assert errors == []


# ──────────────────────────── Adzuna ─────────────────────────────

def test_adzuna_maps_response():
    body = {"results": [{
        "id": "ad-1",
        "title": "Workforce Analytics Director",
        "company": {"display_name": "ExampleCo"},
        "location": {"display_name": "New York, NY"},
        "description": "Lead PA function...",
        "salary_min": 180000, "salary_max": 240000,
        "redirect_url": "https://adzuna.com/ad-1",
        "created": "2026-04-10T12:00:00Z",
    }]}
    with patch("src.sources.adzuna.retry_request", return_value=_mock_resp(body)), \
         patch("src.sources.adzuna.time.sleep"):
        jobs, errors, _ = adzuna.fetch("app_id", "app_key",
                                       queries=[{"what": "people analytics"}],
                                       countries=["us"])
    assert len(jobs) == 1
    j = jobs[0]
    assert j["external_id"] == "adzuna_ad-1"
    assert j["company"] == "ExampleCo"
    assert j["location_country"] == "US"
    assert j["salary_range"] == "$180K-$240K"


def test_adzuna_missing_creds():
    jobs, errors, _ = adzuna.fetch("", "")
    assert jobs == []
    assert errors


def test_adzuna_multi_country_bonus():
    body = {"results": [{"id": "1", "title": "T", "company": {"display_name": "C"}, "redirect_url": "u"}]}
    with patch("src.sources.adzuna.retry_request", return_value=_mock_resp(body)), \
         patch("src.sources.adzuna.time.sleep"):
        jobs, errors, _ = adzuna.fetch("id", "k",
                                       queries=[{"what": "q"}],
                                       countries=["us", "gb"])
    # One job × 2 countries — different map per country
    assert len(jobs) == 2
    assert {j["location_country"] for j in jobs} == {"US", "GB"}


# ──────────────────────────── USAJobs ────────────────────────────

def test_usajobs_maps_response():
    body = {"SearchResult": {"SearchResultItems": [{
        "MatchedObjectId": "98765",
        "MatchedObjectDescriptor": {
            "PositionTitle": "Industrial Organizational Psychologist",
            "OrganizationName": "OPM",
            "PositionLocation": [{"LocationName": "Washington, DC"}],
            "QualificationSummary": "Experience with federal surveys...",
            "PositionRemuneration": [{"MinimumRange": "95000", "MaximumRange": "140000"}],
            "PositionURI": "https://usajobs.gov/GetJob/98765",
            "PublicationStartDate": "2026-04-01",
            "TeleworkEligible": True,
        },
    }]}}
    with patch("src.sources.usajobs.retry_request", return_value=_mock_resp(body)), \
         patch("src.sources.usajobs.time.sleep"):
        jobs, errors, _ = usajobs.fetch("me@x.com", "key", keywords=["people analytics"])
    assert len(jobs) == 1
    j = jobs[0]
    assert j["external_id"] == "usajobs_98765"
    assert j["location_country"] == "US"
    assert j["salary_range"] == "$95K-$140K"
    assert j["is_remote"] == "hybrid"
    assert j["source_name"] == "usajobs"


def test_usajobs_missing_creds():
    jobs, errors, _ = usajobs.fetch("", "")
    assert jobs == []
    assert errors


# ────────────────────── Google Alerts / RSS ──────────────────────

def test_google_alerts_parses_feed():
    fake_entry = MagicMock()
    fake_entry.get = lambda k, default=None: {
        "link": "https://jobs.example.com/1",
        "title": "Employee Listening Manager - Netflix",
        "summary": "Join Netflix...",
        "published": "Mon, 14 Apr 2026 10:00:00 GMT",
    }.get(k, default)
    fake_entry.source = MagicMock(title="Netflix Jobs")
    fake_entry.published_parsed = (2026, 4, 14, 10, 0, 0, 0, 0, 0)

    fake_feed = MagicMock()
    fake_feed.entries = [fake_entry]
    fake_feed.bozo = False
    with patch("src.sources.google_alerts.feedparser.parse", return_value=fake_feed), \
         patch("src.sources.google_alerts.datetime") as mock_dt:
        from datetime import datetime as real_dt, timezone as real_tz
        # Make stale check see recent entry
        mock_dt.now.return_value = real_dt(2026, 4, 16, tzinfo=real_tz.utc)
        mock_dt.side_effect = real_dt
        jobs, errors, meta = google_alerts.fetch(feed_urls=["https://fake/rss"])
    assert len(jobs) == 1
    assert jobs[0]["external_id"].startswith("galert_")
    assert jobs[0]["description_is_snippet"] is True
    assert meta["stale_feeds"] == []


def test_google_alerts_filters_blog_posts():
    """Heuristic: SKIP entries whose title includes 'blog', 'article', 'guide', etc."""
    titles = ["Blog: Why employee listening matters", "How to build a pulse survey", "Employee Listening Manager at Netflix"]
    entries = []
    for t in titles:
        e = MagicMock()
        e.get = lambda k, default=None, _t=t: {"link": f"https://x/{_t}", "title": _t, "summary": "", "published": None}.get(k, default)
        e.source = None
        e.published_parsed = None
        entries.append(e)
    fake_feed = MagicMock()
    fake_feed.entries = entries
    fake_feed.bozo = False
    with patch("src.sources.google_alerts.feedparser.parse", return_value=fake_feed):
        jobs, errors, meta = google_alerts.fetch(feed_urls=["https://x"])
    titles_kept = [j["title"] for j in jobs]
    assert titles_kept == ["Employee Listening Manager at Netflix"]


def test_google_alerts_no_feeds_configured():
    jobs, errors, meta = google_alerts.fetch(feed_urls=[])
    assert jobs == []
    assert errors == []


def test_google_alerts_deduplicates_within_batch():
    """Same link across multiple feeds → one job."""
    e = MagicMock()
    e.get = lambda k, default=None: {"link": "https://same", "title": "Same Job", "summary": "", "published": None}.get(k, default)
    e.source = None
    e.published_parsed = None
    feed = MagicMock()
    feed.entries = [e]
    feed.bozo = False
    with patch("src.sources.google_alerts.feedparser.parse", return_value=feed):
        jobs, _, _ = google_alerts.fetch(feed_urls=["a", "b"])
    assert len(jobs) == 1


# ────────────────────── Phase C: apply_url extraction ──────────────

def test_jsearch_apply_url_prefers_direct_link():
    body = {"data": [{
        "job_id": "abc",
        "job_title": "PA Manager",
        "employer_name": "X",
        "job_apply_link": "https://careers.x.com/apply/abc",
        "job_google_link": "https://google.com/jobs?q=abc",
    }]}
    with patch("src.sources.jsearch.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = jsearch.fetch("k", queries=[{"query": "q"}])
    assert jobs[0]["apply_url"] == "https://careers.x.com/apply/abc"


def test_jsearch_apply_url_falls_back_to_google_link():
    body = {"data": [{
        "job_id": "abc",
        "job_title": "PA Manager",
        "employer_name": "X",
        "job_google_link": "https://google.com/jobs?q=abc",
    }]}
    with patch("src.sources.jsearch.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = jsearch.fetch("k", queries=[{"query": "q"}])
    assert jobs[0]["apply_url"] == "https://google.com/jobs?q=abc"


def test_jooble_apply_url_is_link():
    body = {"jobs": [{"id": 1, "title": "T", "company": "C", "link": "https://jooble.org/desc/1"}]}
    with patch("src.sources.jooble.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = jooble.fetch("k", queries=[{"keywords": "x", "location": "United States"}])
    assert jobs[0]["apply_url"] == "https://jooble.org/desc/1"


def test_adzuna_apply_url_is_redirect_url():
    body = {"results": [{
        "id": "1", "title": "T",
        "company": {"display_name": "C"},
        "redirect_url": "https://adzuna.com/redirect/1",
    }]}
    with patch("src.sources.adzuna.retry_request", return_value=_mock_resp(body)), \
         patch("src.sources.adzuna.time.sleep"):
        jobs, _, _ = adzuna.fetch("id", "k", queries=[{"what": "q"}], countries=["us"])
    assert jobs[0]["apply_url"] == "https://adzuna.com/redirect/1"


def test_usajobs_apply_url_is_position_uri():
    body = {"SearchResult": {"SearchResultItems": [{
        "MatchedObjectId": "123",
        "MatchedObjectDescriptor": {
            "PositionTitle": "Analyst",
            "OrganizationName": "OPM",
            "PositionURI": "https://usajobs.gov/GetJob/123",
        },
    }]}}
    with patch("src.sources.usajobs.retry_request", return_value=_mock_resp(body)):
        jobs, _, _ = usajobs.fetch("me@x.com", "k", keywords=["q"])
    assert jobs[0]["apply_url"] == "https://usajobs.gov/GetJob/123"


def test_google_alerts_apply_url_is_link():
    e = MagicMock()
    e.get = lambda k, default=None: {
        "link": "https://example.com/job/42",
        "title": "Employee Listening Manager - Example",
        "summary": "",
        "published": None,
    }.get(k, default)
    e.source = None
    e.published_parsed = None
    feed = MagicMock()
    feed.entries = [e]
    feed.bozo = False
    with patch("src.sources.google_alerts.feedparser.parse", return_value=feed):
        jobs, _, _ = google_alerts.fetch(feed_urls=["https://fake/rss"])
    assert jobs[0]["apply_url"] == "https://example.com/job/42"


def test_build_job_apply_url_defaults_to_source_url():
    from src.shared import build_job
    j = build_job(
        source_name="test",
        external_id="t1",
        title="T",
        company="C",
        source_url="https://example.com/job/1",
    )
    assert j["apply_url"] == "https://example.com/job/1"


def test_build_job_apply_url_explicit_wins():
    from src.shared import build_job
    j = build_job(
        source_name="test",
        external_id="t1",
        title="T",
        company="C",
        source_url="https://aggregator.com/redirect/1",
        apply_url="https://careers.c.com/apply/1",
    )
    assert j["apply_url"] == "https://careers.c.com/apply/1"
    assert j["source_url"] == "https://aggregator.com/redirect/1"


# ────────────────────── All-sources dict contract ──────────────────

def test_all_sources_return_standardized_shape():
    """Every source must return (list[dict], list[str], dict)."""
    # JSearch with empty key
    r = jsearch.fetch("")
    assert isinstance(r, tuple) and len(r) == 3
    assert isinstance(r[0], list) and isinstance(r[1], list) and isinstance(r[2], dict)
    # Jooble
    r = jooble.fetch("")
    assert len(r) == 3 and isinstance(r[2], dict)
    # Adzuna
    r = adzuna.fetch("", "")
    assert len(r) == 3 and isinstance(r[2], dict)
    # USAJobs
    r = usajobs.fetch("", "")
    assert len(r) == 3 and isinstance(r[2], dict)
    # Google Alerts
    r = google_alerts.fetch(feed_urls=[])
    assert len(r) == 3 and isinstance(r[2], dict)
