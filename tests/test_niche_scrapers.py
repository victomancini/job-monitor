"""Tests for Phase 4 (R3) niche PA boards: One Model, Included.ai, SIOP."""
from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from src import db as dbmod
from src.sources import included_ai, onemodel, siop, _html_scrape


SAMPLE_HTML = """
<html><body>
<a href="https://careers.netflix.com/job/1">Senior Manager, People Analytics — Netflix</a>
<a href="https://careers.airbnb.com/position/abc">Staff People Scientist at Airbnb</a>
<a href="https://example.com/blog">Some Blog Post</a>
<a href="/relative/path">Relative Link Should Be Skipped</a>
</body></html>
"""


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    dbmod.migrate(c)
    yield c
    c.close()


# ──────────────────────── onemodel ────────────────────────

def test_onemodel_parses_anchors(conn):
    with patch("src.sources._html_scrape.requests.get") as mget:
        mget.return_value = MagicMock(status_code=200, text=SAMPLE_HTML,
                                      url=onemodel.BOARD_URL)
        jobs, errors, meta = onemodel.fetch(conn=conn)
    assert errors == []
    # Two anchors with "Title — Company" / "Title at Company" shapes
    titles = {j["title"] for j in jobs}
    assert "Senior Manager, People Analytics" in titles
    assert "Staff People Scientist" in titles
    assert meta["checked"] is True
    assert meta["unchanged"] is False


def test_onemodel_cache_detects_unchanged(conn):
    with patch("src.sources._html_scrape.requests.get") as mget:
        mget.return_value = MagicMock(status_code=200, text=SAMPLE_HTML,
                                      url=onemodel.BOARD_URL)
        onemodel.fetch(conn=conn)
        # Second call with identical HTML should be short-circuited
        jobs, _, meta = onemodel.fetch(conn=conn)
    assert jobs == []
    assert meta.get("unchanged") is True


def test_onemodel_http_error_returns_error_string():
    with patch("src.sources._html_scrape.requests.get") as mget:
        mget.return_value = MagicMock(status_code=500, text="",
                                      url=onemodel.BOARD_URL)
        jobs, errors, _ = onemodel.fetch()
    assert jobs == []
    assert len(errors) == 1


def test_onemodel_fetch_exception_returns_error():
    with patch("src.sources._html_scrape.requests.get",
               side_effect=__import__("requests").ConnectionError("boom")):
        jobs, errors, _ = onemodel.fetch()
    assert jobs == []
    assert len(errors) == 1


# ──────────────────────── included.ai ─────────────────────

def test_included_ai_parses_anchors():
    with patch("src.sources._html_scrape.requests.get") as mget:
        mget.return_value = MagicMock(status_code=200, text=SAMPLE_HTML,
                                      url=included_ai.BOARD_URL)
        jobs, errors, _ = included_ai.fetch()
    assert len(jobs) >= 2
    for j in jobs:
        assert j["source_name"] == "included_ai"
        assert j["apply_url"].startswith("http")


# ──────────────────────── SIOP ────────────────────────────

def test_siop_rss_path():
    """If feedparser returns entries, we use RSS and skip HTML fetch."""
    fake_entry = MagicMock()
    fake_entry.get = lambda k, default=None: {
        "link": "https://jobs.siop.org/jobs/42",
        "title": "Industrial-Organizational Psychologist — Some Univ",
        "summary": "Teaching + research role.",
    }.get(k, default)
    fake_entry.link = "https://jobs.siop.org/jobs/42"
    fake_entry.title = "Industrial-Organizational Psychologist — Some Univ"
    fake_entry.summary = "Teaching + research role."
    fake_entry.source = None

    fake_feed = MagicMock()
    fake_feed.entries = [fake_entry]

    with patch("src.sources.siop.feedparser.parse", return_value=fake_feed), \
         patch("src.sources.siop._html_scrape.fetch_html") as mfetch:
        jobs, errors, meta = siop.fetch()
    assert len(jobs) == 1
    assert meta["via"] == "rss"
    assert jobs[0]["title"] == "Industrial-Organizational Psychologist"
    assert jobs[0]["company"] == "Some Univ"
    # HTML fallback must not be reached
    mfetch.assert_not_called()


def test_siop_html_fallback_when_rss_empty():
    fake_feed = MagicMock()
    fake_feed.entries = []
    with patch("src.sources.siop.feedparser.parse", return_value=fake_feed), \
         patch("src.sources.siop._html_scrape.fetch_html",
               return_value=(SAMPLE_HTML, 200, siop.BOARD_URL)):
        jobs, errors, meta = siop.fetch()
    assert meta["via"] == "html"
    assert len(jobs) >= 1


# ──────────────────────── helper coverage ──────────────────

def test_split_title_company_handles_various_separators():
    cases = [
        ("Senior Manager — Netflix", ("Senior Manager", "Netflix")),
        ("People Scientist at Airbnb", ("People Scientist", "Airbnb")),
        ("Staff Researcher | Google", ("Staff Researcher", "Google")),
        ("Just a Title", ("", "")),
    ]
    for text, expected in cases:
        assert onemodel._split_title_company(text) == expected


def test_content_hash_stable():
    h1 = _html_scrape.content_hash("<html>X</html>")
    h2 = _html_scrape.content_hash("<html>X</html>")
    h3 = _html_scrape.content_hash("<html>Y</html>")
    assert h1 == h2
    assert h1 != h3


def test_strip_tags():
    assert _html_scrape.strip_tags("<p>Hello <b>World</b></p>") == "Hello World"
    assert _html_scrape.strip_tags("") == ""
