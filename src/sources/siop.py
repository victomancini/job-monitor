"""Scraper for the SIOP Career Center job board:
`https://jobs.siop.org/jobs/`

Best-effort: try RSS first (SIOP historically exposed a feed), then fall back
to HTML scraping via feedparser + BeautifulSoup. Specialist I-O psychology
positions are the key coverage gap this plugs — they rarely make it to
aggregators."""
from __future__ import annotations

import hashlib
import logging
from typing import Any

import feedparser

from src import db as dbmod
from src.shared import build_job
from src.sources import _html_scrape
from src.sources.onemodel import _split_title_company

log = logging.getLogger(__name__)

RSS_URL = "https://jobs.siop.org/jobs/rss/"
BOARD_URL = "https://jobs.siop.org/jobs/"
SOURCE_NAME = "siop"
ATS_NAME = "siop"
CACHE_SLUG = "main"


def _rss_to_jobs(rss_url: str) -> list[dict[str, Any]]:
    try:
        parsed = feedparser.parse(rss_url)
    except Exception as e:  # noqa: BLE001
        log.warning("siop: feedparser error on %s: %s", rss_url, e)
        return []
    entries = list(getattr(parsed, "entries", []) or [])
    jobs: list[dict[str, Any]] = []
    for entry in entries:
        link = getattr(entry, "link", "") or (entry.get("link") if hasattr(entry, "get") else "")
        title = getattr(entry, "title", "") or (entry.get("title") if hasattr(entry, "get") else "")
        if not link or not title:
            continue
        company = ""
        # Some RSS feeds put company in <source> or in title as "Title — Company"
        src = getattr(entry, "source", None)
        if src is not None:
            company = getattr(src, "title", None) or (src.get("title") if hasattr(src, "get") else "") or ""
        if not company:
            t, c = _split_title_company(title)
            if t and c:
                title, company = t, c
        summary = getattr(entry, "summary", "") or (entry.get("summary") if hasattr(entry, "get") else "")
        jid = hashlib.sha256(link.encode("utf-8")).hexdigest()[:12]
        jobs.append(build_job(
            source_name=SOURCE_NAME,
            external_id=f"{SOURCE_NAME}_{jid}",
            title=title,
            company=company,
            source_url=link,
            apply_url=link,
            description=summary,
            description_is_snippet=True,
            raw_data={"link": link, "title": title, "company": company},
        ))
    return jobs


def _html_to_jobs(html: str) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    soup = _html_scrape.try_bs4(html)
    if soup is not None:
        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True)
            href = a["href"]
            if not text or not href.startswith("http"):
                continue
            title, company = _split_title_company(text)
            if title and company:
                jid = hashlib.sha256(href.encode("utf-8")).hexdigest()[:12]
                jobs.append(build_job(
                    source_name=SOURCE_NAME,
                    external_id=f"{SOURCE_NAME}_{jid}",
                    title=title,
                    company=company,
                    source_url=href,
                    apply_url=href,
                ))
        return jobs
    for href, text in _html_scrape.iter_anchors_fallback(html):
        if not text or not href.startswith("http"):
            continue
        title, company = _split_title_company(text)
        if title and company:
            jid = hashlib.sha256(href.encode("utf-8")).hexdigest()[:12]
            jobs.append(build_job(
                source_name=SOURCE_NAME,
                external_id=f"{SOURCE_NAME}_{jid}",
                title=title,
                company=company,
                source_url=href,
                apply_url=href,
            ))
    return jobs


def fetch(conn=None) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    # 1. Try RSS first
    jobs = _rss_to_jobs(RSS_URL)
    if jobs:
        return jobs, [], {"via": "rss", "found": len(jobs)}

    # 2. Fall back to HTML scrape
    html, status, _ = _html_scrape.fetch_html(BOARD_URL)
    if status != 200 or not html:
        msg = f"siop: HTTP {status}" if status else "siop: fetch failed"
        return [], [msg], {"via": "html", "checked": False}

    page_hash = _html_scrape.content_hash(html)
    cached = dbmod.get_ats_status(conn, ATS_NAME, CACHE_SLUG) if conn is not None else None
    if cached and cached.get("status") == page_hash:
        return [], [], {"via": "html", "unchanged": True}

    jobs = _html_to_jobs(html)
    if conn is not None:
        dbmod.set_ats_status(conn, ATS_NAME, CACHE_SLUG, page_hash, jobs_found=len(jobs))
    return jobs, [], {"via": "html", "unchanged": False, "found": len(jobs)}
