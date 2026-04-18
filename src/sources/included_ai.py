"""Scraper for Included.ai's community PA job board:
`https://included.ai/roles-in-people-analytics/`

Same cache + parsing pattern as One Model."""
from __future__ import annotations

import hashlib
import logging
from typing import Any

from src import db as dbmod
from src.shared import build_job
from src.sources import _html_scrape
from src.sources.onemodel import _split_title_company

log = logging.getLogger(__name__)

BOARD_URL = "https://included.ai/roles-in-people-analytics/"
SOURCE_NAME = "included_ai"
ATS_NAME = "included_ai"
CACHE_SLUG = "main"


def _parse(html: str) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    soup = _html_scrape.try_bs4(html)
    if soup is not None:
        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True)
            href = a["href"]
            if not text or not href.startswith("http"):
                continue
            title, company = _split_title_company(text)
            if title and company:
                out.append((title, company, href))
        return out
    for href, text in _html_scrape.iter_anchors_fallback(html):
        if not text or not href.startswith("http"):
            continue
        title, company = _split_title_company(text)
        if title and company:
            out.append((title, company, href))
    return out


def fetch(conn=None) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    html, status, _ = _html_scrape.fetch_html(BOARD_URL)
    if status != 200 or not html:
        return [], [f"included_ai: HTTP {status}"] if status else ["included_ai: fetch failed"], {
            "checked": False,
        }
    page_hash = _html_scrape.content_hash(html)
    cached = dbmod.get_ats_status(conn, ATS_NAME, CACHE_SLUG) if conn is not None else None
    if cached and cached.get("status") == page_hash:
        return [], [], {"checked": True, "unchanged": True}

    entries = _parse(html)
    jobs: list[dict[str, Any]] = []
    for title, company, url in entries:
        jid = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        try:
            jobs.append(build_job(
                source_name=SOURCE_NAME,
                external_id=f"{SOURCE_NAME}_{jid}",
                title=title,
                company=company,
                source_url=url,
                apply_url=url,
                raw_data={"url": url, "title": title, "company": company},
            ))
        except Exception as e:  # noqa: BLE001
            log.warning("included_ai: map error on %s: %s", url, e)

    if conn is not None:
        dbmod.set_ats_status(conn, ATS_NAME, CACHE_SLUG, page_hash, jobs_found=len(jobs))

    return jobs, [], {"checked": True, "unchanged": False, "found": len(jobs)}
