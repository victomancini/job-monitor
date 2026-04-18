"""Scraper for One Model's community job board:
`https://www.onemodel.co/roles-in-people-analytics-hr-technology`

Static HTML with a list of job links. Updates every ~2 weeks so we cache the
page content hash in `ats_company_status` (reusing that table with
ats='onemodel' and slug='main') to avoid reprocessing unchanged pages."""
from __future__ import annotations

import hashlib
import logging
from typing import Any

from src import db as dbmod
from src.shared import build_job
from src.sources import _html_scrape

log = logging.getLogger(__name__)

BOARD_URL = "https://www.onemodel.co/roles-in-people-analytics-hr-technology"
SOURCE_NAME = "onemodel"
ATS_NAME = "onemodel"
CACHE_SLUG = "main"


def _parse(html: str) -> list[tuple[str, str, str]]:
    """Return list of (title, company, url) tuples."""
    out: list[tuple[str, str, str]] = []
    soup = _html_scrape.try_bs4(html)
    if soup is not None:
        # Best effort: any anchor whose text contains a "-" separating Title and Company.
        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True)
            href = a["href"]
            if not text or not href.startswith("http"):
                continue
            # One Model format is typically "Title — Company" or "Title at Company"
            title, company = _split_title_company(text)
            if title and company:
                out.append((title, company, href))
        return out
    # Fallback: regex anchor sweep
    for href, text in _html_scrape.iter_anchors_fallback(html):
        if not text or not href.startswith("http"):
            continue
        title, company = _split_title_company(text)
        if title and company:
            out.append((title, company, href))
    return out


def _split_title_company(text: str) -> tuple[str, str]:
    for sep in (" — ", " – ", " - ", " at ", " @ ", " | "):
        if sep in text:
            left, right = text.split(sep, 1)
            return left.strip(), right.strip()
    return "", ""


def fetch(conn=None) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    html, status, _ = _html_scrape.fetch_html(BOARD_URL)
    if status != 200 or not html:
        return [], [f"onemodel: HTTP {status}"] if status else ["onemodel: fetch failed"], {
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
            log.warning("onemodel: map error on %s: %s", url, e)

    if conn is not None:
        # Store content hash in the 'status' column for cheap no-change detection.
        dbmod.set_ats_status(conn, ATS_NAME, CACHE_SLUG, page_hash, jobs_found=len(jobs))

    return jobs, [], {"checked": True, "unchanged": False, "found": len(jobs)}
