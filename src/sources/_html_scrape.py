"""Shared helpers for the Phase 4 (R3) HTML scrapers.

BeautifulSoup is an optional dependency — the helpers fall back to regex-based
parsing if it isn't installed. Exact selectors for each site are maintained in
the individual source modules; this module supplies only the plumbing."""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Iterable

import requests

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 20.0
DEFAULT_UA = "Mozilla/5.0 (compatible; job-monitor/1.0)"

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def fetch_html(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> tuple[str, int, str]:
    """GET `url` and return (html, status_code, final_url). On exception returns
    ("", 0, url) so callers can branch on status == 200."""
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": DEFAULT_UA},
            allow_redirects=True,
        )
    except requests.RequestException as e:
        log.warning("html_scrape: failed to fetch %s: %s", url, e)
        return "", 0, url
    return resp.text or "", resp.status_code, getattr(resp, "url", url)


def strip_tags(html: str) -> str:
    if not html:
        return ""
    return _WS_RE.sub(" ", _HTML_TAG_RE.sub(" ", html)).strip()


def content_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


def try_bs4(html: str):
    """Return a BeautifulSoup object if the library is importable, else None."""
    if not html:
        return None
    try:
        from bs4 import BeautifulSoup  # type: ignore
    except Exception:  # noqa: BLE001
        return None
    try:
        return BeautifulSoup(html, "html.parser")
    except Exception as e:  # noqa: BLE001
        log.warning("html_scrape: bs4 failed: %s", e)
        return None


def iter_anchors_fallback(html: str) -> Iterable[tuple[str, str]]:
    """Regex fallback when bs4 isn't available: yield (href, inner_text_stripped)."""
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, re.IGNORECASE | re.DOTALL):
        yield m.group(1), strip_tags(m.group(2))
