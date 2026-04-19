"""Google Alerts + Talkwalker + SIOP RSS feeds. All results require mandatory LLM review."""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import feedparser
import requests

from src.shared import build_job, env

log = logging.getLogger(__name__)

# R8-M11: feedparser.parse(url) invokes urllib internally with no timeout,
# so a hung RSS host can block the entire collector for minutes. Fetch via
# requests (which has a real timeout) and pass the response text to
# feedparser so the parse step is network-free.
RSS_FETCH_TIMEOUT_SEC = 15.0
_RSS_USER_AGENT = "Mozilla/5.0 (compatible; job-monitor-rss/1.0)"


def _fetch_feed(url: str):
    """Return a parsed feedparser object. On network failure returns a
    feedparser-shaped empty object so callers can treat it uniformly."""
    try:
        resp = requests.get(
            url,
            timeout=RSS_FETCH_TIMEOUT_SEC,
            headers={"User-Agent": _RSS_USER_AGENT},
        )
    except requests.RequestException as e:
        log.warning("google_alerts: fetch error on %s: %s", url[:60], e)
        return feedparser.parse("")  # empty, .entries == []
    if resp.status_code != 200:
        log.warning("google_alerts: HTTP %d on %s", resp.status_code, url[:60])
        return feedparser.parse("")
    return feedparser.parse(resp.text)

_BLOG_HEURISTIC = [
    "blog", "article", "guide", "how to", "opinion", "review",
    "podcast", "webinar",
]

_FEED_ENV_VARS = [
    "GOOGLE_ALERT_RSS_1", "GOOGLE_ALERT_RSS_2", "GOOGLE_ALERT_RSS_3",
    "GOOGLE_ALERT_RSS_4", "GOOGLE_ALERT_RSS_5", "GOOGLE_ALERT_SIOP",
    "TALKWALKER_RSS_1", "TALKWALKER_RSS_2", "TALKWALKER_RSS_3",
]


def _looks_like_content_not_job(title: str) -> bool:
    lower = title.lower()
    return any(h in lower for h in _BLOG_HEURISTIC)


def _extract_company(entry: Any, title: str) -> str:
    """Try source.title, then trailing segment of title."""
    src = getattr(entry, "source", None) or (entry.get("source") if hasattr(entry, "get") else None)
    if src:
        src_title = getattr(src, "title", None) or (src.get("title") if hasattr(src, "get") else None)
        if src_title:
            return str(src_title).strip()
    for sep in [" - ", " | ", " – ", " — "]:
        if sep in title:
            return title.split(sep)[-1].strip()
    return ""


def _map(entry: Any) -> dict[str, Any] | None:
    link = entry.get("link") or ""
    title = entry.get("title") or ""
    if not link or not title:
        return None
    title = title.strip()
    if _looks_like_content_not_job(title):
        return None
    ext_id = f"galert_{hashlib.sha256(link.encode('utf-8')).hexdigest()[:12]}"
    summary = entry.get("summary") or ""
    company = _extract_company(entry, title)
    published = entry.get("published")
    return build_job(
        source_name="google_alerts",
        external_id=ext_id,
        title=title,
        company=company,
        source_url=link,
        apply_url=link,
        description=summary,
        description_is_snippet=True,
        date_posted=published,
        raw_data={"title": title, "link": link, "published": published},
    )


def _feed_is_stale(parsed: Any, days: int = 7) -> bool:
    """True if no entry in last `days` days. Best-effort — Google Alerts can silently stop."""
    if not parsed.entries:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    for e in parsed.entries:
        pp = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        if pp:
            try:
                dt = datetime(*pp[:6], tzinfo=timezone.utc)
                if dt > cutoff:
                    return False
            except (TypeError, ValueError):
                continue
    return True


def fetch(feed_urls: list[str] | None = None) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    """Read all configured RSS feeds. Empty URL vars are silently skipped."""
    if feed_urls is None:
        feed_urls = [env(v) for v in _FEED_ENV_VARS]
    active = [u for u in feed_urls if u]
    if not active:
        return [], [], {"stale_feeds": []}

    results: list[dict[str, Any]] = []
    errors: list[str] = []
    stale: list[str] = []
    seen_ext_ids: set[str] = set()
    for url in active:
        try:
            parsed = _fetch_feed(url)
            if getattr(parsed, "bozo", False) and getattr(parsed, "bozo_exception", None):
                # feedparser reports parse warnings in bozo. Log but continue — feeds often still parseable.
                log.warning("google_alerts: parse warning on %s: %s", url[:60], parsed.bozo_exception)
            if _feed_is_stale(parsed):
                stale.append(url)
            for entry in parsed.entries[:20]:  # Google Alerts caps at 20
                try:
                    job = _map(entry)
                    if job and job["external_id"] not in seen_ext_ids:
                        seen_ext_ids.add(job["external_id"])
                        results.append(job)
                except Exception as e:  # noqa: BLE001
                    errors.append(f"google_alerts: map error: {e}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"google_alerts: fetch error on {url[:60]}: {e}")

    return results, errors, {"stale_feeds": stale}
