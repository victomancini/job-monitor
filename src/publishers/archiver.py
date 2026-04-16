"""Turso-side archival. Marks jobs as inactive after 7 days unseen, sets archived_date
and days_active. WordPress-side archival is handled independently by the WP plugin's cron.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from src import db

log = logging.getLogger(__name__)

STALE_DAYS = 7


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.split("T")[0]).date()
    except (TypeError, ValueError):
        return None


def _days_between(start: str | None, end: str | None) -> int:
    a = _parse_date(start)
    b = _parse_date(end)
    if not a or not b:
        return 0
    return max(1, (b - a).days)


def archive_stale(conn, *, days: int = STALE_DAYS) -> dict[str, int]:
    """Find active jobs with last_seen_date older than `days` and mark them archived.
    Returns {'archived': N}. db.get_stale_active_jobs already enforces LIMIT 100.
    """
    stale = db.get_stale_active_jobs(conn, days=days)
    archived = 0
    for row in stale:
        try:
            days_active = _days_between(row.get("first_seen_date"), row.get("last_seen_date"))
            db.archive_job(conn, row["external_id"], days_active=days_active)
            archived += 1
        except Exception as e:  # noqa: BLE001
            log.warning("archiver: failed on %s: %s", row.get("external_id"), e)
    return {"archived": archived}
