"""Turso-side archival with a Phase 6 (R3) two-step lifecycle:

1. 7+ days unseen AND lifecycle_status='active'  → mark 'likely_closed' (muted in UI)
2. 21+ days unseen (14+ days past the likely_closed transition) → full archive

WordPress-side archival is handled independently by the WP plugin's cron."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from src import db

log = logging.getLogger(__name__)

LIKELY_CLOSED_DAYS = 7   # after this many days unseen → likely_closed
ARCHIVE_DAYS = 21        # after this many days unseen → full archive (likely_closed + 14)


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


def mark_likely_closed(conn, *, days: int = LIKELY_CLOSED_DAYS) -> dict[str, int]:
    """Step 1: jobs unseen for `days` days get marked 'likely_closed'. Still
    visible in the table, just muted."""
    stale = db.get_active_stale_jobs(conn, days=days)
    marked = 0
    for row in stale:
        try:
            db.mark_job_likely_closed(conn, row["external_id"])
            marked += 1
        except Exception as e:  # noqa: BLE001
            log.warning("archiver: mark_likely_closed failed on %s: %s",
                        row.get("external_id"), e)
    return {"marked_likely_closed": marked}


def archive_stale(conn, *, days: int = ARCHIVE_DAYS) -> dict[str, int]:
    """Step 2 (+ step 1): jobs unseen for `days` days get fully archived
    (is_active=0). Runs step 1 first so freshly-stale jobs slide into
    likely_closed rather than jumping straight to archived."""
    step1 = mark_likely_closed(conn, days=LIKELY_CLOSED_DAYS)

    stale = db.get_stale_active_jobs(conn, days=days)
    archived = 0
    for row in stale:
        try:
            days_active = _days_between(row.get("first_seen_date"), row.get("last_seen_date"))
            db.archive_job(conn, row["external_id"], days_active=days_active)
            archived += 1
        except Exception as e:  # noqa: BLE001
            log.warning("archiver: failed on %s: %s", row.get("external_id"), e)
    return {"archived": archived, **step1}
