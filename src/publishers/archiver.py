"""Turso-side archival with a two-track lifecycle:

Track A — source-of-truth veto (fast-path). Jobs whose source page or ATS API
confirmed them closed (lifecycle_checker flipped lifecycle_status to
'likely_closed' AND stamped last_lifecycle_check) are archived immediately.
No grace period — we have an authoritative 404.

Track B — aggregator staleness (time-based). For jobs where no authoritative
check is available (aggregator-only apply_url):
  1. 7+ days unseen AND lifecycle_status='active'  → mark 'likely_closed'
  2. 21+ days unseen → full archive (likely_closed + 14 days grace)

WordPress-side archival is handled independently by the WP plugin's cron."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
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
    stale = db.get_jobs_to_mark_likely_closed(conn, days=days)
    marked = 0
    for row in stale:
        try:
            db.mark_job_likely_closed(conn, row["external_id"])
            marked += 1
        except Exception as e:  # noqa: BLE001
            log.warning("archiver: mark_likely_closed failed on %s: %s",
                        row.get("external_id"), e)
    return {"marked_likely_closed": marked}


def archive_confirmed_closed(conn) -> int:
    """Track A fast-path: archive any active row whose lifecycle_checker
    already flipped it to 'likely_closed' via a source-of-truth check (ATS API
    404 or direct company page 404). Skips the 21-day grace window — we have
    an authoritative signal."""
    rows = db.get_jobs_to_archive_confirmed_closed(conn)
    archived = 0
    for row in rows:
        try:
            days_active = _days_between(row.get("first_seen_date"), row.get("last_seen_date"))
            db.archive_job(conn, row["external_id"], days_active=days_active)
            archived += 1
        except Exception as e:  # noqa: BLE001
            log.warning("archiver: fast-path failed on %s: %s", row.get("external_id"), e)
    return archived


def archive_stale(conn, *, days: int = ARCHIVE_DAYS) -> dict[str, int]:
    """Run both tracks. Track A first so confirmed-closed jobs archive
    immediately; then Track B for everything else still hanging on."""
    # Track A — fast-path confirmed closures
    fast_archived = archive_confirmed_closed(conn)

    # Track B — time-based. Step 1 (mark likely_closed) then step 2 (archive)
    step1 = mark_likely_closed(conn, days=LIKELY_CLOSED_DAYS)

    stale = db.get_jobs_to_archive(conn, days=days)
    archived = 0
    for row in stale:
        try:
            days_active = _days_between(row.get("first_seen_date"), row.get("last_seen_date"))
            db.archive_job(conn, row["external_id"], days_active=days_active)
            archived += 1
        except Exception as e:  # noqa: BLE001
            log.warning("archiver: failed on %s: %s", row.get("external_id"), e)
    return {
        "archived": archived + fast_archived,
        "archived_fast_path": fast_archived,
        **step1,
    }
