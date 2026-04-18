"""Phase 7 (R3): end-of-pipeline aggregations for the dashboard.

Writes (stat_date, stat_type, stat_key, stat_value) rows into `monthly_stats`.
Rows are upserted with `ON CONFLICT (stat_date, stat_type, stat_key) DO UPDATE`
so re-running on the same date overwrites previous snapshots rather than
duplicating them.
"""
from __future__ import annotations

import logging
import statistics
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

# Seniority tiers we compute salary percentiles for. "Intern" omitted — rarely
# has meaningful salary coverage.
_SALARY_TIERS = ["IC", "Senior IC", "Manager", "Senior Manager",
                 "Director", "Senior Director", "VP", "Executive"]


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _upsert_stat(conn, stat_date: str, stat_type: str, stat_key: str, stat_value: int) -> None:
    if stat_key is None or stat_key == "":
        return
    conn.execute(
        "INSERT INTO monthly_stats (stat_date, stat_type, stat_key, stat_value) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(stat_date, stat_type, stat_key) DO UPDATE SET stat_value=excluded.stat_value",
        (stat_date, stat_type, stat_key, int(stat_value)),
    )


def aggregate_daily_stats(conn, today: str | None = None) -> dict[str, int]:
    """Snapshot today's active-job slices into monthly_stats. Returns a
    small dict of counts for logging."""
    today = today or _today()
    written = 0

    def _each(query: str, stat_type: str, default_key: str = "") -> None:
        nonlocal written
        for key, count in conn.execute(query).fetchall():
            k = (key if key is not None else "") or default_key
            if not k:
                continue
            _upsert_stat(conn, today, stat_type, str(k), int(count or 0))
            written += 1

    # Jobs by category
    _each(
        "SELECT category, COUNT(*) FROM jobs WHERE is_active=1 GROUP BY category",
        "category_count",
        default_key="General PA",
    )

    # Jobs by seniority
    _each(
        "SELECT seniority, COUNT(*) FROM jobs WHERE is_active=1 GROUP BY seniority",
        "seniority_count",
        default_key="Unknown",
    )

    # Remote distribution
    _each(
        "SELECT is_remote, COUNT(*) FROM jobs WHERE is_active=1 GROUP BY is_remote",
        "remote_count",
        default_key="unknown",
    )

    # Top hiring companies (LIMIT 20)
    for company, count in conn.execute(
        "SELECT company, COUNT(*) AS c FROM jobs WHERE is_active=1 "
        "GROUP BY company ORDER BY c DESC LIMIT 20"
    ).fetchall():
        if not company:
            continue
        _upsert_stat(conn, today, "company_count", company, int(count or 0))
        written += 1

    # Vendor mentions (from comma-separated vendors_mentioned column)
    vendor_counts: dict[str, int] = {}
    for (vendors_str,) in conn.execute(
        "SELECT vendors_mentioned FROM jobs WHERE is_active=1 "
        "AND vendors_mentioned IS NOT NULL AND vendors_mentioned != ''"
    ).fetchall():
        for v in (vendors_str or "").split(","):
            v = v.strip()
            if v:
                vendor_counts[v] = vendor_counts.get(v, 0) + 1
    for vendor, count in vendor_counts.items():
        _upsert_stat(conn, today, "vendor_count", vendor, count)
        written += 1

    # Salary percentiles per seniority (p25 / p50 / p75)
    for sen in _SALARY_TIERS:
        rows = conn.execute(
            "SELECT salary_min FROM jobs WHERE is_active=1 AND seniority=? "
            "AND salary_min IS NOT NULL AND salary_min > 0 ORDER BY salary_min",
            (sen,),
        ).fetchall()
        vals = [float(r[0]) for r in rows if r[0] is not None]
        if len(vals) >= 3:
            q = statistics.quantiles(vals, n=4)  # q[0]=p25, q[2]=p75
            _upsert_stat(conn, today, "salary_p25", sen, int(q[0]))
            _upsert_stat(conn, today, "salary_p50", sen, int(statistics.median(vals)))
            _upsert_stat(conn, today, "salary_p75", sen, int(q[2]))
            written += 3

    # Total active count — the dashboard trend line uses this by default
    total = conn.execute("SELECT COUNT(*) FROM jobs WHERE is_active=1").fetchone()[0] or 0
    _upsert_stat(conn, today, "total_active", "all", int(total))
    written += 1

    conn.commit()
    return {"rows_written": written, "total_active": int(total)}


def build_dashboard_payload(conn, today: str | None = None, trend_days: int = 30) -> dict[str, Any]:
    """Read the monthly_stats table and shape the JSON the WP dashboard consumes."""
    today = today or _today()

    def _slice(stat_type: str, for_date: str | None = None) -> dict[str, int]:
        cur = conn.execute(
            "SELECT stat_key, stat_value FROM monthly_stats "
            "WHERE stat_date=? AND stat_type=? ORDER BY stat_value DESC",
            (for_date or today, stat_type),
        )
        return {row[0]: int(row[1] or 0) for row in cur.fetchall()}

    def _top_list(stat_type: str, limit: int, for_date: str | None = None) -> list[dict[str, Any]]:
        cur = conn.execute(
            "SELECT stat_key, stat_value FROM monthly_stats "
            "WHERE stat_date=? AND stat_type=? ORDER BY stat_value DESC LIMIT ?",
            (for_date or today, stat_type, limit),
        )
        return [{"name": row[0], "count": int(row[1] or 0)} for row in cur.fetchall()]

    trend_cur = conn.execute(
        "SELECT stat_date, stat_value FROM monthly_stats "
        "WHERE stat_type='total_active' AND stat_key='all' "
        "ORDER BY stat_date DESC LIMIT ?",
        (trend_days,),
    )
    trend = [{"date": r[0], "count": int(r[1] or 0)} for r in trend_cur.fetchall()]
    trend.reverse()  # chronological

    salary_bands: dict[str, dict[str, int]] = {}
    for tier in _SALARY_TIERS:
        row = conn.execute(
            "SELECT "
            "MAX(CASE WHEN stat_type='salary_p25' THEN stat_value END), "
            "MAX(CASE WHEN stat_type='salary_p50' THEN stat_value END), "
            "MAX(CASE WHEN stat_type='salary_p75' THEN stat_value END) "
            "FROM monthly_stats WHERE stat_date=? AND stat_key=?",
            (today, tier),
        ).fetchone()
        if row and any(v is not None for v in row):
            salary_bands[tier] = {
                "p25": int(row[0] or 0),
                "p50": int(row[1] or 0),
                "p75": int(row[2] or 0),
            }

    return {
        "snapshot_date": today,
        "category_count": _slice("category_count"),
        "seniority_count": _slice("seniority_count"),
        "remote_count": _slice("remote_count"),
        "company_count": _top_list("company_count", 10),
        "vendor_count": _top_list("vendor_count", 15),
        "total_active_trend": trend,
        "salary_bands": salary_bands,
    }
