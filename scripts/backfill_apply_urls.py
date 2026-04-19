"""R9-Part-2-D: one-shot backfill of apply_urls that are still pointing to
aggregator hosts. Run this ONCE after deploying the Part 2 enrichment fix.

What it does:
  1. Reads all active jobs from Turso where `apply_url` host is an aggregator.
  2. Runs the same resolution chain enrich_job uses (HTTP GET → body-redirect
     parse → HEAD fallback), without touching salary/remote/location — we
     only care about the URL.
  3. Writes resolved URLs back to Turso via db.upgrade_apply_url.
  4. Pushes updated rows to WordPress using the minimal WP payload helper.

Usage:
    export TURSO_DB_URL=libsql://...
    export TURSO_AUTH_TOKEN=...
    export WP_URL=https://...
    export WP_USERNAME=...
    export WP_APP_PASSWORD=...
    python scripts/backfill_apply_urls.py            # dry run (no writes)
    python scripts/backfill_apply_urls.py --apply    # actually write

Prints a progress line per job and a final summary.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Any
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import db
from src.processors.enrichment import (
    _extract_body_redirect,
    _head_final_url,
    USER_AGENT,
    FETCH_TIMEOUT_SEC,
)
from src.publishers import wordpress
from src.shared import is_aggregator_host

import requests

log = logging.getLogger("backfill")

FETCH_DELAY_SEC = 1.0  # be polite to aggregators


def _host(url: str) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""


def resolve_one(url: str) -> str | None:
    """Attempt to resolve an aggregator URL to its direct-company target.
    Returns the resolved URL if we moved off-aggregator, else None."""
    orig_host = _host(url)
    if not is_aggregator_host(orig_host):
        return None  # already resolved

    # Attempt 1: GET with redirect following
    try:
        resp = requests.get(
            url, timeout=FETCH_TIMEOUT_SEC, allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
    except requests.RequestException as e:
        log.debug("resolve_one: GET failed for %s: %s", url[:80], e)
        resp = None

    if resp is not None:
        final = getattr(resp, "url", url) or url
        if is_aggregator_host(_host(final)) is False and final != url:
            return final
        # Body redirect parse
        if resp.status_code == 200:
            body_redirect = _extract_body_redirect(resp.text or "", url)
            if body_redirect and not is_aggregator_host(_host(body_redirect)):
                return body_redirect

    # Attempt 2: HEAD fallback (some aggregators redirect on HEAD only)
    resolved = _head_final_url(url)
    if resolved and not is_aggregator_host(_host(resolved)):
        return resolved
    return None


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ap = argparse.ArgumentParser(description="Backfill aggregator apply_urls")
    ap.add_argument("--apply", action="store_true",
                    help="Write resolved URLs to Turso and push to WP. Without"
                    " this flag the script runs in dry-run mode.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Process at most N rows.")
    args = ap.parse_args()

    conn = db.connect()
    cur = conn.execute(
        "SELECT external_id, title, apply_url FROM jobs "
        "WHERE is_active = 1 AND apply_url IS NOT NULL AND apply_url <> ''"
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    candidates = [r for r in rows if is_aggregator_host(_host(r["apply_url"]))]
    if args.limit:
        candidates = candidates[:args.limit]
    log.info("Found %d aggregator URLs in %d active rows", len(candidates), len(rows))

    resolved_count = 0
    unresolved_count = 0
    wp_payloads: list[dict[str, Any]] = []

    for i, r in enumerate(candidates):
        if i > 0:
            time.sleep(FETCH_DELAY_SEC)
        ext_id = r["external_id"]
        old_url = r["apply_url"]
        new_url = resolve_one(old_url)
        if new_url is None:
            unresolved_count += 1
            log.info("[%d/%d] %s UNRESOLVED %s", i + 1, len(candidates),
                     ext_id, old_url[:100])
            continue
        resolved_count += 1
        log.info("[%d/%d] %s RESOLVED %s -> %s", i + 1, len(candidates),
                 ext_id, old_url[:60], new_url[:80])
        if args.apply:
            try:
                db.upgrade_apply_url(conn, ext_id, new_url)
            except Exception as e:  # noqa: BLE001
                log.warning("db.upgrade_apply_url failed for %s: %s", ext_id, e)
                continue
            if r.get("title"):
                wp_payloads.append({
                    "external_id": ext_id,
                    "title": r["title"],
                    "apply_url": new_url,
                })

    if args.apply and wp_payloads:
        log.info("Pushing %d updates to WordPress...", len(wp_payloads))
        result = wordpress.publish(
            wp_payloads,
            wp_url=os.environ.get("WP_URL", ""),
            username=os.environ.get("WP_USERNAME", ""),
            app_password=os.environ.get("WP_APP_PASSWORD", ""),
            conn=conn,
        )
        log.info("WordPress push result: %s", result)

    print()
    print("=" * 60)
    print(f"Resolved {resolved_count} of {len(candidates)} aggregator URLs")
    print(f"  unresolved: {unresolved_count}")
    print(f"  mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    if args.apply:
        print(f"  WP push: {len(wp_payloads)} payloads")
    else:
        print("  (no writes — rerun with --apply to persist)")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
