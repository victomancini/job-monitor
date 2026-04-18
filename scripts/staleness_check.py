#!/usr/bin/env python3
"""Standalone Phase 6 (R3) lifecycle sweep.

Runs the two-step archiver outside of a full pipeline run — useful as a weekly
GitHub Actions cron so likely_closed / archived transitions happen even when no
new jobs come in.

Usage:
    python scripts/staleness_check.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from src import db  # noqa: E402
from src.publishers import archiver  # noqa: E402


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    conn = db.connect()
    db.migrate(conn)
    result = archiver.archive_stale(conn)
    logging.info("staleness_check: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
