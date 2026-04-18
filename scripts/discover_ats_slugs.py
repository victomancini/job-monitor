#!/usr/bin/env python3
"""Validate candidate ATS slugs (Greenhouse / Lever / Ashby) for known companies
and emit `config/ats_companies.json` with only the slugs that actually return a
job board.

Run once after adding new companies, then periodically (e.g. monthly) to catch
employers who switch ATS vendors.

Usage:
    python scripts/discover_ats_slugs.py                # validate all three
    python scripts/discover_ats_slugs.py --ats ashby    # validate one
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Make the project root importable when invoked directly.
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import requests  # noqa: E402

from src.sources.greenhouse import (  # noqa: E402
    BASE_URL as GH_URL,
    DEFAULT_COMPANIES as GH_COMPANIES,
)
from src.sources.lever import (  # noqa: E402
    BASE_URL as LEVER_URL,
    DEFAULT_COMPANIES as LEVER_COMPANIES,
)
from src.sources.ashby import (  # noqa: E402
    BASE_URL as ASHBY_URL,
    DEFAULT_COMPANIES as ASHBY_COMPANIES,
)

OUTPUT = _ROOT / "config" / "ats_companies.json"
DELAY_SEC = 1.0
HEADERS = {"User-Agent": "job-monitor discovery/1.0"}


def _check_one(url: str, params: dict | None = None) -> bool:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
    except requests.RequestException:
        return False
    return r.status_code == 200


def _scan(ats: str, companies: dict[str, str]) -> dict[str, str]:
    validated: dict[str, str] = {}
    total = len(companies)
    for i, (slug, name) in enumerate(companies.items(), 1):
        if ats == "greenhouse":
            ok = _check_one(GH_URL.format(slug=slug), {"content": "true"})
        elif ats == "lever":
            ok = _check_one(LEVER_URL.format(slug=slug), {"mode": "json"})
        elif ats == "ashby":
            ok = _check_one(ASHBY_URL.format(slug=slug), {"includeCompensation": "true"})
        else:
            raise ValueError(f"unknown ATS: {ats}")
        marker = "OK" if ok else "--"
        print(f"  [{i}/{total}] {marker} {slug} ({name})", flush=True)
        if ok:
            validated[slug] = name
        if i < total:
            time.sleep(DELAY_SEC)
    return validated


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ats", choices=["greenhouse", "lever", "ashby"], default=None,
                        help="Only scan one ATS; default: scan all three")
    args = parser.parse_args()

    existing = {}
    if OUTPUT.exists():
        try:
            existing = json.loads(OUTPUT.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}

    targets: list[tuple[str, dict[str, str]]] = []
    if args.ats is None or args.ats == "greenhouse":
        targets.append(("greenhouse", GH_COMPANIES))
    if args.ats is None or args.ats == "lever":
        targets.append(("lever", LEVER_COMPANIES))
    if args.ats is None or args.ats == "ashby":
        targets.append(("ashby", ASHBY_COMPANIES))

    for ats, companies in targets:
        print(f"\n=== Scanning {ats} ({len(companies)} candidates) ===")
        validated = _scan(ats, companies)
        existing[ats] = validated
        print(f"  → {len(validated)}/{len(companies)} valid")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"\nWrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
