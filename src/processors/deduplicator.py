"""Composite fuzzy dedup via RapidFuzz. Compares new jobs against batch peers + active DB rows."""
from __future__ import annotations

import logging
import re
from typing import Any, Iterable

import rapidfuzz
from rapidfuzz import fuzz, utils

log = logging.getLogger(__name__)

_COMPANY_SUFFIXES = [" inc", " llc", " corp", " ltd", " co.", " co", " corporation", " company"]
_TITLE_ABBREV = [
    (r"\bsr\.?\b", "senior"),
    (r"\bjr\.?\b", "junior"),
    (r"\bmgr\b", "manager"),
    (r"\bdir\b", "director"),
    (r"\bvp\b", "vice president"),
]

DUPLICATE_THRESHOLD = 85
FLAG_THRESHOLD = 70


def normalize_company(raw: str) -> str:
    c = (raw or "").strip().lower()
    c = re.sub(r"[,.]", "", c)
    for suf in _COMPANY_SUFFIXES:
        if c.endswith(suf):
            c = c[: -len(suf)].strip()
    return c.strip()


def normalize_title(raw: str) -> str:
    t = (raw or "").strip().lower()
    for pat, repl in _TITLE_ABBREV:
        t = re.sub(pat, repl, t)
    # Collapse repeated whitespace and strip punctuation
    t = re.sub(r"[,.]", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def _city(location: str) -> str:
    if not location:
        return ""
    # "Los Gatos, CA" → "los gatos"; "Remote" → "remote"
    return location.split(",")[0].strip().lower()


def _composite_similarity(a: dict[str, Any], b: dict[str, Any]) -> float:
    company_sim = fuzz.WRatio(
        normalize_company(a.get("company", "")),
        normalize_company(b.get("company", "")),
        processor=utils.default_process,
    )
    title_sim = fuzz.token_sort_ratio(
        normalize_title(a.get("title", "")),
        normalize_title(b.get("title", "")),
        processor=utils.default_process,
    )
    city_a, city_b = _city(a.get("location", "")), _city(b.get("location", ""))
    if not city_a or not city_b:
        # Missing location data on either side → city factor is neutral; re-normalize to company+title only
        return 0.5 * company_sim + 0.5 * title_sim
    city_match = 100.0 if city_a == city_b else 0.0
    return 0.4 * company_sim + 0.4 * title_sim + 0.2 * city_match


def compare(a: dict[str, Any], b: dict[str, Any]) -> float:
    """Return composite similarity score 0-100."""
    return _composite_similarity(a, b)


def find_duplicate(
    job: dict[str, Any],
    existing: Iterable[dict[str, Any]],
) -> tuple[dict[str, Any] | None, float]:
    """Return (best_match_record, score) if score >= FLAG_THRESHOLD, else (None, 0)."""
    best: dict[str, Any] | None = None
    best_score = 0.0
    for other in existing:
        if other.get("external_id") == job.get("external_id"):
            continue
        s = _composite_similarity(job, other)
        if s > best_score:
            best = other
            best_score = s
    if best_score >= FLAG_THRESHOLD:
        return best, best_score
    return None, 0.0


def deduplicate(
    new_jobs: list[dict[str, Any]],
    active_db_rows: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (kept, skipped). `kept` includes 'flag' (70-84) and unique; `skipped` is >=85 dupes."""
    active_db_rows = active_db_rows or []
    kept: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    comparison_pool = list(active_db_rows)
    for job in new_jobs:
        match, score = find_duplicate(job, comparison_pool + kept)
        if match is not None and score >= DUPLICATE_THRESHOLD:
            skipped.append({**job, "_dedup_score": score, "_dedup_against": match.get("external_id")})
            continue
        if match is not None and score >= FLAG_THRESHOLD:
            job = {**job, "_dedup_flag": True, "_dedup_score": score}
        kept.append(job)
    return kept, skipped
