"""Composite fuzzy dedup via RapidFuzz. Compares new jobs against batch peers + active DB rows."""
from __future__ import annotations

import logging
import re
from typing import Any, Iterable
from urllib.parse import urlparse

import rapidfuzz
from rapidfuzz import fuzz, utils

from src.shared import AGGREGATOR_HOSTS, is_aggregator_host


def _apply_url_score(url: str) -> int:
    """Score an apply_url on quality: higher is better.
    3 = direct company URL over HTTPS
    2 = direct company URL over HTTP (rare)
    1 = aggregator redirect
    0 = empty / unparseable
    """
    if not url:
        return 0
    try:
        parsed = urlparse(url)
    except Exception:  # noqa: BLE001
        return 0
    host = (parsed.netloc or "").lower()
    if not host:
        return 0
    if is_aggregator_host(host):
        return 1
    # Direct company URL
    return 3 if parsed.scheme == "https" else 2


def _better_apply_url(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Return whichever job dict has the higher-quality apply_url (ties → a)."""
    return a if _apply_url_score(a.get("apply_url", "")) >= _apply_url_score(b.get("apply_url", "")) else b


def _company_similarity(a: dict[str, Any], b: dict[str, Any]) -> float:
    return fuzz.WRatio(
        normalize_company(a.get("company", "")),
        normalize_company(b.get("company", "")),
        processor=utils.default_process,
    )


def _effective_threshold(a: dict[str, Any], b: dict[str, Any]) -> float:
    """Phase E: use the lower 75 threshold when companies match near-exactly."""
    if _company_similarity(a, b) >= SAME_COMPANY_MIN_SIM:
        return SAME_COMPANY_THRESHOLD
    return DUPLICATE_THRESHOLD


def _merge_locations(primary: str, new: str) -> str:
    """Merge a new location into a primary (possibly already-merged) location string.
    Rules:
      - empty/blank inputs are no-ops
      - already in the list → no change
      - "Multiple Locations (N)" form: can't recover individual cities, so increment N
      - 1–MERGE_LOCATIONS_DISPLAY_LIMIT cities → join with "; "
      - >MERGE_LOCATIONS_DISPLAY_LIMIT cities → collapse to "Multiple Locations (N)"
    """
    new = (new or "").strip()
    primary = (primary or "").strip()
    if not new:
        return primary
    if not primary:
        return new
    m = re.match(r"Multiple Locations \((\d+)\)$", primary)
    if m:
        return f"Multiple Locations ({int(m.group(1)) + 1})"
    existing = [s.strip() for s in primary.split(";") if s.strip()]
    if new in existing:
        return "; ".join(existing)
    existing.append(new)
    if len(existing) > MERGE_LOCATIONS_DISPLAY_LIMIT:
        return f"Multiple Locations ({len(existing)})"
    return "; ".join(existing)

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
# Phase E (R2): lower the dupe threshold when the company is a near-exact match
# so we catch same-role-different-city repeats (Deloitte × 4 cities, etc.).
SAME_COMPANY_THRESHOLD = 75
SAME_COMPANY_MIN_SIM = 95
# When three or more unique locations merge into one row, swap to a "Multiple
# Locations (N)" label rather than a long semicolon list.
MERGE_LOCATIONS_DISPLAY_LIMIT = 3


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
    """Return (kept, skipped). `kept` includes 'flag' (70-84) and unique; `skipped` is >=85 dupes.

    Phase A: when a duplicate is against a batch peer (already in `kept`), we keep
    whichever job has the better-quality apply_url (direct company URL > aggregator redirect).
    Duplicates against DB rows still win for the DB row — apply_url upgrades on existing records
    happen via upsert when the new record is not flagged as a dupe."""
    active_db_rows = active_db_rows or []
    kept: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    comparison_pool = list(active_db_rows)
    for job in new_jobs:
        match, score = find_duplicate(job, comparison_pool + kept)
        threshold = _effective_threshold(job, match) if match is not None else DUPLICATE_THRESHOLD
        if match is not None and score >= threshold:
            # Is this match a batch peer (already in kept)?
            match_idx = next(
                (i for i, k in enumerate(kept)
                 if k.get("external_id") == match.get("external_id")),
                None,
            )
            if match_idx is not None:
                # Phase E: merge locations across the duplicate group
                merged_loc = _merge_locations(
                    kept[match_idx].get("location", ""),
                    job.get("location", ""),
                )
                if _better_apply_url(kept[match_idx], job) is job:
                    # Newer job has a better apply_url → it becomes the primary,
                    # inheriting the merged location so earlier peers aren't lost.
                    displaced = kept[match_idx]
                    skipped.append({**displaced, "_dedup_score": score,
                                    "_dedup_against": job.get("external_id")})
                    kept[match_idx] = {**job, "location": merged_loc}
                else:
                    # Keep the existing primary, but update its location.
                    kept[match_idx] = {**kept[match_idx], "location": merged_loc}
                    skipped.append({**job, "_dedup_score": score,
                                    "_dedup_against": match.get("external_id")})
            else:
                # Duplicate against a DB row — we don't touch DB locations from
                # here. But if the incoming job's apply_url is materially
                # better (direct company URL vs. DB row's aggregator URL), we
                # stash the upgrade hint on the skipped record so the caller
                # can upsert the improved apply_url onto the existing row
                # instead of dropping this signal.
                skipped_entry = {**job, "_dedup_score": score,
                                 "_dedup_against": match.get("external_id")}
                incoming_score = _apply_url_score(job.get("apply_url", ""))
                db_score = _apply_url_score(match.get("apply_url", ""))
                if incoming_score > db_score:
                    skipped_entry["_apply_url_upgrade"] = {
                        "external_id": match.get("external_id"),
                        "apply_url": job.get("apply_url", ""),
                    }
                skipped.append(skipped_entry)
            continue
        if match is not None and score >= FLAG_THRESHOLD:
            job = {**job, "_dedup_flag": True, "_dedup_score": score}
        kept.append(job)
    return kept, skipped
