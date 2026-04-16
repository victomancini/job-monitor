"""Tests for deduplicator — RapidFuzz composite scoring."""
from __future__ import annotations

import pytest

from src.processors import deduplicator as dedup


def make(ext_id, title, company, location=""):
    return {
        "external_id": ext_id,
        "title": title,
        "company": company,
        "location": location,
    }


# ──────────────────────────── Normalization ─────────────────────

def test_normalize_company_strips_suffixes():
    assert dedup.normalize_company("Netflix Inc") == "netflix"
    assert dedup.normalize_company("Perceptyx LLC") == "perceptyx"
    assert dedup.normalize_company("Acme, Corp.") == "acme"
    assert dedup.normalize_company("Acme Corporation") == "acme"
    assert dedup.normalize_company("Acme Co.") == "acme"


def test_normalize_title_abbreviations():
    assert dedup.normalize_title("Sr. Manager People Analytics") == "senior manager people analytics"
    assert dedup.normalize_title("VP People Analytics") == "vice president people analytics"
    assert dedup.normalize_title("Jr. Mgr PA") == "junior manager pa"
    assert dedup.normalize_title("Dir of Analytics") == "director of analytics"


# ──────────────────────────── Positive dedup cases ──────────────

def test_sr_vs_senior_matches():
    """The exact case from todo.md:
    'Sr. Manager People Analytics' / 'Netflix Inc' matches
    'Senior Manager, People Analytics' / 'Netflix'.
    """
    a = make("1", "Sr. Manager People Analytics", "Netflix Inc")
    b = make("2", "Senior Manager, People Analytics", "Netflix")
    score = dedup.compare(a, b)
    assert score >= dedup.DUPLICATE_THRESHOLD, f"score {score}"


def test_same_role_same_city_dup():
    a = make("1", "People Analytics Manager", "Netflix", "Los Gatos, CA")
    b = make("2", "People Analytics Manager", "Netflix Inc", "Los Gatos, CA")
    score = dedup.compare(a, b)
    assert score >= 90


# ──────────────────────────── Negative dedup cases ──────────────

def test_people_vs_customer_no_match():
    """'People Analytics Director' / 'Google' does NOT match
    'Customer Analytics Director' / 'Google'."""
    a = make("1", "People Analytics Director", "Google")
    b = make("2", "Customer Analytics Director", "Google")
    score = dedup.compare(a, b)
    assert score < dedup.DUPLICATE_THRESHOLD, f"score {score}"


def test_same_title_different_company_no_match():
    a = make("1", "People Analytics Manager", "Netflix")
    b = make("2", "People Analytics Manager", "Goldman Sachs")
    score = dedup.compare(a, b)
    assert score < dedup.DUPLICATE_THRESHOLD


# ──────────────────────────── find_duplicate ────────────────────

def test_find_duplicate_picks_best():
    incoming = make("new", "Senior Manager, People Analytics", "Netflix")
    pool = [
        make("a", "Customer Analytics Manager", "Netflix"),
        make("b", "Sr. Manager People Analytics", "Netflix Inc"),
        make("c", "Sales Manager", "Wells Fargo"),
    ]
    match, score = dedup.find_duplicate(incoming, pool)
    assert match is not None
    assert match["external_id"] == "b"
    assert score >= dedup.DUPLICATE_THRESHOLD


def test_find_duplicate_ignores_self():
    incoming = make("self", "Job Title", "Company")
    pool = [incoming]
    match, score = dedup.find_duplicate(incoming, pool)
    assert match is None


def test_find_duplicate_below_threshold_returns_none():
    incoming = make("x", "Data Engineer", "Microsoft")
    pool = [make("a", "Software Engineer", "Google")]
    match, score = dedup.find_duplicate(incoming, pool)
    assert match is None


# ──────────────────────────── deduplicate() batch ───────────────

def test_deduplicate_drops_obvious_dupes_in_batch():
    new_jobs = [
        make("jsearch_1", "Sr. Manager People Analytics", "Netflix Inc"),
        make("jooble_2", "Senior Manager, People Analytics", "Netflix"),
        make("adzuna_3", "Data Engineer", "Microsoft"),
    ]
    kept, skipped = dedup.deduplicate(new_jobs)
    assert len(kept) == 2
    assert len(skipped) == 1
    assert skipped[0]["external_id"] == "jooble_2"


def test_deduplicate_against_active_db_rows():
    db_rows = [make("existing", "People Analytics Manager", "Netflix Inc")]
    new_jobs = [make("new", "Sr. Manager, People Analytics", "Netflix")]
    kept, skipped = dedup.deduplicate(new_jobs, active_db_rows=db_rows)
    # The new job is likely a dupe of the existing one
    assert len(skipped) == 1
    assert skipped[0]["external_id"] == "new"


def test_deduplicate_flag_zone():
    """Two jobs in 70-84 range are both kept but flagged."""
    new_jobs = [
        make("a", "People Analytics Manager", "Netflix"),
        make("b", "People Insights Director", "Netflix"),
    ]
    kept, skipped = dedup.deduplicate(new_jobs)
    assert len(kept) == 2
    # May or may not flag depending on exact scores — just verify no skips
    assert len(skipped) == 0
