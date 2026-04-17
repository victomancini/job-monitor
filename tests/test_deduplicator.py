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
    """Two similar-looking jobs at DIFFERENT companies sit in the flag zone —
    both kept, possibly flagged. Must not be skipped.
    (Before Phase E the test used same-company; now same-company triggers the
    lowered SAME_COMPANY_THRESHOLD, so we use distinct companies here.)"""
    new_jobs = [
        make("a", "People Analytics Manager", "Netflix"),
        make("b", "People Insights Director", "Airbnb"),
    ]
    kept, skipped = dedup.deduplicate(new_jobs)
    assert len(kept) == 2
    assert len(skipped) == 0


# ──────────────────────────── Phase A: apply_url preference ─────

def test_apply_url_score_direct_https():
    assert dedup._apply_url_score("https://careers.netflix.com/job/1") == 3


def test_apply_url_score_direct_http():
    assert dedup._apply_url_score("http://careers.netflix.com/job/1") == 2


def test_apply_url_score_aggregator():
    assert dedup._apply_url_score("https://jooble.org/desc/1") == 1
    assert dedup._apply_url_score("https://www.indeed.com/viewjob?jk=abc") == 1
    assert dedup._apply_url_score("https://www.linkedin.com/jobs/view/123") == 1


def test_apply_url_score_empty():
    assert dedup._apply_url_score("") == 0
    assert dedup._apply_url_score(None) == 0


def test_better_apply_url_prefers_direct_over_aggregator():
    a = {"external_id": "a", "apply_url": "https://jooble.org/desc/1"}
    b = {"external_id": "b", "apply_url": "https://careers.netflix.com/job/1"}
    assert dedup._better_apply_url(a, b) is b
    assert dedup._better_apply_url(b, a) is b


def test_better_apply_url_prefers_https_over_http():
    a = {"external_id": "a", "apply_url": "https://careers.netflix.com/job/1"}
    b = {"external_id": "b", "apply_url": "http://careers.netflix.com/job/1"}
    assert dedup._better_apply_url(a, b) is a


def test_deduplicate_swaps_batch_peer_for_better_apply_url():
    """Two batch peers are dupes: the newer one with a better apply_url displaces
    the older one in `kept`."""
    older = {
        "external_id": "jooble_1",
        "title": "Senior Manager, People Analytics",
        "company": "Netflix",
        "location": "Los Gatos, CA",
        "apply_url": "https://jooble.org/desc/1",
    }
    newer = {
        "external_id": "jsearch_2",
        "title": "Sr. Manager People Analytics",
        "company": "Netflix Inc",
        "location": "Los Gatos, CA",
        "apply_url": "https://careers.netflix.com/job/2",
    }
    kept, skipped = dedup.deduplicate([older, newer])
    assert len(kept) == 1
    assert kept[0]["external_id"] == "jsearch_2"  # the direct-URL one wins
    assert len(skipped) == 1
    assert skipped[0]["external_id"] == "jooble_1"


# ──────────────────────────── Phase E: same-company threshold + location merge ─

def test_merge_locations_two_cities_joined():
    assert dedup._merge_locations("New York, NY", "Chicago, IL") == "New York, NY; Chicago, IL"


def test_merge_locations_three_cities_still_joined():
    merged = dedup._merge_locations("New York, NY; Chicago, IL", "Atlanta, GA")
    assert merged == "New York, NY; Chicago, IL; Atlanta, GA"


def test_merge_locations_four_cities_collapses():
    merged = dedup._merge_locations("New York, NY; Chicago, IL; Atlanta, GA", "Dallas, TX")
    assert merged == "Multiple Locations (4)"


def test_merge_locations_already_collapsed_increments():
    merged = dedup._merge_locations("Multiple Locations (5)", "Austin, TX")
    assert merged == "Multiple Locations (6)"


def test_merge_locations_duplicate_city_noop():
    assert dedup._merge_locations("New York, NY", "New York, NY") == "New York, NY"


def test_merge_locations_empty_inputs_noop():
    assert dedup._merge_locations("", "New York, NY") == "New York, NY"
    assert dedup._merge_locations("New York, NY", "") == "New York, NY"


def test_effective_threshold_same_company():
    # "Deloitte Inc" normalizes to "deloitte" (Inc is a stripped suffix) → 100.
    a = {"company": "Deloitte Inc"}
    b = {"company": "Deloitte"}
    t = dedup._effective_threshold(a, b)
    assert t == dedup.SAME_COMPANY_THRESHOLD  # 75


def test_effective_threshold_different_company():
    a = {"company": "Netflix"}
    b = {"company": "Goldman Sachs"}
    assert dedup._effective_threshold(a, b) == dedup.DUPLICATE_THRESHOLD  # 85


def test_deduplicate_deloitte_4x_collapses_to_one():
    """Phase E headline test: same company + same title in 4 cities → one row with
    'Multiple Locations (4)'."""
    new_jobs = [
        make("d1", "Employee Listening Specialist", "Deloitte", "New York, NY"),
        make("d2", "Employee Listening Specialist", "Deloitte", "Chicago, IL"),
        make("d3", "Employee Listening Specialist", "Deloitte", "Atlanta, GA"),
        make("d4", "Employee Listening Specialist", "Deloitte", "Dallas, TX"),
    ]
    kept, skipped = dedup.deduplicate(new_jobs)
    assert len(kept) == 1
    assert len(skipped) == 3
    assert kept[0]["location"] == "Multiple Locations (4)"


def test_deduplicate_same_role_two_cities_joined():
    new_jobs = [
        make("a", "Employee Experience Advisor", "Forsta", "Seattle, WA"),
        make("b", "Employee Experience Advisor", "Forsta", "Remote"),
    ]
    kept, skipped = dedup.deduplicate(new_jobs)
    assert len(kept) == 1
    assert len(skipped) == 1
    assert "Seattle, WA" in kept[0]["location"]
    assert "Remote" in kept[0]["location"]


def test_deduplicate_keeps_first_when_apply_url_equal_quality():
    """If both URLs score the same, the first-seen (already-kept) one wins."""
    a = {
        "external_id": "a",
        "title": "Sr. Manager People Analytics",
        "company": "Netflix",
        "location": "Los Gatos, CA",
        "apply_url": "https://jooble.org/desc/a",
    }
    b = {
        "external_id": "b",
        "title": "Senior Manager, People Analytics",
        "company": "Netflix Inc",
        "location": "Los Gatos, CA",
        "apply_url": "https://adzuna.com/redir/b",
    }
    kept, skipped = dedup.deduplicate([a, b])
    assert len(kept) == 1
    assert kept[0]["external_id"] == "a"
    assert skipped[0]["external_id"] == "b"
