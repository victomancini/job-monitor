"""Tests for keyword_filter. Spec: zero false positives; word-boundary matching;
conflict → LLM; google_alerts always → LLM; company boost for Tier 1/2 vendors."""
from __future__ import annotations

import pytest

from src.processors import keyword_filter as kf


def job(title, company="NoCo", description="", source_name="jsearch"):
    return {
        "title": title,
        "company": company,
        "description": description,
        "source_name": source_name,
    }


# ───────────── Good titles: 20 must score >= 50 (auto_include) ─────────────

GOOD_TITLES = [
    "Employee Listening Manager",
    "People Analytics Director",
    "Senior Manager, People Analytics",
    "Head of Employee Listening",
    "Voice of Employee Program Manager",
    "Principal People Scientist",
    "People Scientist",
    "People Analytics Lead",
    "VP People Analytics",
    "Senior Director of People Analytics",
    "Employee Listening Strategy Lead",
    "Continuous Listening Manager",
    "Workforce Sensing Lead",
    "Human Capital Analytics Director",
    "Talent Analytics Manager",
    "Employee Listening Principal",
    "Employee Listening Director",
    "People Analytics Manager",
    "People Analytics Senior",
    "Organizational Network Analysis Lead",
]


@pytest.mark.parametrize("title", GOOD_TITLES)
def test_good_titles_auto_include(title):
    r = kf.classify(job(title))
    assert r["score"] >= 50, f"{title}: score {r['score']}"
    assert r["decision"] == "auto_include"


# ───────────── Bad titles: 20 must score < 10 (reject or low_score) ─────────

BAD_TITLES = [
    "Customer Experience Analyst",
    "Marketing Analytics Manager",
    "Social Media Manager",
    "SEO Analyst",
    "Call Center Manager",
    "Patient Experience Manager",
    "Customer Satisfaction Director",
    "Brand Monitoring Specialist",
    "Voice of Customer Manager",
    "NPS Program Manager",
    "Social Listening Manager",
    "Speech Pathologist",
    "Audiologist",
    "Media Monitoring Analyst",
    "Contact Center Lead",
    "Digital Marketing Specialist",
    "Performance Marketing Manager",
    "Demand Generation Director",
    "Customer Service Representative",
    "Threat Intelligence Analyst",
]


@pytest.mark.parametrize("title", BAD_TITLES)
def test_bad_titles_score_low(title):
    r = kf.classify(job(title))
    assert r["score"] < 10, f"{title}: score {r['score']}"
    assert r["decision"] in ("auto_reject", "low_score")


# ───────────── THE #1 false positive: "active listening" ────────────────────

def test_active_listening_in_title_rejected():
    r = kf.classify(job("Call Center Agent with Active Listening Skills"))
    assert r["decision"] == "auto_reject"
    assert r["score"] == -100


def test_active_listening_in_description_rejected_when_no_positives():
    r = kf.classify(job("Customer Service Rep",
                        description="Requires active listening skills and empathy"))
    assert r["decision"] == "auto_reject"


# ───────────── Conflict: positive + negative → LLM, never auto-decide ───────

def test_positive_plus_negative_routes_to_llm():
    """Listening bar + people analytics mention → conflict → LLM, not auto-include."""
    r = kf.classify(job(
        "People Analytics Director at our new Listening Bar",
        description="Drive people analytics strategy. Also help with active listening events.",
    ))
    assert r["decision"] == "llm_review", f"score={r['score']}, matched={r['matched']}"


def test_word_boundary_avoids_substring_false_positives():
    """'I-O psychologist' must not fire on 'psychology' alone? Actually those are separate terms.
    Test: 'organizational psychologist' does NOT substring-match 'organisational psychologist' (different spelling)."""
    r = kf.classify(job("Software Engineer at psychologyapp"))
    assert r["score"] < 10


def test_hyphenated_term_matches_hyphen():
    """'industrial-organizational' in title should match."""
    r = kf.classify(job("Industrial-Organizational Psychologist"))
    assert r["score"] >= 10  # at minimum tier2


# ───────────── Company boost (Tier 1/2 vendor adds +15) ─────────────────────

def test_company_boost_tier1_vendor():
    """Generic-titled role at Perceptyx gets +15 to cross into llm_review."""
    r = kf.classify(job("Senior Manager", company="Perceptyx"))
    assert r["score"] >= 15
    # Should move to at least llm_review zone
    assert r["decision"] in ("llm_review", "auto_include")


def test_company_boost_handles_corp_suffix():
    """'Perceptyx Inc' still matches Perceptyx entry."""
    r = kf.classify(job("Senior Manager", company="Perceptyx Inc"))
    assert r["score"] >= 15


def test_no_boost_for_unknown_company():
    r = kf.classify(job("Senior Manager", company="Random Startup LLC"))
    assert r["score"] < 15


# ───────────── google_alerts: always LLM ────────────────────────────────────

def test_google_alerts_high_score_still_llm():
    r = kf.classify(job("People Analytics Manager", company="Netflix", source_name="google_alerts"))
    assert r["decision"] == "llm_review"


def test_google_alerts_low_score_still_llm():
    r = kf.classify(job("Software Engineer", company="X", source_name="google_alerts"))
    # Low score from google_alerts goes to LLM, not silent reject
    assert r["decision"] == "llm_review"


# ───────────── Score cap and fit_score alias ────────────────────────────────

def test_score_capped_at_100():
    """Many matches shouldn't exceed 100."""
    r = kf.classify(job(
        "Employee Listening Manager People Analytics Director Voice of Employee Workforce Sensing Lead",
        description="employee listening people analytics voice of employee workforce sensing continuous listening",
    ))
    assert r["score"] <= 100


def test_fit_score_mirrors_keyword_score():
    j = job("People Analytics Manager")
    kf.classify(j)
    assert j["fit_score"] == j["keyword_score"]


def test_description_snippet_populated():
    desc = "x" * 500
    j = job("People Analytics Manager", description=desc)
    kf.classify(j)
    assert len(j["description_snippet"]) == 300


def test_job_mutation_adds_keyword_fields():
    j = job("People Analytics Manager")
    kf.classify(j)
    assert "keyword_score" in j
    assert "keywords_matched" in j
    assert "fit_score" in j


# ───────────── HRIS admin in reducer list (not auto-reject, just penalized) ─

def test_hris_admin_penalty():
    """HRIS admin is -15 reducer — but since there are no positives, score is 0 after reducer.
    Decision should be low_score (no negative_auto_reject matched)."""
    r = kf.classify(job("HRIS Analyst"))
    assert r["decision"] == "low_score"
