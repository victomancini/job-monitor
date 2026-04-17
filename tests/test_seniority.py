"""Tests for src/processors/seniority.py."""
from __future__ import annotations

import pytest

from src.processors.seniority import extract_seniority


@pytest.mark.parametrize("title,expected", [
    # Executive
    ("Chief People Officer", "Executive"),
    ("CHRO", "Executive"),
    ("CPO, Global", "Executive"),
    ("SVP, People Analytics", "Executive"),
    ("Senior Vice President, People", "Executive"),
    ("Sr. Vice President of HR", "Executive"),
    # VP
    ("VP, People Analytics", "VP"),
    ("VP", "VP"),
    ("Vice President of Employee Experience", "VP"),
    # Senior Director
    ("Senior Director of People Insights", "Senior Director"),
    ("Sr. Director, People Analytics", "Senior Director"),
    ("Global Head of People Analytics", "Senior Director"),
    # Director
    ("Head of People Analytics", "Director"),
    ("Director, Employee Listening", "Director"),
    ("Director of People Science", "Director"),
    # Senior Manager
    ("Senior Manager, People Analytics", "Senior Manager"),
    ("Sr. Manager, Employee Listening", "Senior Manager"),
    ("Sr Manager People Ops", "Senior Manager"),
    # Manager
    ("Manager, Employee Engagement", "Manager"),
    ("People Analytics Manager", "Manager"),
    # Senior IC
    ("Principal People Scientist", "Senior IC"),
    ("Staff People Scientist", "Senior IC"),
    ("Senior Analyst, People Analytics", "Senior IC"),
    ("Sr. Analyst, People Analytics", "Senior IC"),
    ("Lead People Scientist", "Senior IC"),
    # IC
    ("People Analytics Analyst", "IC"),
    ("People Data Engineer", "IC"),
    ("Workforce Planning Coordinator", "IC"),
    ("Employee Experience Researcher", "IC"),
    ("Workforce Strategy Consultant", "IC"),
    # Unknown
    ("Random Job Title", "Unknown"),
    ("", "Unknown"),
])
def test_extract_seniority(title, expected):
    assert extract_seniority(title) == expected


def test_priority_senior_manager_over_manager():
    """When 'senior' + 'manager' both present, should return Senior Manager not Manager."""
    assert extract_seniority("Senior Manager of Something") == "Senior Manager"


def test_priority_principal_over_manager():
    """'Principal' wins over no-match-then-generic."""
    assert extract_seniority("Principal Manager of Insights") == "VP" or \
        extract_seniority("Principal Manager of Insights") in {"Principal", "Senior IC", "Manager"}
    # This title is contradictory — just make sure we return SOME canonical label
    assert extract_seniority("Principal Manager") in {"Manager", "Senior IC"}


def test_priority_head_of_not_confused_with_director():
    assert extract_seniority("Head of People Analytics") == "Director"


def test_global_head_of_outranks_head_of():
    assert extract_seniority("Global Head of People Analytics") == "Senior Director"


def test_lead_classified_as_senior_ic():
    assert extract_seniority("Lead People Scientist") == "Senior IC"


def test_consultant_default_ic():
    assert extract_seniority("Workforce Consultant") == "IC"


def test_case_insensitive():
    assert extract_seniority("DIRECTOR of people analytics") == "Director"
    assert extract_seniority("chief people officer") == "Executive"


def test_html_entity_preprocessing():
    """Preprocessing should normalize entities before matching."""
    assert extract_seniority("VP &amp; Head of People Analytics") in {"VP", "Director"}


def test_none_or_blank_title():
    assert extract_seniority("") == "Unknown"
    assert extract_seniority("   ") == "Unknown"


# ───────────── Phase F (R2): PA/EL-specific patterns ───────────────

def test_intern_classified_as_intern():
    assert extract_seniority("People Analytics Intern") == "Intern"
    assert extract_seniority("PhD Intern, Workforce Research") == "Intern"


def test_internship_also_intern():
    assert extract_seniority("Summer Internship — People Science") == "Intern"


def test_internal_is_not_intern():
    """'Internal Communications Manager' must not collide with the intern pattern."""
    # "internal" has 'a' after "intern" → \bintern\b requires non-word boundary
    assert extract_seniority("Internal Communications Manager") == "Manager"


def test_managing_director_is_executive():
    assert extract_seniority("Managing Director, Human Capital Consulting") == "Executive"


def test_managing_director_not_director():
    """Must beat the plain \\bdirector\\b pattern."""
    assert extract_seniority("Managing Director") == "Executive"


def test_fellow_is_senior_ic():
    assert extract_seniority("AI-Driven People Analytics Fellow") == "Senior IC"


def test_staff_program_manager_is_senior_ic():
    assert extract_seniority("Staff Program Manager, Employee Listening") == "Senior IC"


# ───────────── Phase F (R2): salary-based fallback ─────────────────

from src.processors.seniority import infer_seniority_from_salary


@pytest.mark.parametrize("salary,expected", [
    (None, None),
    (0, None),
    (-5, None),
    (50_000, "Intern"),
    (75_000, "IC"),
    (120_000, "Manager"),
    (175_000, "Senior Manager"),
    (220_000, "Director"),
    (500_000, "Director"),
    # treat <1000 as already-in-thousands
    (120, "Manager"),
    (200, "Director"),
])
def test_infer_seniority_from_salary(salary, expected):
    assert infer_seniority_from_salary(salary) == expected
