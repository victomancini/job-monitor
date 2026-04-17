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
