"""Tests for src/processors/category.py."""
from __future__ import annotations

import pytest

from src.processors.category import classify_category


@pytest.mark.parametrize("title,expected", [
    # Employee Listening
    ("Employee Listening Manager", "Employee Listening"),
    ("Voice of Employee Program Lead", "Employee Listening"),
    ("Continuous Listening Architect", "Employee Listening"),
    # HRIS & Systems
    ("HRIS Analyst", "HRIS & Systems"),
    ("HRIS & People Analytics Manager", "HRIS & Systems"),  # HRIS beats PA per spec
    # Research / I-O
    ("People Scientist, Future of Work", "Research / I-O"),
    ("Research Scientist, Workforce", "Research / I-O"),
    ("I-O Psychologist", "Research / I-O"),
    ("Industrial-Organizational Psychologist", "Research / I-O"),
    ("Behavioral Scientist, People", "Research / I-O"),
    # Data Engineering
    ("People Analytics Data Engineer", "Data Engineering"),
    ("Analytics Engineer, HR Data", "Data Engineering"),
    ("Automation Engineer, People Ops", "Data Engineering"),
    # Pay Equity
    ("Pay Equity Analyst", "Pay Equity"),
    ("Workplace Equity Lead", "Pay Equity"),
    # Workforce Planning
    ("Strategic Workforce Planning Manager", "Workforce Planning"),
    # Talent Intelligence
    ("Talent Intelligence Lead", "Talent Intelligence"),
    ("Workforce Intelligence Director", "Talent Intelligence"),
    ("Skills Intelligence Analyst", "Talent Intelligence"),
    # EX & Culture
    ("Employee Experience Advisor", "EX & Culture"),
    ("EX Advisor, Workforce", "EX & Culture"),
    ("Culture Analytics Lead", "EX & Culture"),
    ("Engagement Manager, People", "EX & Culture"),
    # People Analytics (broad catch-all)
    ("People Analytics Manager", "People Analytics"),
    ("HR Analytics Lead", "People Analytics"),
    ("Human Capital Analytics Director", "People Analytics"),
])
def test_classify_category(title, expected):
    assert classify_category(title) == expected


def test_consulting_fallback_by_company():
    # Title doesn't match any category regex → falls back to company check
    assert classify_category("Senior Manager", company="Deloitte") == "Consulting"
    assert classify_category("Senior Associate", company="McKinsey & Co") == "Consulting"
    assert classify_category("Director", company="PwC") == "Consulting"


def test_category_regex_wins_over_company():
    """If title matches a specific category, use it — don't downgrade to Consulting."""
    assert classify_category("Employee Listening Manager", company="Deloitte") == "Employee Listening"


def test_general_pa_fallback():
    """No regex match and not a known consulting company → General PA."""
    assert classify_category("Random Title", company="Random Corp") == "General PA"
    assert classify_category("", company="") == "General PA"


def test_description_contributes_to_match():
    """First 500 chars of description can trigger a category."""
    r = classify_category(
        title="Senior Manager",
        description="Lead our employee listening program and voice of the employee analytics.",
    )
    assert r == "Employee Listening"


def test_consulting_case_insensitive_and_trim():
    assert classify_category("Senior Manager", company="  DELOITTE  ") == "Consulting"
