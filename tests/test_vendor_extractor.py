"""Tests for Phase 5 (R3) vendor/tool extraction."""
from __future__ import annotations

import pytest

from src.processors.vendor_extractor import (
    VENDOR_PATTERNS,
    extract_vendors,
    vendors_to_str,
)


@pytest.mark.parametrize("text,expected", [
    ("We use Qualtrics and Medallia for employee surveys.", {"Qualtrics", "Medallia"}),
    ("Experience with Culture Amp, Perceptyx, Glint required.", {"Culture Amp", "Perceptyx", "Glint"}),
    ("Peakon platform experience preferred.", {"Workday Peakon"}),
    ("Workday Peakon or Gallup Q12.", {"Workday Peakon", "Gallup"}),
    ("Familiarity with Microsoft Viva Insights and Viva Pulse.", {"Microsoft Viva"}),
])
def test_extract_common_el_platforms(text, expected):
    assert set(extract_vendors(text)) >= expected


def test_workday_alone_not_workday_peakon():
    """Workday (HCM) ≠ Workday Peakon (employee voice). Both should be detectable
    separately."""
    assert set(extract_vendors("Experience with Workday HCM configurator")) == {"Workday"}
    assert set(extract_vendors("Workday Peakon platform")) == {"Workday Peakon"}


def test_r_programming_matches_in_programming_context():
    assert "R" in extract_vendors("Proficient in R, Python, SQL.")
    assert "R" in extract_vendors("Strong R programming skills.")


def test_r_does_not_false_positive_on_rd_or_hr():
    """'R&D', 'R & D', 'R and D', and 'HR' must not register as R."""
    assert "R" not in extract_vendors("Join our R&D department.")
    assert "R" not in extract_vendors("Work with R & D teams.")
    assert "R" not in extract_vendors("Partner with R and D to drive innovation.")
    # 'HR' has word char before R → \b fails → no match
    assert "R" not in extract_vendors("HR is our focus.")


def test_extract_sql_python_tableau():
    text = "Dashboards in Tableau using SQL backed Snowflake."
    vendors = set(extract_vendors(text))
    assert "SQL" in vendors
    assert "Tableau" in vendors
    assert "Snowflake" in vendors


def test_ona_matches_acronym_and_phrase():
    assert "Organizational Network Analysis" in extract_vendors("Experience with ONA projects.")
    assert "Organizational Network Analysis" in extract_vendors(
        "Run organizational network analysis quarterly."
    )


def test_extract_empty_or_none():
    assert extract_vendors("") == []
    assert extract_vendors(None) == []  # type: ignore[arg-type]


def test_extract_no_matches_returns_empty():
    assert extract_vendors("Generic HR role with no tool mentions.") == []


def test_vendors_to_str_comma_separated():
    assert vendors_to_str(["Qualtrics", "Medallia", "R"]) == "Qualtrics,Medallia,R"
    assert vendors_to_str([]) == ""


def test_result_order_is_deterministic():
    """Returned order follows VENDOR_PATTERNS key order, not input text order."""
    # Text mentions Python before Qualtrics, but Qualtrics appears earlier in the dict
    text = "Python and Qualtrics together."
    result = extract_vendors(text)
    # The order in VENDOR_PATTERNS puts Qualtrics before Python
    qt_idx = list(VENDOR_PATTERNS.keys()).index("Qualtrics")
    py_idx = list(VENDOR_PATTERNS.keys()).index("Python")
    assert result.index("Qualtrics") < result.index("Python")
    assert qt_idx < py_idx  # sanity
