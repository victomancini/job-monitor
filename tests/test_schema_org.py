"""R11 Phase 4: schema.org JobPosting extraction. Exercises the pure
parsing function against representative markup shapes (direct dict, @graph
wrapper, list wrapper, nested Place/PostalAddress, MonetaryAmount salary).
"""
from __future__ import annotations

import json

import pytest

from src.processors import schema_org as so


# Helpers ──────────────────────────────────────────────────

def _wrap(jsonld: dict | list) -> str:
    """Wrap a JSON-LD object in the <script> tag an ATS would serve."""
    return (
        '<html><head>'
        '<script type="application/ld+json">'
        + json.dumps(jsonld)
        + '</script></head></html>'
    )


def _bare_job_posting(**overrides) -> dict:
    base = {
        "@context": "https://schema.org/",
        "@type": "JobPosting",
        "title": "People Analytics Manager",
        "hiringOrganization": {"@type": "Organization", "name": "Netflix"},
    }
    base.update(overrides)
    return base


# Direct JobPosting ──────────────────────────────────────

def test_extracts_nothing_from_empty_html():
    assert so.extract_job_posting("") == {}


def test_extracts_nothing_when_no_ld_json():
    assert so.extract_job_posting("<html><body>no structured data</body></html>") == {}


def test_extracts_remote_from_job_location_type():
    jp = _bare_job_posting(jobLocationType="TELECOMMUTE")
    out = so.extract_job_posting(_wrap(jp))
    assert out["is_remote"] == "remote"
    assert out["work_arrangement"] == "remote"


def test_extracts_hybrid_from_job_location_type():
    jp = _bare_job_posting(jobLocationType="HYBRID")
    out = so.extract_job_posting(_wrap(jp))
    assert out["is_remote"] == "hybrid"
    assert out["work_arrangement"] == "hybrid"


def test_extracts_location_from_nested_address():
    jp = _bare_job_posting(jobLocation={
        "@type": "Place",
        "address": {
            "@type": "PostalAddress",
            "addressLocality": "Los Gatos",
            "addressRegion": "CA",
            "addressCountry": "US",
        },
    })
    out = so.extract_job_posting(_wrap(jp))
    assert out["location"] == "Los Gatos, CA"
    assert out["location_country"] == "US"


def test_extracts_country_from_named_object():
    """Some sites emit `addressCountry` as {"@type":"Country","name":"United States"}
    instead of a 2-letter code. Normalize to US."""
    jp = _bare_job_posting(jobLocation={
        "address": {
            "addressLocality": "Austin",
            "addressRegion": "TX",
            "addressCountry": {"@type": "Country", "name": "United States"},
        },
    })
    out = so.extract_job_posting(_wrap(jp))
    assert out["location_country"] == "US"


def test_extracts_salary_from_monetary_amount():
    jp = _bare_job_posting(baseSalary={
        "@type": "MonetaryAmount",
        "currency": "USD",
        "value": {
            "@type": "QuantitativeValue",
            "minValue": 150000,
            "maxValue": 200000,
            "unitText": "YEAR",
        },
    })
    out = so.extract_job_posting(_wrap(jp))
    assert out["salary_min"] == 150000.0
    assert out["salary_max"] == 200000.0


def test_extracts_salary_single_value_fills_both_min_max():
    jp = _bare_job_posting(baseSalary={
        "value": {"value": 175000},
    })
    out = so.extract_job_posting(_wrap(jp))
    assert out["salary_min"] == 175000.0
    assert out["salary_max"] == 175000.0


def test_extracts_date_posted_trims_to_iso_date():
    jp = _bare_job_posting(datePosted="2026-04-15T12:34:56Z")
    out = so.extract_job_posting(_wrap(jp))
    assert out["date_posted"] == "2026-04-15"


def test_extracts_date_posted_bare_date():
    jp = _bare_job_posting(datePosted="2026-04-15")
    out = so.extract_job_posting(_wrap(jp))
    assert out["date_posted"] == "2026-04-15"


# Wrapper shapes ─────────────────────────────────────────

def test_finds_job_posting_inside_graph_wrapper():
    """Some sites wrap multiple ld+json nodes in an @graph array."""
    wrapped = {
        "@context": "https://schema.org",
        "@graph": [
            {"@type": "Organization", "name": "Netflix"},
            _bare_job_posting(jobLocationType="TELECOMMUTE"),
        ],
    }
    out = so.extract_job_posting(_wrap(wrapped))
    assert out["is_remote"] == "remote"


def test_finds_job_posting_inside_bare_list():
    nodes = [
        {"@type": "WebSite", "name": "Careers"},
        _bare_job_posting(jobLocationType="TELECOMMUTE"),
    ]
    out = so.extract_job_posting(_wrap(nodes))
    assert out["is_remote"] == "remote"


def test_finds_job_posting_when_type_is_list():
    """@type can be a list; JobPosting must still be detected inside it."""
    jp = _bare_job_posting(jobLocationType="TELECOMMUTE")
    jp["@type"] = ["JobPosting", "Thing"]
    out = so.extract_job_posting(_wrap(jp))
    assert out["is_remote"] == "remote"


def test_ignores_unrelated_ld_json_blocks():
    """A BreadcrumbList and Organization block on the same page must not
    match — only JobPosting counts."""
    html = (
        '<script type="application/ld+json">'
        + json.dumps({"@type": "BreadcrumbList", "itemListElement": []})
        + '</script>'
        '<script type="application/ld+json">'
        + json.dumps(_bare_job_posting(jobLocationType="HYBRID"))
        + '</script>'
    )
    out = so.extract_job_posting(html)
    assert out["is_remote"] == "hybrid"


# Tolerance ──────────────────────────────────────────────

def test_malformed_json_block_does_not_crash():
    """A bad JSON-LD block must not mask a good one elsewhere on the page."""
    html = (
        '<script type="application/ld+json">NOT_JSON}{</script>'
        '<script type="application/ld+json">'
        + json.dumps(_bare_job_posting(jobLocationType="TELECOMMUTE"))
        + '</script>'
    )
    out = so.extract_job_posting(html)
    assert out["is_remote"] == "remote"


def test_missing_fields_return_empty():
    """A minimal JobPosting (title only) returns empty — no fields to emit
    as observations, so no provenance noise."""
    jp = _bare_job_posting()  # no location, salary, date, remote
    out = so.extract_job_posting(_wrap(jp))
    assert out == {}


# apply_to_job integration ──────────────────────────────

def test_apply_to_job_emits_provenance():
    jp = _bare_job_posting(
        jobLocationType="TELECOMMUTE",
        datePosted="2026-04-15",
        baseSalary={"value": {"minValue": 150000, "maxValue": 200000}},
    )
    job = {"title": "X"}
    n = so.apply_to_job(job, _wrap(jp))
    # is_remote + work_arrangement + salary_min + salary_max + date_posted = 5
    assert n == 5
    fs = job["_field_sources"]
    assert fs["is_remote"][0]["source"] == "schema_org"
    assert fs["is_remote"][0]["confidence"] == 0.85
    assert fs["salary_min"][0]["value"] == 150000.0
    assert fs["date_posted"][0]["value"] == "2026-04-15"


def test_apply_to_job_no_schema_returns_zero():
    job = {"title": "X"}
    n = so.apply_to_job(job, "<html>no structured data</html>")
    assert n == 0
    assert "_field_sources" not in job


def test_apply_to_job_does_not_touch_flat_values():
    """schema_org must not overwrite flat is_remote — that's consensus
    voting's job. This module only adds a new observation."""
    jp = _bare_job_posting(jobLocationType="TELECOMMUTE")
    job = {"is_remote": "onsite"}  # aggregator said onsite
    so.apply_to_job(job, _wrap(jp))
    assert job["is_remote"] == "onsite"  # untouched
    assert job["_field_sources"]["is_remote"][0]["value"] == "remote"
