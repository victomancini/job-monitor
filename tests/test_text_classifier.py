"""R11 Phase 2: text classifier that re-derives work_arrangement / is_remote
from description text. Ensures aggregator noise (is_remote=true on roles
described as "not remote") gets corrected via description inspection.
"""
from __future__ import annotations

import pytest

from src.processors import text_classifier as tc


# ─── classify_work_arrangement core ─────────────────────────

def test_fully_remote_high_confidence():
    arr, conf = tc.classify_work_arrangement(
        "This is a fully remote role, work from anywhere in the US."
    )
    assert arr == "remote"
    assert conf >= 0.85


def test_onsite_rejection_overrides_remote_mention():
    """'Not a remote role' must map to onsite even though the word 'remote'
    appears — this is the JSearch false-positive case the classifier exists
    to fix."""
    arr, _ = tc.classify_work_arrangement(
        "Note: this is not a remote role. Must be onsite at our NYC office."
    )
    assert arr == "onsite"


def test_hybrid_days_in_office_pattern():
    arr, conf = tc.classify_work_arrangement(
        "You'll work 3 days a week in the office and 2 days remote."
    )
    assert arr == "hybrid"
    assert conf >= 0.80


def test_hybrid_word_phrase():
    arr, _ = tc.classify_work_arrangement(
        "Our team operates on a hybrid schedule from the Austin HQ."
    )
    assert arr == "hybrid"


def test_onsite_only_explicit():
    arr, conf = tc.classify_work_arrangement(
        "This is an onsite only role at our Seattle office."
    )
    assert arr == "onsite"
    assert conf >= 0.85


def test_work_from_home_medium_confidence():
    arr, conf = tc.classify_work_arrangement(
        "We support work from home arrangements for this role."
    )
    assert arr == "remote"
    assert 0.60 <= conf <= 0.80


def test_no_signal_empty_description():
    arr, conf = tc.classify_work_arrangement("")
    assert arr is None
    assert conf == 0.0


def test_no_signal_irrelevant_description():
    arr, _ = tc.classify_work_arrangement(
        "We are looking for a passionate data scientist to join our team. "
        "You will work with cutting-edge analytics tools."
    )
    assert arr is None


def test_conflict_hybrid_phrase_outranks_bare_remote():
    """'Hybrid role' (0.85 strong match) beats a bare 'remote' mention
    (0.60 medium). Most-restrictive wins when confidence is close."""
    arr, _ = tc.classify_work_arrangement(
        "We offer hybrid roles and remote roles depending on the team."
    )
    assert arr == "hybrid"


def test_conflict_hybrid_days_pattern_outranks_remote_role_mention():
    arr, _ = tc.classify_work_arrangement(
        "This is a remote role partially — you'll spend 3 days per week in the office."
    )
    # "3 days per week in the office" (0.85) beats "remote role" (0.75)
    assert arr == "hybrid"


def test_long_description_only_scans_first_n_chars():
    """Description boilerplate past the scan limit must not trigger false
    positives — if onsite signal is only in the EEO footer, don't flag it."""
    prefix = "Great opportunity for a people analytics leader. " * 50  # ~2000+ chars
    desc = prefix + " This role requires onsite attendance."
    arr, _ = tc.classify_work_arrangement(desc)
    # Onsite signal was pushed past the scan limit → no signal
    assert arr is None


# ─── classify_batch provenance integration ────────────────

def test_classify_batch_adds_provenance_without_overwriting_flat():
    """text_classifier must NOT touch the flat is_remote field — only the
    provenance history. Final adjudication happens in Phase 3 consensus
    voting, which reads from _field_sources."""
    jobs = [{
        "description": "Fully remote position based in the US.",
        "is_remote": "onsite",  # source said onsite (wrongly)
        "work_arrangement": "Onsite",
    }]
    stats = tc.classify_batch(jobs)
    assert stats["classified"] == 1
    assert stats["remote"] == 1
    # Flat fields untouched
    assert jobs[0]["is_remote"] == "onsite"
    assert jobs[0]["work_arrangement"] == "Onsite"
    # But provenance carries the text_classifier's dissent
    fs = jobs[0]["_field_sources"]
    is_remote_obs = fs["is_remote"]
    assert any(
        o["source"] == "text_classifier" and o["value"] == "remote"
        for o in is_remote_obs
    )


def test_classify_batch_no_signal_increments_stats():
    jobs = [
        {"description": "Data scientist role working on analytics."},
        {"description": "Fully remote position."},
    ]
    stats = tc.classify_batch(jobs)
    assert stats["no_signal"] == 1
    assert stats["classified"] == 1
    assert stats["remote"] == 1


def test_classify_batch_empty_description_is_safe():
    jobs = [{"description": None}, {"description": ""}, {}]
    stats = tc.classify_batch(jobs)
    assert stats["no_signal"] == 3
    assert stats["classified"] == 0


def test_classify_batch_appends_to_existing_provenance():
    """When a source already recorded a provenance entry via build_job, the
    text classifier APPENDS its observation — it doesn't replace — so
    voting has both data points."""
    jobs = [{
        "description": "Hybrid role — 3 days per week in office.",
        "is_remote": "remote",
        "_field_sources": {
            "is_remote": [
                {"source": "jsearch", "value": "remote", "confidence": 0.55},
            ],
        },
    }]
    tc.classify_batch(jobs)
    obs = jobs[0]["_field_sources"]["is_remote"]
    assert len(obs) == 2
    assert obs[0]["source"] == "jsearch"
    assert obs[1]["source"] == "text_classifier"
    assert obs[1]["value"] == "hybrid"
