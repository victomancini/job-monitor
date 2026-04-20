"""R11 Phase 3: consensus voting across multiple observations of the same
field. Tests the weighted-majority logic, peer-merge provenance handoff,
and the override threshold that prevents flaky low-confidence flips.
"""
from __future__ import annotations

import pytest

from src.processors import deduplicator as dd


# ─── compute_consensus ──────────────────────────────────────

def test_consensus_single_observation():
    obs = [{"source": "jsearch", "value": "remote", "confidence": 0.55}]
    winner, conf, sources = dd.compute_consensus(obs)
    assert winner == "remote"
    assert conf == 1.0
    assert sources == ["jsearch"]


def test_consensus_majority_wins():
    """Two high-confidence sources beat one low-confidence source."""
    obs = [
        {"source": "jsearch", "value": "remote", "confidence": 0.55},
        {"source": "greenhouse", "value": "hybrid", "confidence": 0.90},
        {"source": "text_classifier", "value": "hybrid", "confidence": 0.85},
    ]
    winner, conf, sources = dd.compute_consensus(obs)
    assert winner == "hybrid"
    # hybrid weight = 0.90 + 0.85 = 1.75; total = 2.30; conf = 1.75 / 2.30 ≈ 0.76
    assert 0.70 < conf < 0.80
    assert set(sources) == {"greenhouse", "text_classifier"}


def test_consensus_weighted_not_simple_count():
    """A single canonical ATS observation should beat two low-confidence
    aggregator observations when its confidence is high enough. This is the
    intended behavior — Greenhouse at 0.90 should outvote Jooble+Adzuna
    at 0.50 each (sum 1.00 vs 0.90) — actually here 2×0.50=1.00 > 0.90, so
    majority can still win. Test asserts the MATH, not a specific outcome."""
    obs = [
        {"source": "jooble", "value": "remote", "confidence": 0.50},
        {"source": "adzuna", "value": "remote", "confidence": 0.50},
        {"source": "greenhouse", "value": "hybrid", "confidence": 0.90},
    ]
    winner, conf, _ = dd.compute_consensus(obs)
    # remote = 1.00, hybrid = 0.90 → remote wins
    assert winner == "remote"
    assert 0.50 < conf < 0.60


def test_consensus_restrictiveness_tiebreaker():
    """On a dead-even vote (equal total weights), the most-restrictive
    arrangement wins — candidate would rather overestimate commute than
    falsely assume fully remote."""
    obs = [
        {"source": "a", "value": "remote", "confidence": 0.50},
        {"source": "b", "value": "hybrid", "confidence": 0.50},
    ]
    winner, _, _ = dd.compute_consensus(obs)
    assert winner == "hybrid"  # hybrid more restrictive than remote


def test_consensus_restrictiveness_onsite_over_hybrid():
    obs = [
        {"source": "a", "value": "hybrid", "confidence": 0.70},
        {"source": "b", "value": "onsite", "confidence": 0.70},
    ]
    winner, _, _ = dd.compute_consensus(obs)
    assert winner == "onsite"


def test_consensus_skips_unknown_and_empty():
    """'unknown' / '' / None are non-votes — they shouldn't drag totals down."""
    obs = [
        {"source": "a", "value": "unknown", "confidence": 0.55},
        {"source": "b", "value": None, "confidence": 0.90},
        {"source": "c", "value": "remote", "confidence": 0.50},
    ]
    winner, conf, _ = dd.compute_consensus(obs)
    assert winner == "remote"
    assert conf == 1.0  # only remote counted, so confidence is 100%


def test_consensus_empty_returns_none():
    assert dd.compute_consensus([]) is None
    assert dd.compute_consensus([{"value": "unknown", "confidence": 0.5}]) is None


# ─── merge_field_sources ──────────────────────────────────

def test_merge_field_sources_concatenates_per_field():
    primary = {"_field_sources": {
        "is_remote": [{"source": "jsearch", "value": "remote", "confidence": 0.55}],
    }}
    peer = {"_field_sources": {
        "is_remote": [{"source": "greenhouse", "value": "hybrid", "confidence": 0.90}],
        "salary_min": [{"source": "greenhouse", "value": 150000, "confidence": 0.90}],
    }}
    dd.merge_field_sources(primary, peer)
    assert len(primary["_field_sources"]["is_remote"]) == 2
    assert primary["_field_sources"]["salary_min"][0]["value"] == 150000


def test_merge_field_sources_creates_primary_fs_when_missing():
    primary = {"title": "X"}  # no _field_sources yet
    peer = {"_field_sources": {
        "is_remote": [{"source": "a", "value": "remote", "confidence": 0.5}],
    }}
    dd.merge_field_sources(primary, peer)
    assert "is_remote" in primary["_field_sources"]


def test_merge_field_sources_noop_when_peer_has_none():
    primary = {"_field_sources": {"x": [{"v": 1}]}}
    dd.merge_field_sources(primary, {})
    assert primary["_field_sources"] == {"x": [{"v": 1}]}


# ─── apply_consensus integration ──────────────────────────

def test_apply_consensus_overrides_flat_when_vote_strong():
    """Aggregator said remote, text_classifier + Greenhouse say hybrid →
    consensus promotes 'hybrid' into the flat is_remote field."""
    job = {
        "is_remote": "remote",
        "_field_sources": {
            "is_remote": [
                {"source": "jsearch", "value": "remote", "confidence": 0.55},
                {"source": "greenhouse", "value": "hybrid", "confidence": 0.90},
                {"source": "text_classifier", "value": "hybrid", "confidence": 0.85},
            ],
        },
    }
    stats = dd.apply_consensus([job])
    assert job["is_remote"] == "hybrid"
    assert stats["overrides"] == 1
    # Consensus metadata stashed for downstream display
    assert job["_consensus"]["is_remote"]["value"] == "hybrid"
    assert job["_consensus"]["is_remote"]["confidence"] > 0.70
    assert "greenhouse" in job["_consensus"]["is_remote"]["sources"]


def test_apply_consensus_leaves_flat_when_vote_matches():
    """No-op path: consensus agrees with the flat value, no update needed
    but vote is still stashed for transparency."""
    job = {
        "is_remote": "remote",
        "_field_sources": {
            "is_remote": [
                {"source": "greenhouse", "value": "remote", "confidence": 0.90},
                {"source": "text_classifier", "value": "remote", "confidence": 0.85},
            ],
        },
    }
    stats = dd.apply_consensus([job])
    assert job["is_remote"] == "remote"
    assert stats["overrides"] == 0
    assert stats["tied_to_source"] == 1


def test_apply_consensus_below_threshold_keeps_flat():
    """A marginal vote (close to 50/50) shouldn't override the source's
    explicit value. The override threshold protects against flaky flips
    when pattern matches are ambiguous."""
    job = {
        "is_remote": "remote",
        "_field_sources": {
            "is_remote": [
                {"source": "jsearch", "value": "remote", "confidence": 0.55},
                {"source": "text_classifier", "value": "hybrid", "confidence": 0.60},
            ],
        },
    }
    stats = dd.apply_consensus([job])
    # hybrid weight 0.60 / total 1.15 ≈ 0.52 → below 0.65 threshold
    assert job["is_remote"] == "remote"
    assert stats["below_threshold"] >= 1


def test_apply_consensus_no_observations_skipped():
    job = {"is_remote": "remote"}  # no _field_sources at all
    stats = dd.apply_consensus([job])
    assert job["is_remote"] == "remote"
    assert stats["votes_applied"] == 0


def test_apply_consensus_work_arrangement_independent_of_is_remote():
    """Voting is per-field; work_arrangement can have a different winner
    than is_remote when sources disagree asymmetrically."""
    job = {
        "is_remote": "remote",
        "work_arrangement": "Hybrid",
        "_field_sources": {
            "work_arrangement": [
                {"source": "greenhouse", "value": "hybrid", "confidence": 0.90},
                {"source": "text_classifier", "value": "hybrid", "confidence": 0.85},
            ],
        },
    }
    stats = dd.apply_consensus([job])
    assert "work_arrangement" in job["_consensus"]
    assert job["_consensus"]["work_arrangement"]["value"] == "hybrid"


# ─── End-to-end: dedup peer merge preserves provenance ───

def test_dedup_peer_merge_carries_provenance():
    """Two jobs about the same role from different sources should merge
    their observations so consensus sees all votes. Previously, the
    displaced peer's _field_sources was dropped — halving the evidence."""
    from src.processors.deduplicator import deduplicate
    job_a = {
        "external_id": "jsearch_1",
        "title": "People Analytics Manager",
        "company": "Netflix",
        "company_normalized": "netflix",
        "location": "Los Gatos, CA",
        "apply_url": "https://jooble.org/jdp/1",
        "is_remote": "remote",
        "_field_sources": {
            "is_remote": [{"source": "jsearch", "value": "remote", "confidence": 0.55}],
        },
    }
    job_b = {
        "external_id": "gh_2",
        "title": "People Analytics Manager",
        "company": "Netflix",
        "company_normalized": "netflix",
        "location": "Los Gatos, CA",
        "apply_url": "https://boards.greenhouse.io/netflix/jobs/2",
        "is_remote": "hybrid",
        "_field_sources": {
            "is_remote": [{"source": "greenhouse", "value": "hybrid", "confidence": 0.90}],
        },
    }
    kept, skipped = deduplicate([job_a, job_b])
    assert len(kept) == 1
    # Whoever won, the winning primary must carry BOTH observations
    primary = kept[0]
    obs = primary["_field_sources"]["is_remote"]
    sources = {o["source"] for o in obs}
    assert sources == {"jsearch", "greenhouse"}, (
        f"expected both sources' observations, got {sources}"
    )
