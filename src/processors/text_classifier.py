"""R11 Phase 2: re-derive work_arrangement / is_remote from description text.

Why this exists: aggregator-supplied `is_remote` fields are noisy. JSearch's
`job_is_remote=true` fires on any mention of "remote" — including "this role
is not remote". Jooble and Adzuna often don't set the field at all. The
description text is the least-ambiguous signal, and the company wrote it —
so we treat it as an independent observation and let consensus voting
(Phase 3) adjudicate when sources disagree.

Unlike enrichment.py's `_extract_remote_status` (which only fills when
is_remote is unset), this module ALWAYS emits a provenance observation
regardless of the current flat value. The two coexist because they feed
different consumers — enrichment writes flat fields, this writes votes.
"""
from __future__ import annotations

import logging
import re
from typing import Any

log = logging.getLogger(__name__)


# Patterns ordered by specificity. Each entry: (regex, arrangement, confidence).
# Confidence 0.85-0.90 = unambiguous phrases, 0.65-0.75 = context-sensitive,
# 0.50 = weak/fallback. The strongest confidence match wins; ties break
# toward the most-restrictive arrangement (onsite > hybrid > remote) so
# ambiguous text doesn't falsely promote roles to remote.
_PATTERNS: list[tuple[re.Pattern, str, float]] = [
    # Explicit remote REJECTIONS — someone said the role is NOT remote.
    # Highest priority because sources that misfire on bare "remote" get
    # corrected here.
    (re.compile(r"\b(?:not\s+a\s+remote|no\s+remote\s+(?:work|option|policy|position|role)|this\s+is\s+not\s+(?:a\s+)?remote|remote\s+is\s+not)\b", re.I),
     "onsite", 0.90),
    (re.compile(r"\b(?:on[- ]?site\s+only|in[- ]?office\s+only|must\s+(?:work\s+)?on[- ]?site|must\s+be\s+(?:in|on)[- ]?site|full[- ]?time\s+in\s+(?:the\s+)?office)\b", re.I),
     "onsite", 0.90),
    # Strong remote signals — phrases that unambiguously mean fully-remote.
    (re.compile(r"\b(?:fully\s+remote|100%\s+remote|remote[- ]first|remote[- ]only|work\s+from\s+anywhere|remote\s+anywhere)\b", re.I),
     "remote", 0.90),
    # Strong hybrid — "N days in office/onsite" phrasing. Pattern matches 1-5
    # days + optional range like "2-3 days" + optional "a week"/"per week".
    (re.compile(r"\b\d+(?:\s*[-\u2013]\s*\d+)?\s*days?\s*(?:a\s+week|per\s+week|each\s+week|weekly)?\s*(?:in[- ](?:the\s+)?(?:office|person|hq)|on[- ]?site)\b", re.I),
     "hybrid", 0.85),
    (re.compile(r"\bhybrid\s+(?:work|role|schedule|position|model|team|policy|arrangement|setup)\b", re.I),
     "hybrid", 0.85),
    # Medium remote
    (re.compile(r"\b(?:this\s+is\s+a\s+remote|remote\s+(?:position|role|opportunity|job|eligible))\b", re.I),
     "remote", 0.75),
    (re.compile(r"\b(?:work\s+from\s+home|telecommut(?:e|ing))\b", re.I),
     "remote", 0.70),
    # Medium hybrid — bare "hybrid" word. Lower than specific phrases because
    # it can appear in boilerplate ("we offer hybrid work") on roles that are
    # actually onsite.
    (re.compile(r"\bhybrid\b", re.I),
     "hybrid", 0.60),
    # Weak onsite — bare onsite/in-office/in-person. Low confidence because
    # these terms appear in unrelated contexts ("in-person interview",
    # "office hours").
    (re.compile(r"\b(?:on[- ]?site|in[- ](?:the\s+)?office|in[- ]person)\b", re.I),
     "onsite", 0.50),
]

# Scan the first N chars only — text after this is usually EEO / benefits /
# company boilerplate that rarely carries new arrangement signal.
_SCAN_LIMIT = 2000

_RESTRICTIVENESS = {"onsite": 0, "hybrid": 1, "remote": 2}


def classify_work_arrangement(description: str | None) -> tuple[str | None, float]:
    """Return (arrangement, confidence).

    arrangement ∈ {"remote", "hybrid", "onsite"} or None when no pattern
    matches. On confidence ties, most restrictive wins — so a description
    mentioning both "hybrid" and "remote" maps to "hybrid" (more restrictive
    for a candidate evaluating fit) rather than "remote".
    """
    if not description:
        return None, 0.0
    text = description[:_SCAN_LIMIT]
    hits: list[tuple[float, str]] = []
    for rgx, arrangement, conf in _PATTERNS:
        if rgx.search(text):
            hits.append((conf, arrangement))
    if not hits:
        return None, 0.0
    hits.sort(key=lambda t: (-t[0], _RESTRICTIVENESS.get(t[1], 3)))
    conf, arrangement = hits[0]
    return arrangement, conf


def classify_batch(jobs: list[dict[str, Any]]) -> dict[str, int]:
    """Emit text_classifier provenance observations for each job.

    Does NOT overwrite the flat `is_remote` or `work_arrangement` — those
    stay as whatever the source set; consensus voting (Phase 3) picks the
    final value. Only adds an observation to the field_sources history.
    """
    stats = {"classified": 0, "remote": 0, "hybrid": 0, "onsite": 0, "no_signal": 0}
    for job in jobs:
        desc = job.get("description") or ""
        arrangement, conf = classify_work_arrangement(desc)
        if arrangement is None:
            stats["no_signal"] += 1
            continue
        fs = job.setdefault("_field_sources", {})
        obs = {
            "source": "text_classifier",
            "value": arrangement,
            "confidence": conf,
        }
        fs.setdefault("is_remote", []).append(obs)
        fs.setdefault("work_arrangement", []).append(obs)
        stats["classified"] += 1
        stats[arrangement] += 1
    return stats
