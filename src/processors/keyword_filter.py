"""Keyword filter: word-boundary regex, three-tier scoring, explicit conflict → LLM."""
from __future__ import annotations

import logging
import re
from functools import lru_cache
from typing import Any

from src.shared import load_companies, load_keywords

log = logging.getLogger(__name__)

COMPANY_BOOST_POINTS = 15
DESCRIPTION_SNIPPET_LEN = 300


@lru_cache(maxsize=None)
def _compile_terms(terms: tuple[str, ...]) -> list[tuple[str, re.Pattern[str]]]:
    return [(t, re.compile(r"\b" + re.escape(t) + r"\b", re.IGNORECASE)) for t in terms]


def _find_matches(text: str, terms: tuple[str, ...]) -> list[str]:
    if not text:
        return []
    return [t for t, pat in _compile_terms(terms) if pat.search(text)]


@lru_cache(maxsize=1)
def _boost_companies() -> set[str]:
    """Return lowercase names of Tier 1 + Tier 2 companies for +15 keyword boost."""
    cfg = load_companies()
    out: set[str] = set()
    for tier in ("tier1", "tier2"):
        for entry in cfg.get(tier, []) or []:
            name = (entry.get("name") or "").strip().lower()
            if name:
                out.add(name)
    return out


def _company_matches_boost_list(company: str) -> bool:
    c = (company or "").strip().lower()
    if not c:
        return False
    if c in _boost_companies():
        return True
    # Also match "Acme Inc" / "Acme Corp" / "Acme, LLC" → "Acme"
    stripped = re.sub(r",?\s+(inc|llc|corp|ltd|co\.?|corporation|company)\.?$", "", c)
    return stripped in _boost_companies() or stripped.rstrip(".") in _boost_companies()


def classify(job: dict[str, Any]) -> dict[str, Any]:
    """Score and decide. Mutates `job` with keyword_score, keywords_matched, fit_score,
    description_snippet, and returns a decision dict {'decision': str, 'score': int, 'matched': [...]}.

    Decisions:
      - 'auto_include' : score >= 50
      - 'llm_review'   : score in [10, 49], OR positive+negative conflict, OR source=google_alerts
      - 'auto_reject'  : only negative_auto_reject hit on title AND no positives
      - 'low_score'    : score < 10 with no negatives (caller may reject or log)
    """
    kw = load_keywords()
    title = (job.get("title") or "").strip()
    desc = (job.get("description") or "").strip()
    title_lower_space = f" {title} "  # for word-boundary regex to handle edge cases

    score = 0
    matched: list[str] = []

    def _accum(text: str, terms: tuple[str, ...], points: int):
        nonlocal score
        for m in _find_matches(text, terms):
            score += points
            matched.append(m)

    _accum(title, tuple(kw["tier1_title"]["terms"]), kw["tier1_title"]["score"])
    _accum(desc, tuple(kw["tier1_description"]["terms"]), kw["tier1_description"]["score"])
    _accum(title, tuple(kw["tier2_title"]["terms"]), kw["tier2_title"]["score"])
    _accum(desc, tuple(kw["tier2_description"]["terms"]), kw["tier2_description"]["score"])
    _accum(title, tuple(kw["tier3_title"]["terms"]), kw["tier3_title"]["score"])

    # Negative auto-reject terms
    neg_auto_title = _find_matches(title, tuple(kw["negative_auto_reject"]["terms"]))
    neg_auto_desc = _find_matches(desc, tuple(kw["negative_auto_reject"]["terms"]))
    has_any_negative = bool(neg_auto_title or neg_auto_desc)

    # Score reducers
    reducer_pts = kw["negative_score_reducers"]["score"]  # negative integer
    for m in _find_matches(f"{title}\n{desc}", tuple(kw["negative_score_reducers"]["terms"])):
        score += reducer_pts
        matched.append(f"-:{m}")

    # Company boost
    if _company_matches_boost_list(job.get("company", "")):
        score += COMPANY_BOOST_POINTS
        matched.append(f"+:company:{job.get('company', '')}")

    # De-duplicate matched terms while preserving order
    seen: set[str] = set()
    matched_unique: list[str] = []
    for m in matched:
        if m not in seen:
            seen.add(m)
            matched_unique.append(m)

    # Cap score at 100, floor at -100
    if score > 100:
        score = 100

    thresholds = kw.get("thresholds", {})
    auto_include_t = thresholds.get("auto_include", 50)
    llm_review_min = thresholds.get("llm_review_min", 10)

    decision: str
    if has_any_negative and score < llm_review_min:
        # Pure negative, no meaningful positive → hard reject
        score = -100
        decision = "auto_reject"
    elif has_any_negative and score >= llm_review_min:
        # CONFLICT — never auto-decide. LLM must review.
        decision = "llm_review"
    elif score >= auto_include_t:
        decision = "auto_include"
    elif score >= llm_review_min:
        decision = "llm_review"
    else:
        decision = "low_score"

    # google_alerts ALWAYS routes to LLM regardless of keyword score
    if job.get("source_name") == "google_alerts" and decision in ("auto_include", "low_score"):
        decision = "llm_review"

    # Persist into the job dict
    job["keyword_score"] = score
    job["fit_score"] = max(score, 0)
    job["keywords_matched"] = ", ".join(matched_unique)
    if desc:
        job["description_snippet"] = desc[:DESCRIPTION_SNIPPET_LEN]
    else:
        job["description_snippet"] = ""

    return {"decision": decision, "score": score, "matched": matched_unique}
