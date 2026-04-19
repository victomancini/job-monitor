"""Keyword filter: word-boundary regex, three-tier scoring, explicit conflict → LLM."""
from __future__ import annotations

import html
import logging
import re
import unicodedata
from functools import lru_cache
from typing import Any

from src.shared import load_companies, load_keywords

log = logging.getLogger(__name__)

COMPANY_BOOST_POINTS = 10  # Phase B3: was 15, now gated on positive-keyword match
DESCRIPTION_SNIPPET_LEN = 300
T2_DESC_CAP = 16  # Phase B1

# Phase B5: co-signal list for generic T3 title matches (checked in description)
T3_COSIGNAL = (
    "analytics", "insights", "data", "dashboard", "survey",
    "Python", "SQL", "Tableau", "R programming",
    "Qualtrics", "Workday", "people data", "HRIS", "Visier",
)

# Phase B8: narrower co-signal for "employee experience" / "workforce planning"
# (checked in title + first 400 chars of description). Per-term cosignals —
# workforce planning gets a broader set because it's routinely paired with a
# role/optimization word rather than an analytics word (e.g., "Senior Workforce
# Planning Manager", "Workforce Planning Optimisation Manager"). Employee
# experience keeps the stricter analytics-only cosignal so hospitality
# "Employee Experience Coordinator" roles stay out.
_B8_BASE_COSIGNAL: tuple[str, ...] = ("analytics", "insights", "data", "survey", "listening")
B8_COSIGNALS: dict[str, tuple[str, ...]] = {
    "employee experience": _B8_BASE_COSIGNAL,
    "workforce planning": _B8_BASE_COSIGNAL + (
        "manager", "director", "lead", "head", "analyst",
        "optimization", "optimisation", "strategy", "strategic",
    ),
}
# Negative cosignal: if any of these appears in title+desc, the B8 gate for
# "workforce planning" fails even when a positive cosignal is present. Catches
# retail/hospital workforce-scheduling roles that title-match the PA function
# but have shift/staff-scheduling context.
B8_NEGATIVE_COSIGNALS: dict[str, tuple[str, ...]] = {
    "workforce planning": (
        "nurse", "nursing", "shift schedule", "shift schedules",
        "staff schedule", "staff scheduling", "staffing ratio",
        "staffing ratios", "clinical staff", "retail staff", "call center",
        "call centre", "contact center", "contact centre",
    ),
}
# Per-term point value. Default keeps the conservative 5-pt B8 floor for
# "employee experience". Workforce planning pays 15 — the llm_review_min
# floor — so gated matches reach LLM triage instead of being dropped.
_B8_DEFAULT_POINTS = 5
B8_POINTS_BY_TERM: dict[str, int] = {
    "employee experience": 5,
    "workforce planning": 15,
}
# Kept for backwards compat with any external imports
B8_COSIGNAL = _B8_BASE_COSIGNAL
B8_TERMS = frozenset(B8_COSIGNALS.keys())
B8_POINTS = _B8_DEFAULT_POINTS
B8_DESC_WINDOW = 400

# Phase B7: continuous-listening gate
CONT_LISTENING_TERM = "continuous listening"
CONT_LISTENING_COTERMS = ("employee", "workforce")
CONT_LISTENING_WINDOW = 60
CONT_LISTENING_T1_POINTS = 50

# Phase B6: "change management consultant" only penalized when no listening/analytics co-term
CMC_TERM = "change management consultant"
CMC_GATE_COTERMS = ("listening", "analytics")

# Phase C5: "XM scientist" only scores when "employee"/"EX"/"EE" appears within 60 chars of match,
# or anywhere in description (broader context).
XM_SCIENTIST_TERM = "xm scientist"
XM_SCIENTIST_COTERMS = ("employee", "EX", "EE")
XM_SCIENTIST_WINDOW = 60

# Phase D2: gated T2-title terms. Each requires at least one co-term to appear within
# XM_SCIENTIST_WINDOW chars of the match in title, or anywhere in description.
T2_TITLE_GATES: dict[str, tuple[str, ...]] = {
    "behavioral scientist": ("HR", "people", "employee", "workforce"),
    "labor economist": ("HR", "people", "employee", "employer", "workforce", "corporate", "company"),
    "partner experience analyst": ("Starbucks", "HR", "people", "partner engagement"),
    # R-audit (shadow log 2026-04-18): "Insights Analyst - Employee Engagement &
    # Survey Analytics" scored 0 because bare "employee engagement" wasn't a
    # keyword and no role-variant matched contiguously. Adding bare
    # "employee engagement" with an analytics/leadership cosignal catches the
    # role-analyst and director-level variants while excluding event-planning
    # "Employee Engagement Coordinator" titles (no cosignal → fail).
    "employee engagement": (
        "analytics", "insights", "data", "survey", "listening",
        "analyst", "director", "head", "vp",
    ),
}


def _preprocess(text: str) -> str:
    """Normalize text before keyword matching."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unicodedata.normalize('NFKC', text)
    text = re.sub(r'[\u2010-\u2015\u2212\u00AD]', '-', text)
    text = re.sub(r'[\u2018\u2019]', "'", text)
    text = re.sub(r'[\u201C\u201D]', '"', text)
    text = re.sub(r"'s\b", '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


@lru_cache(maxsize=None)
def _compile_terms(terms: tuple[str, ...]) -> list[tuple[str, re.Pattern[str]]]:
    return [(t, re.compile(r"\b" + re.escape(t) + r"\b", re.IGNORECASE)) for t in terms]


def _find_matches(text: str, terms: tuple[str, ...]) -> list[str]:
    if not text:
        return []
    return [t for t, pat in _compile_terms(terms) if pat.search(text)]


def _find_match_spans(text: str, terms: tuple[str, ...]) -> list[tuple[str, tuple[int, int]]]:
    """Return all (term, (start, end)) matches — one entry per occurrence."""
    if not text:
        return []
    out: list[tuple[str, tuple[int, int]]] = []
    for term, pat in _compile_terms(terms):
        for m in pat.finditer(text):
            out.append((term, m.span()))
    return out


def _prune_covered_spans(
    hits: list[tuple[str, str, int, tuple[int, int]]],
) -> list[tuple[str, str, int, tuple[int, int]]]:
    """Phase C7 maximal-munch: per field, drop any hit whose span is strictly contained
    in another hit's span. Hits come as (term, field, score, span)."""
    by_field: dict[str, list[tuple[str, str, int, tuple[int, int]]]] = {}
    for h in hits:
        by_field.setdefault(h[1], []).append(h)
    kept: list[tuple[str, str, int, tuple[int, int]]] = []
    for _field, group in by_field.items():
        # Sort by span length desc; break ties by higher score to prefer informative hits
        group_sorted = sorted(
            group, key=lambda h: (-(h[3][1] - h[3][0]), -h[2])
        )
        kept_spans: list[tuple[int, int]] = []
        for h in group_sorted:
            s, e = h[3]
            covered = any(
                ks <= s and e <= ke and (ks, ke) != (s, e) for (ks, ke) in kept_spans
            )
            if not covered:
                kept.append(h)
                kept_spans.append((s, e))
    return kept


def _cross_field_dedup(
    hits: list[tuple[str, str, int, tuple[int, int]]],
) -> list[tuple[str, str, int, tuple[int, int]]]:
    """Phase C6: if the same (normalized) term matches in multiple fields, keep the
    single highest-scoring hit. Distinct terms are always kept."""
    best: dict[str, tuple[str, str, int, tuple[int, int]]] = {}
    for h in hits:
        key = h[0].lower()
        cur = best.get(key)
        if cur is None or h[2] > cur[2] or (h[2] == cur[2] and h[1] == "title" and cur[1] == "desc"):
            best[key] = h
    return list(best.values())


def _has_any(text: str, terms: tuple[str, ...]) -> bool:
    if not text:
        return False
    for _, pat in _compile_terms(terms):
        if pat.search(text):
            return True
    return False


def _proximity_match(text: str, target: str, co_terms: tuple[str, ...], window: int) -> bool:
    """True if any co_term appears within `window` chars of any match of `target`."""
    if not text:
        return False
    target_pat = re.compile(r"\b" + re.escape(target) + r"\b", re.IGNORECASE)
    for m in target_pat.finditer(text):
        s = max(0, m.start() - window)
        e = min(len(text), m.end() + window)
        if _has_any(text[s:e], co_terms):
            return True
    return False


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
    title = _preprocess(job.get("title") or "")
    desc = _preprocess(job.get("description") or "")

    # Each hit: (term, field, score, span). `tier` is tracked out-of-band in `hit_tier`
    # (keyed by id(hit)) so we can detect T2-desc hits for the cap later.
    hits: list[tuple[str, str, int, tuple[int, int]]] = []
    hit_tier: dict[int, str] = {}

    def _push(term: str, field: str, pts: int, span: tuple[int, int], tier: str) -> None:
        h = (term, field, pts, span)
        hits.append(h)
        hit_tier[id(h)] = tier

    # ── T1 title (excluding "continuous listening"; handled specially) ──
    for t in kw["tier1_title"]["terms"]:
        if t.lower() == CONT_LISTENING_TERM:
            continue
        for term, span in _find_match_spans(title, (t,)):
            _push(term, "title", kw["tier1_title"]["score"], span, "t1_title")

    # ── T1 description ──
    for term, span in _find_match_spans(desc, tuple(kw["tier1_description"]["terms"])):
        _push(term, "desc", kw["tier1_description"]["score"], span, "t1_desc")

    # ── T2 title (with C5 XM-scientist gate and continuous-listening carve-out) ──
    for t in kw["tier2_title"]["terms"]:
        tl = t.lower()
        if tl == CONT_LISTENING_TERM:
            continue
        for term, span in _find_match_spans(title, (t,)):
            if tl == XM_SCIENTIST_TERM:
                # C5 gate: co-term within 60 chars of match in title, OR anywhere in desc
                s, e = span
                win = title[max(0, s - XM_SCIENTIST_WINDOW):min(len(title), e + XM_SCIENTIST_WINDOW)]
                if not (_has_any(win, XM_SCIENTIST_COTERMS) or _has_any(desc, XM_SCIENTIST_COTERMS)):
                    continue
            elif tl in T2_TITLE_GATES:
                # D2 gate: co-term within 60 chars of match in title, OR anywhere in desc
                gate_terms = T2_TITLE_GATES[tl]
                s, e = span
                win = title[max(0, s - XM_SCIENTIST_WINDOW):min(len(title), e + XM_SCIENTIST_WINDOW)]
                if not (_has_any(win, gate_terms) or _has_any(desc, gate_terms)):
                    continue
            _push(term, "title", kw["tier2_title"]["score"], span, "t2_title")

    # ── Phase B7: "continuous listening" in title — 25 default, 50 with co-term proximity ──
    for term, span in _find_match_spans(title, (CONT_LISTENING_TERM,)):
        if _proximity_match(title, CONT_LISTENING_TERM, CONT_LISTENING_COTERMS, CONT_LISTENING_WINDOW):
            _push(term, "title", CONT_LISTENING_T1_POINTS, span, "cont_listening_t1")
        else:
            _push(term, "title", kw["tier2_title"]["score"], span, "cont_listening_t2")

    # ── T2 description (cap applied after dedup) ──
    for term, span in _find_match_spans(desc, tuple(kw["tier2_description"]["terms"])):
        _push(term, "desc", kw["tier2_description"]["score"], span, "t2_desc")

    # ── T3 title with gating (Phases B5, B8) ──
    t3_pts = kw["tier3_title"]["score"]
    for term, span in _find_match_spans(title, tuple(kw["tier3_title"]["terms"])):
        tl = term.lower()
        if tl in B8_TERMS:
            scope = f"{title} {desc[:B8_DESC_WINDOW]}"
            cosignal = B8_COSIGNALS.get(tl, _B8_BASE_COSIGNAL)
            neg_cosignal = B8_NEGATIVE_COSIGNALS.get(tl, ())
            # Full title+desc for the negative check — we want a nurse-shift
            # mention anywhere in the posting to veto, not just the first 400.
            full_scope = f"{title} {desc}"
            if _has_any(scope, cosignal) and not _has_any(full_scope, neg_cosignal):
                pts = B8_POINTS_BY_TERM.get(tl, _B8_DEFAULT_POINTS)
                _push(term, "title", pts, span, "t3_b8")
        else:
            if _has_any(desc, T3_COSIGNAL):
                _push(term, "title", t3_pts, span, "t3_title")

    # ── Phase C7: per-field maximal-munch (drop spans strictly inside longer spans) ──
    hits = _prune_covered_spans(hits)
    # ── Phase C6: same term across title+desc counted once at highest score ──
    hits = _cross_field_dedup(hits)

    # ── Apply T2 desc cap (Phase B1) ──
    t2_desc_contrib = sum(h[2] for h in hits if hit_tier.get(id(h)) == "t2_desc")
    if t2_desc_contrib > T2_DESC_CAP:
        overflow = t2_desc_contrib - T2_DESC_CAP
    else:
        overflow = 0

    score = sum(h[2] for h in hits) - overflow
    matched: list[str] = [h[0] for h in hits]
    title_has_positive = any(h[1] == "title" for h in hits)
    any_positive = bool(hits)

    # ── Negative auto-reject ──
    neg_auto_title = _find_matches(title, tuple(kw["negative_auto_reject"]["terms"]))
    neg_auto_desc = _find_matches(desc, tuple(kw["negative_auto_reject"]["terms"]))
    has_any_negative = bool(neg_auto_title or neg_auto_desc)

    # ── Graduated score reducers (Phase B6) ──
    reducers_cfg = kw["negative_score_reducers"]
    for tier_name in ("hard", "medium", "standard"):
        tier_cfg = reducers_cfg.get(tier_name)
        if not tier_cfg:
            continue
        tier_pts = tier_cfg["score"]
        for m in _find_matches(f"{title}\n{desc}", tuple(tier_cfg["terms"])):
            # Gate: "change management consultant" only penalized when no listening/analytics co-term
            if m.lower() == CMC_TERM and _has_any(f"{title} {desc}", CMC_GATE_COTERMS):
                continue
            score += tier_pts
            matched.append(f"-:{m}")

    # ── Company boost (Phase B3/R-audit): +10 only if the TITLE already has a
    # positive keyword. Previously gated on `any_positive` (title OR desc), which
    # let vendor-self-mentions in the description trigger a boost on irrelevant
    # roles (e.g., "Allbound SDR" at Culture Amp, where the boilerplate "About
    # Culture Amp" matched tier1_description). Requiring a title positive keeps
    # the boost aligned with real role-level relevance.
    if title_has_positive and _company_matches_boost_list(job.get("company", "")):
        score += COMPANY_BOOST_POINTS
        matched.append(f"+:company:{job.get('company', '')}")

    # De-duplicate matched terms while preserving order
    seen: set[str] = set()
    matched_unique: list[str] = []
    for m in matched:
        if m not in seen:
            seen.add(m)
            matched_unique.append(m)

    # Cap score at 100
    if score > 100:
        score = 100

    thresholds = kw.get("thresholds", {})
    auto_include_t = thresholds.get("auto_include", 50)
    llm_review_min = thresholds.get("llm_review_min", 15)

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

    # Phase B2: auto-publish requires at least one positive title match
    if decision == "auto_include" and not title_has_positive:
        decision = "llm_review"

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
