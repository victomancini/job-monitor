"""Extract seniority/level from job titles using priority-ordered pattern matching."""
from __future__ import annotations

import re

from src.processors.keyword_filter import _preprocess

# Order matters — first match wins. See spec in tasks/table-enrichment.md Phase B
# and tasks/table-improvements-r2.md Phase F for PA/EL-specific additions.
SENIORITY_MAP: list[tuple[str, str]] = [
    # Phase F (R2): PA/EL-specific patterns, most specific first
    (r"\bintern(ship)?\b", "Intern"),
    (r"\b(managing\s+director)\b", "Executive"),  # must beat plain \bdirector\b below
    (r"\b(staff)\s+(program\s+manager|scientist|engineer)\b", "Senior IC"),
    (r"\b(fellow)\b", "Senior IC"),  # "AI-Driven People Analytics Fellow"
    # Executive
    (r"\b(chief|c-suite|chro|cpo|cao|cdo|cto)\b", "Executive"),
    (r"\bsvp\b", "Executive"),
    (r"\b(senior|sr\.?)\s+vice\s+president\b", "Executive"),
    # VP
    (r"\bvice\s+president\b", "VP"),
    (r"\bvp\b", "VP"),
    # Director
    (r"\b(senior|sr\.?)\s+director\b", "Senior Director"),
    (r"\bglobal\s+head\s+of\b", "Senior Director"),
    (r"\bhead\s+of\b", "Director"),
    (r"\bdirector\b", "Director"),
    # Senior Manager
    (r"\b(senior|sr\.?)\s+manager\b", "Senior Manager"),
    # Manager
    (r"\bmanager\b", "Manager"),
    # Senior IC
    (r"\bprincipal\b", "Senior IC"),
    (r"\bstaff\b", "Senior IC"),
    (r"\b(senior|sr\.?)\s+(analyst|scientist|engineer|researcher|consultant|associate|specialist)\b", "Senior IC"),
    (r"\blead\b", "Senior IC"),
    # IC
    (r"\b(analyst|scientist|engineer|researcher|specialist|coordinator|associate)\b", "IC"),
    # Consultant
    (r"\bconsultant\b", "IC"),
]

VALID_SENIORITIES = {
    "Executive", "VP", "Senior Director", "Director",
    "Senior Manager", "Manager", "Senior IC", "IC",
    "Intern",  # Phase F (R2)
    "Unknown",
}


def infer_seniority_from_salary(salary_min: float | int | None) -> str | None:
    """Phase F (R2): coarse salary-based fallback when regex + LLM both return
    Unknown. Returns None when salary is missing or cannot be interpreted."""
    if salary_min is None:
        return None
    try:
        s = float(salary_min)
    except (TypeError, ValueError):
        return None
    if s <= 0:
        return None
    if s < 1000:  # "120" → $120K
        s *= 1000
    if s >= 200_000:
        return "Director"
    if s >= 150_000:
        return "Senior Manager"
    if s >= 100_000:
        return "Manager"
    if s >= 60_000:
        return "IC"
    return "Intern"


def extract_seniority(title: str) -> str:
    """Return the seniority string or 'Unknown' (case-insensitive match)."""
    if not title:
        return "Unknown"
    text = _preprocess(title)
    for pattern, level in SENIORITY_MAP:
        if re.search(pattern, text, re.IGNORECASE):
            return level
    return "Unknown"
