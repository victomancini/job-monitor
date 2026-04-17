"""Assign a functional sub-category to each job after keyword filtering.

Used purely for UI filtering in the WordPress table — not for any scoring. First
matching regex wins; falls back to consulting-company check, then 'General PA'.
"""
from __future__ import annotations

import re
from typing import Final

# Order matters. More specific categories first — "HRIS" beats "People Analytics",
# "Employee Listening" beats "EX & Culture", etc.
JOB_CATEGORIES: Final[list[tuple[str, str]]] = [
    # Employee Listening (most specific — check first)
    (r"\b(employee\s+listening|voice\s+of\s+(the\s+)?employee|continuous\s+listening|listening\s+strategy|survey\s+analyst.*listening)\b", "Employee Listening"),
    # HRIS & Systems (check before PA — "HRIS & People Analytics" should be HRIS)
    (r"\bHRIS\b", "HRIS & Systems"),
    # Research / I-O Psychology — trailing prefixes (scien, psych, etc.), no \b suffix
    (r"\b(people\s+scien|research\s+scien|I-O\s+psych|industrial.organizational|behavioral\s+scien|psychometri)", "Research / I-O"),
    # Data Engineering — full words
    (r"\b(data\s+engineer|analytics\s+engineer|automation\s+engineer|data\s+architect)\b", "Data Engineering"),
    # Pay Equity — mixed (equity full, analy prefix)
    (r"\b(pay\s+equity|workplace\s+equity|compensation\s+analy)", "Pay Equity"),
    # Workforce Planning — SWP full token, optimi prefix
    (r"\b(workforce\s+planning|SWP|workforce\s+optimi)", "Workforce Planning"),
    # Talent Intelligence — "intelligen" prefix
    (r"\b(talent\s+intelligen|workforce\s+intelligen|skills\s+intelligen)", "Talent Intelligence"),
    # EX / Culture — mixed (experience/manager/director full, analy prefix)
    (r"\b(employee\s+experience|EX\s+advisor|culture\s+analy|engagement\s+manager|engagement\s+director)", "EX & Culture"),
    # People Analytics (broadest — "analy" prefix matches analytics/analysis/analyst)
    (r"\b(people\s+analy|HR\s+analy|workforce\s+analy|talent\s+analy|human\s+capital\s+analy)", "People Analytics"),
]

CONSULTING_COMPANIES: Final[set[str]] = {
    "deloitte", "pwc", "mckinsey", "ey", "kpmg", "mercer", "wtw",
    "korn ferry", "kincentric", "bain", "bcg", "accenture",
}

_CONSULTING_SUFFIXES = (
    " inc", " llc", " corp", " ltd", " co", " corporation", " company",
    " group", " consulting", " llp", " & co", " & company",
)


def _normalize_consulting_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[,.&]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Strip trailing common corporate suffixes repeatedly (handles "Mercer LLC Inc")
    changed = True
    while changed:
        changed = False
        for suf in _CONSULTING_SUFFIXES:
            if s.endswith(suf):
                s = s[: -len(suf)].strip()
                changed = True
    return s


def classify_category(title: str, company: str = "", description: str = "") -> str:
    """Return the functional sub-category for a job. First-match-wins; falls back
    to 'Consulting' for known-consulting companies, then 'General PA'."""
    text = f"{title or ''} {(description or '')[:500]}"
    for pattern, category in JOB_CATEGORIES:
        if re.search(pattern, text, re.IGNORECASE):
            return category
    if _normalize_consulting_name(company) in CONSULTING_COMPANIES:
        return "Consulting"
    return "General PA"
