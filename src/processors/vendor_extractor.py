"""Phase 5 (R3): extract vendor/tool/skill mentions from a job description.

Pure regex, zero dependencies. Each pattern yields a canonical display name —
stored comma-separated on the job dict so the WordPress column (or any
downstream query) can split without loss.

Design notes:
- "R" as a programming language is tricky. We use `\bR\b` plus a negative
  lookahead for "&" / "and" so "R&D", "R & D", and "R and D" don't false-fire.
- "Workday" on its own excludes "Workday Peakon" (different vendor entry).
"""
from __future__ import annotations

import re
from typing import Final

# name → compiled pattern
VENDOR_PATTERNS: Final[dict[str, re.Pattern[str]]] = {
    # EL / engagement platforms
    "Qualtrics": re.compile(r"\bqualtrics\b", re.IGNORECASE),
    "Medallia": re.compile(r"\bmedallia\b", re.IGNORECASE),
    "Glint": re.compile(r"\bglint\b", re.IGNORECASE),
    "Culture Amp": re.compile(r"\bculture\s*amp\b", re.IGNORECASE),
    "Perceptyx": re.compile(r"\bperceptyx\b", re.IGNORECASE),
    "Workday Peakon": re.compile(r"\b(peakon|workday\s+peakon)\b", re.IGNORECASE),
    "Gallup": re.compile(r"\bgallup\b", re.IGNORECASE),
    "Lattice": re.compile(r"\blattice\b", re.IGNORECASE),
    "15Five": re.compile(r"\b15\s*five\b", re.IGNORECASE),
    "BetterUp": re.compile(r"\bbetterup\b", re.IGNORECASE),
    "SurveyMonkey": re.compile(r"\b(surveymonkey|momentive)\b", re.IGNORECASE),
    "Microsoft Viva": re.compile(
        r"\b(viva\s+insights?|microsoft\s+viva|workplace\s+analytics)\b",
        re.IGNORECASE,
    ),
    "Quantum Workplace": re.compile(r"\bquantum\s+workplace\b", re.IGNORECASE),
    "TINYpulse": re.compile(r"\btinypulse\b", re.IGNORECASE),

    # HRIS / HCM
    "Workday": re.compile(r"\bworkday\b(?!\s+peakon)", re.IGNORECASE),
    "SAP SuccessFactors": re.compile(r"\b(successfactors|sap\s+sf)\b", re.IGNORECASE),
    "Oracle HCM": re.compile(r"\b(oracle\s+hcm|oracle\s+cloud\s+hcm)\b", re.IGNORECASE),
    "ADP": re.compile(r"\badp\b", re.IGNORECASE),
    "UKG": re.compile(r"\b(ukg|ultimate\s+kronos|ultipro)\b", re.IGNORECASE),
    "BambooHR": re.compile(r"\bbamboohr\b", re.IGNORECASE),
    "Dayforce": re.compile(r"\b(dayforce|ceridian)\b", re.IGNORECASE),

    # ATS
    "Greenhouse": re.compile(r"\bgreenhouse\b", re.IGNORECASE),
    "Lever": re.compile(r"\blever\b", re.IGNORECASE),
    "iCIMS": re.compile(r"\bicims\b", re.IGNORECASE),
    "SmartRecruiters": re.compile(r"\bsmartrecruiters\b", re.IGNORECASE),
    "Ashby": re.compile(r"\bashby\b", re.IGNORECASE),

    # BI / visualization
    "Tableau": re.compile(r"\btableau\b", re.IGNORECASE),
    "Power BI": re.compile(r"\bpower\s*bi\b", re.IGNORECASE),
    "Looker": re.compile(r"\blooker\b", re.IGNORECASE),

    # Analytics / programming
    # "R" must NOT match "R&D" / "R & D" / "R and D" / "HR" / "our"
    "R": re.compile(r"\bR\b(?!\s*(?:&|\band\b))"),
    "Python": re.compile(r"\bpython\b", re.IGNORECASE),
    "SQL": re.compile(r"\bsql\b", re.IGNORECASE),
    "SPSS": re.compile(r"\bspss\b", re.IGNORECASE),
    "SAS": re.compile(r"\bsas\b", re.IGNORECASE),
    "Stata": re.compile(r"\bstata\b", re.IGNORECASE),

    # Data platforms
    "Snowflake": re.compile(r"\bsnowflake\b", re.IGNORECASE),
    "Databricks": re.compile(r"\bdatabricks\b", re.IGNORECASE),
    "BigQuery": re.compile(r"\bbigquery\b", re.IGNORECASE),
    "Redshift": re.compile(r"\bredshift\b", re.IGNORECASE),

    # ONA / specialized
    # 'analy' is a prefix (analysis/analytics); no trailing \b so it matches both.
    "Organizational Network Analysis": re.compile(
        r"\b(ONA|organizational\s+network\s+analy)", re.IGNORECASE,
    ),
    "Visier": re.compile(r"\bvisier\b", re.IGNORECASE),
    "One Model": re.compile(r"\bone\s+model\b", re.IGNORECASE),
    "Included.ai": re.compile(r"\bincluded\s*(\.ai)?\b", re.IGNORECASE),
    "Crunchr": re.compile(r"\bcrunchr\b", re.IGNORECASE),
    "ChartHop": re.compile(r"\bcharthop\b", re.IGNORECASE),
}


def extract_vendors(description: str) -> list[str]:
    """Return the set of vendor/tool names mentioned in `description`, preserving
    the canonical order of VENDOR_PATTERNS (not insertion order of hits)."""
    if not description:
        return []
    return [name for name, pat in VENDOR_PATTERNS.items() if pat.search(description)]


def vendors_to_str(vendors: list[str]) -> str:
    return ",".join(vendors)
