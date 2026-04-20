"""R11 Phase 4: schema.org JobPosting extraction from canonical company pages.

When enrichment resolves an aggregator URL to a company's own careers page,
roughly 40% of modern ATS products embed `<script type="application/ld+json">`
with a JobPosting structured-data document. That markup is typically
maintained by the company itself and represents the best non-API signal
we can get about the role's actual attributes.

Extraction is pure — given HTML text, return the fields we care about plus
a per-field confidence hint. Integration (when to fetch, per-host budget,
cache) lives in enrichment.py (Phase 5 guardrails). Observations feed the
Phase 3 consensus voter via source='schema_org', reliability 0.85.

Fields extracted (all optional):
- is_remote / work_arrangement ← jobLocationType
- location / location_country ← jobLocation.address
- salary_min / salary_max ← baseSalary.value + .currency
- date_posted ← datePosted
- apply_url ← directApply hint (rarely useful — usually already the URL)
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

log = logging.getLogger(__name__)

# schema.org JobPosting jobLocationType values
# https://schema.org/JobPosting
# TELECOMMUTE is the canonical token; some sites emit free-form strings.
_REMOTE_TOKENS = {"telecommute", "remote", "remote work"}
_HYBRID_TOKENS = {"hybrid", "hybrid work", "hybrid-remote"}

# Scoped regex to pull out every ld+json script block from a page. Non-greedy
# body match, case-insensitive on the script attrs. We intentionally don't
# use a full HTML parser — the body we extract then goes through json.loads
# which is the actual validation.
_LD_JSON_BLOCK = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


def _iter_ld_json_blocks(html: str) -> list[Any]:
    """Extract every ld+json script block and parse each as JSON. Returns the
    parsed objects in document order. Tolerant of malformed blocks — the
    error is swallowed and the block skipped so a bad block on a page doesn't
    hide a valid JobPosting elsewhere."""
    out: list[Any] = []
    if not html:
        return out
    for m in _LD_JSON_BLOCK.finditer(html):
        body = m.group(1).strip()
        if not body:
            continue
        try:
            out.append(json.loads(body))
        except (ValueError, TypeError):
            # Some sites pad their JSON-LD with CDATA wrappers or stray commas.
            # A stricter parser would unwrap those; for now, skip on parse fail
            # and let the caller rely on other signals.
            continue
    return out


def _find_job_posting(obj: Any) -> dict[str, Any] | None:
    """Recursively search a parsed ld+json object for the first node whose
    @type is 'JobPosting'. Supports the common shapes:
      - a bare JobPosting dict
      - a list of nodes
      - an @graph wrapper containing nodes
      - nested @type lists ("JobPosting" among multiple types)
    """
    if obj is None:
        return None
    if isinstance(obj, list):
        for item in obj:
            found = _find_job_posting(item)
            if found is not None:
                return found
        return None
    if not isinstance(obj, dict):
        return None
    types = obj.get("@type")
    if types is not None:
        type_list = types if isinstance(types, list) else [types]
        if any(str(t).lower() == "jobposting" for t in type_list):
            return obj
    # Common wrapper: {"@graph": [...]}
    if "@graph" in obj:
        found = _find_job_posting(obj["@graph"])
        if found is not None:
            return found
    return None


def _parse_remote(jp: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (is_remote_value, work_arrangement) from a JobPosting. Returns
    (None, None) when the field is missing or unrecognized."""
    raw = jp.get("jobLocationType")
    if not raw:
        return None, None
    # Can be a single string or a list
    candidates = raw if isinstance(raw, list) else [raw]
    for c in candidates:
        token = str(c).strip().lower()
        if token in _REMOTE_TOKENS:
            return "remote", "remote"
        if token in _HYBRID_TOKENS:
            return "hybrid", "hybrid"
    return None, None


def _parse_location(jp: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (location, country_code). schema.org JobPosting typically
    nests Place → PostalAddress. Both forms are common."""
    loc = jp.get("jobLocation")
    if not loc:
        return None, None
    # Multiple locations → take the first for the flat field; consensus
    # voting later across multiple observations handles the aggregation.
    if isinstance(loc, list):
        loc = loc[0] if loc else None
        if not loc:
            return None, None
    if not isinstance(loc, dict):
        return None, None
    addr = loc.get("address") or {}
    if isinstance(addr, list):
        addr = addr[0] if addr else {}
    if not isinstance(addr, dict):
        return None, None
    city = addr.get("addressLocality") or ""
    region = addr.get("addressRegion") or ""
    country = addr.get("addressCountry") or ""
    # country can be "US" or a nested {"@type": "Country", "name": "..."}
    if isinstance(country, dict):
        country = country.get("name") or country.get("identifier") or ""
    country = str(country).strip()
    # Normalize to 2-letter where obvious ("United States" → "US")
    country_code = _normalize_country(country)
    parts = [p for p in (str(city).strip(), str(region).strip()) if p]
    display = ", ".join(parts) if parts else None
    return display, country_code


def _normalize_country(raw: str) -> str | None:
    if not raw:
        return None
    s = raw.strip()
    if len(s) == 2 and s.isalpha():
        return s.upper()
    aliases = {
        "united states": "US", "usa": "US", "u.s.a.": "US", "u.s.": "US",
        "united kingdom": "GB", "uk": "GB", "great britain": "GB",
        "canada": "CA", "australia": "AU", "germany": "DE", "france": "FR",
        "netherlands": "NL", "ireland": "IE", "india": "IN", "singapore": "SG",
    }
    return aliases.get(s.lower())


def _parse_salary(jp: dict[str, Any]) -> tuple[float | None, float | None]:
    """Return (min, max) from baseSalary. Handles both MonetaryAmount
    (with QuantitativeValue) and free-form numbers. Ignores currency
    mismatches for now — a non-USD salary will still flow through; the
    consumer can post-filter by currency if needed."""
    bs = jp.get("baseSalary")
    if not bs:
        return None, None
    if isinstance(bs, list):
        bs = bs[0] if bs else None
    if not isinstance(bs, dict):
        return None, None
    value = bs.get("value")
    if isinstance(value, (int, float)):
        v = float(value)
        return v, v
    if isinstance(value, dict):
        mn = value.get("minValue")
        mx = value.get("maxValue")
        single = value.get("value")
        try:
            mn_f = float(mn) if mn is not None else None
            mx_f = float(mx) if mx is not None else None
            single_f = float(single) if single is not None else None
        except (ValueError, TypeError):
            return None, None
        if mn_f is not None or mx_f is not None:
            return mn_f, mx_f
        if single_f is not None:
            return single_f, single_f
    return None, None


def _parse_date_posted(jp: dict[str, Any]) -> str | None:
    """Return ISO 8601 date string (YYYY-MM-DD). schema.org datePosted is
    typically full ISO timestamp; we trim to the date prefix."""
    dp = jp.get("datePosted")
    if not dp:
        return None
    s = str(dp).strip()
    if not s:
        return None
    # Accept "2026-04-15" or "2026-04-15T12:34:56Z" or "2026-04-15T12:34-07:00"
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def extract_job_posting(html: str) -> dict[str, Any]:
    """Parse schema.org JobPosting from a page's HTML. Returns a dict of
    fields we extracted. Empty dict when no JobPosting block found.

    The returned keys are a subset of: is_remote, work_arrangement,
    location, location_country, salary_min, salary_max, date_posted.
    Every present key has a non-empty value.
    """
    if not html:
        return {}
    blocks = _iter_ld_json_blocks(html)
    if not blocks:
        return {}
    jp: dict[str, Any] | None = None
    for block in blocks:
        jp = _find_job_posting(block)
        if jp is not None:
            break
    if jp is None:
        return {}

    out: dict[str, Any] = {}
    is_remote, arrangement = _parse_remote(jp)
    if is_remote:
        out["is_remote"] = is_remote
    if arrangement:
        out["work_arrangement"] = arrangement
    loc, country = _parse_location(jp)
    if loc:
        out["location"] = loc
    if country:
        out["location_country"] = country
    smin, smax = _parse_salary(jp)
    if smin is not None:
        out["salary_min"] = smin
    if smax is not None:
        out["salary_max"] = smax
    dp = _parse_date_posted(jp)
    if dp:
        out["date_posted"] = dp
    return out


def apply_to_job(job: dict[str, Any], html: str, confidence: float = 0.85) -> int:
    """Parse JobPosting from `html` and emit provenance observations onto
    `job["_field_sources"]` under source='schema_org'. Returns the number of
    fields that produced an observation. Does NOT modify flat values —
    consensus voting (Phase 3) decides what the flat field becomes.
    """
    fields = extract_job_posting(html)
    if not fields:
        return 0
    fs = job.setdefault("_field_sources", {})
    n = 0
    for field, value in fields.items():
        fs.setdefault(field, []).append({
            "source": "schema_org",
            "value": value,
            "confidence": confidence,
        })
        n += 1
    return n
