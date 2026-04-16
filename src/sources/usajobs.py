"""USAJobs API — federal government roles (weekly). Auth: Authorization-Key + User-Agent."""
from __future__ import annotations

import logging
import time
from typing import Any

from src.shared import build_job, load_queries
from src.sources._http import retry_request

log = logging.getLogger(__name__)

BASE_URL = "https://data.usajobs.gov/api/search"


def _extract_salary(item: dict[str, Any]) -> tuple[float | None, float | None]:
    ranges = item.get("PositionRemuneration") or []
    if not ranges:
        return None, None
    try:
        mins = [float(r.get("MinimumRange")) for r in ranges if r.get("MinimumRange")]
        maxs = [float(r.get("MaximumRange")) for r in ranges if r.get("MaximumRange")]
    except (TypeError, ValueError):
        return None, None
    return (min(mins) if mins else None, max(maxs) if maxs else None)


def _remote_flag(item: dict[str, Any]) -> str:
    tele = item.get("TeleworkEligible")
    remote = item.get("RemoteIndicator")
    if remote is True or (isinstance(remote, str) and remote.lower() == "true"):
        return "remote"
    if tele is True or (isinstance(tele, str) and tele.lower() == "true"):
        return "hybrid"
    return "onsite"


def _map(item: dict[str, Any]) -> dict[str, Any] | None:
    descriptor = item.get("MatchedObjectDescriptor") or item
    mid = item.get("MatchedObjectId") or descriptor.get("PositionID")
    if not mid:
        return None
    title = descriptor.get("PositionTitle") or ""
    org = descriptor.get("OrganizationName") or descriptor.get("DepartmentName") or ""
    if not title:
        return None
    loc_list = descriptor.get("PositionLocation") or []
    location = loc_list[0].get("LocationName") if loc_list else descriptor.get("PositionLocationDisplay", "")
    salary_min, salary_max = _extract_salary(descriptor)
    desc = descriptor.get("QualificationSummary") or descriptor.get("UserArea", {}).get("Details", {}).get("JobSummary", "")
    date_posted = descriptor.get("PublicationStartDate")
    return build_job(
        source_name="usajobs",
        external_id=f"usajobs_{mid}",
        title=title,
        company=org,
        location=location or "",
        location_country="US",
        description=desc or "",
        salary_min=salary_min,
        salary_max=salary_max,
        source_url=descriptor.get("PositionURI") or "",
        is_remote=_remote_flag(descriptor),
        date_posted=date_posted,
        raw_data=item,
    )


def fetch(email: str, api_key: str, keywords: list[str] | None = None) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    if not email or not api_key:
        return [], ["usajobs: credentials not set"], {}
    cfg = load_queries()["usajobs"]
    keywords = keywords if keywords is not None else cfg["keywords"]
    params_base = cfg.get("params", {})
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": email,
        "Authorization-Key": api_key,
        "Accept": "application/json",
    }
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for i, kw in enumerate(keywords):
        params = {**params_base, "Keyword": kw}
        try:
            resp = retry_request("GET", BASE_URL, headers=headers, params=params)
            if resp.status_code != 200:
                errors.append(f"usajobs: HTTP {resp.status_code} on '{kw}'")
                continue
            data = resp.json()
            items = (data.get("SearchResult") or {}).get("SearchResultItems") or []
            for item in items:
                try:
                    job = _map(item)
                    if job:
                        results.append(job)
                except Exception as e:  # noqa: BLE001
                    errors.append(f"usajobs: map error: {e}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"usajobs: query error '{kw}': {e}")
        if i < len(keywords) - 1:
            time.sleep(1.0)
    return results, errors, {}
