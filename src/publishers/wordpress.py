"""WordPress batch publisher. Posts jobs to custom /wp-json/jobmonitor/v1/update-jobs
endpoint in chunks of 20. On transport failure, pushes jobs onto Turso retry_queue.
Writes returned wp_post_id back onto the job dict and into the jobs table.
"""
from __future__ import annotations

import base64
import logging
import time
from typing import Any

from src import db
from src.sources._http import retry_request

log = logging.getLogger(__name__)

BATCH_SIZE = 20
BETWEEN_BATCHES_SEC = 1.0

# Subset of job dict sent to WordPress — we don't dump raw_data or LLM internals
_WP_FIELDS = [
    "external_id", "title", "company", "location", "location_country",
    "salary_min", "salary_max", "salary_range", "source_url", "apply_url",
    "source_name", "is_remote", "work_arrangement", "description", "description_snippet",
    "keyword_score", "keywords_matched", "llm_classification", "llm_confidence",
    "llm_provider", "llm_reasoning", "fit_score", "category", "seniority",
    # Phase F6: enrichment + confidence fields
    "location_confidence", "salary_confidence", "remote_confidence",
    "enrichment_source", "enrichment_date",
    # Phase B (R2): freshness display + ordering in WP table
    "date_posted",
    # Phase F (R2): seniority confidence badge
    "seniority_confidence",
    # Phase 5 (R3): comma-separated vendor/tool mentions
    "vendors_mentioned",
    # Phase 6 (R3): lifecycle state — 'active' | 'likely_closed'
    "lifecycle_status",
]


def _auth_header(username: str, app_password: str) -> str:
    token = base64.b64encode(f"{username}:{app_password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _build_headers(username: str, app_password: str) -> dict[str, str]:
    """Assemble WP REST headers. Adds X-JM-Secret when WP_SHARED_SECRET is set,
    matching the optional JM_SHARED_SECRET gate on the plugin side."""
    import os
    headers = {
        "Authorization": _auth_header(username, app_password),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    secret = (os.environ.get("WP_SHARED_SECRET") or "").strip()
    if secret:
        headers["X-JM-Secret"] = secret
    return headers


def _payload(job: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for k in _WP_FIELDS:
        v = job.get(k)
        if v is None:
            continue
        if isinstance(v, (int, float)):
            out[k] = v
        else:
            out[k] = str(v)
    return out


def _post_batch(
    url: str,
    headers: dict[str, str],
    jobs: list[dict[str, Any]],
) -> tuple[dict[str, Any], bool]:
    """Returns (response_json_or_empty, success_bool). Non-2xx or exception → (empty, False)."""
    body = {"jobs": [_payload(j) for j in jobs]}
    try:
        resp = retry_request("POST", url, headers=headers, json=body)
    except Exception as e:  # noqa: BLE001
        log.warning("wordpress: transport error on batch: %s", e)
        return {}, False
    if resp.status_code != 200:
        log.warning("wordpress: HTTP %s on batch: %s", resp.status_code, resp.text[:300])
        return {}, False
    try:
        return resp.json(), True
    except Exception:  # noqa: BLE001
        log.warning("wordpress: non-JSON response")
        return {}, False


def publish(
    jobs: list[dict[str, Any]],
    *,
    wp_url: str,
    username: str,
    app_password: str,
    conn=None,
) -> dict[str, Any]:
    """Publish `jobs` in batches. Writes wp_post_id back to Turso if `conn` is provided.
    Jobs in failed batches are enqueued to retry_queue (requires `conn`).
    Returns {'created', 'updated', 'errors', 'queued', 'batches'}.
    """
    if not jobs:
        return {"created": 0, "updated": 0, "errors": 0, "queued": 0, "batches": 0}
    if not wp_url or not username or not app_password:
        log.warning("wordpress: credentials incomplete — queueing all %d jobs", len(jobs))
        queued = _enqueue_all(conn, jobs)
        return {"created": 0, "updated": 0, "errors": 0, "queued": queued, "batches": 0}

    endpoint = wp_url.rstrip("/") + "/wp-json/jobmonitor/v1/update-jobs"
    headers = _build_headers(username, app_password)

    totals = {"created": 0, "updated": 0, "errors": 0, "queued": 0, "batches": 0}
    for batch_idx, i in enumerate(range(0, len(jobs), BATCH_SIZE)):
        batch = jobs[i : i + BATCH_SIZE]
        data, ok = _post_batch(endpoint, headers, batch)
        totals["batches"] += 1
        if not ok:
            totals["queued"] += _enqueue_all(conn, batch)
            continue
        totals["created"] += int(data.get("created", 0) or 0)
        totals["updated"] += int(data.get("updated", 0) or 0)
        totals["errors"] += int(data.get("errors", 0) or 0)
        for ext_id, wp_pid in (data.get("post_ids") or {}).items():
            try:
                pid = int(wp_pid)
            except (TypeError, ValueError):
                continue
            for j in batch:
                if j.get("external_id") == ext_id:
                    j["wp_post_id"] = pid
                    break
            if conn is not None:
                try:
                    db.set_wp_post_id(conn, ext_id, pid)
                except Exception as e:  # noqa: BLE001
                    log.warning("wordpress: failed storing wp_post_id for %s: %s", ext_id, e)
        if i + BATCH_SIZE < len(jobs):
            time.sleep(BETWEEN_BATCHES_SEC)

    return totals


def _enqueue_all(conn, jobs: list[dict[str, Any]]) -> int:
    """Enqueue the FULL job dict (not the _payload-truncated form) so retries
    can rebuild the WP payload fresh at send time. Previous behavior stored
    the cast-to-string payload, which meant retries days later published with
    yesterday's `last_seen_date` / LLM verdict baked in. Fields that can't be
    JSON-serialized (rare — e.g., datetime) are coerced to str."""
    if conn is None:
        return 0
    n = 0
    for j in jobs:
        try:
            db.enqueue_retry(conn, _jsonable(j))
            n += 1
        except Exception as e:  # noqa: BLE001
            log.warning("wordpress: retry_queue insert failed: %s", e)
    return n


def _jsonable(job: dict[str, Any]) -> dict[str, Any]:
    """Shallow-coerce a job dict so json.dumps() never raises. Preserves ints,
    floats, strs, bools, None; str()s everything else (dates, decimals, etc.)."""
    out: dict[str, Any] = {}
    for k, v in job.items():
        if v is None or isinstance(v, (bool, int, float, str)):
            out[k] = v
        else:
            out[k] = str(v)
    return out


def publish_dashboard_stats(
    stats: dict[str, Any],
    *,
    wp_url: str,
    username: str,
    app_password: str,
) -> dict[str, Any]:
    """Phase 8 (R3): POST aggregated stats to the WP dashboard REST endpoint.
    Returns {'ok': bool, 'status': int|None} — never raises."""
    if not wp_url or not username or not app_password:
        return {"ok": False, "status": None, "reason": "credentials incomplete"}
    endpoint = wp_url.rstrip("/") + "/wp-json/jobmonitor/v1/dashboard-stats"
    headers = _build_headers(username, app_password)
    try:
        resp = retry_request("POST", endpoint, headers=headers, json=stats, max_attempts=2)
    except Exception as e:  # noqa: BLE001
        log.warning("wordpress: dashboard-stats transport error: %s", e)
        return {"ok": False, "status": None, "reason": str(e)}
    return {"ok": 200 <= resp.status_code < 300, "status": resp.status_code}


def process_retry_queue(
    conn,
    *,
    wp_url: str,
    username: str,
    app_password: str,
) -> dict[str, Any]:
    """Pull pending retries, attempt publish. Mark success/failure per job.
    After 3 total failed attempts a job is dropped permanently.
    """
    pending = db.fetch_retry_queue(conn, max_attempts=3)
    if not pending:
        dropped = db.drop_exhausted_retries(conn)
        if dropped:
            log.warning("wordpress: dropped %d permanently-failed job(s) from retry queue", dropped)
        return {"attempted": 0, "succeeded": 0, "failed": 0, "dropped": dropped}

    endpoint = wp_url.rstrip("/") + "/wp-json/jobmonitor/v1/update-jobs"
    headers = _build_headers(username, app_password)

    succeeded = 0
    failed = 0
    # Process in batches of BATCH_SIZE but track per-row for queue lifecycle
    for i in range(0, len(pending), BATCH_SIZE):
        chunk = pending[i : i + BATCH_SIZE]
        data, ok = _post_batch(endpoint, headers, [j for _, j in chunk])
        if ok:
            post_ids = data.get("post_ids") or {}
            for retry_id, job in chunk:
                ext_id = job.get("external_id")
                if ext_id in post_ids:
                    try:
                        db.set_wp_post_id(conn, ext_id, int(post_ids[ext_id]))
                    except (TypeError, ValueError):
                        pass
                db.mark_retry_success(conn, retry_id)
                succeeded += 1
        else:
            for retry_id, _ in chunk:
                db.mark_retry_failure(conn, retry_id)
                failed += 1
        if i + BATCH_SIZE < len(pending):
            time.sleep(BETWEEN_BATCHES_SEC)

    dropped = db.drop_exhausted_retries(conn)
    if dropped:
        log.warning("wordpress: dropped %d permanently-failed job(s) from retry queue", dropped)
    return {
        "attempted": len(pending),
        "succeeded": succeeded,
        "failed": failed,
        "dropped": dropped,
    }
