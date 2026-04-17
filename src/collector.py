"""Orchestrator: sources -> keyword_filter -> llm_classifier -> deduplicator ->
wordpress publisher -> notifier -> archiver -> healthcheck ping.

Pipeline ordering is a HARD CONSTRAINT (CLAUDE.md). Do not reorder.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

# Support both `python src/collector.py` and `python -m src.collector`
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import db
from src.processors import deduplicator, enrichment, keyword_filter, llm_classifier
from src.processors.category import classify_category
from src.processors.seniority import extract_seniority, infer_seniority_from_salary
from src.publishers import archiver, notifier, wordpress
from src.shared import env, validate_required_env
from src.sources import adzuna, google_alerts, jooble, jsearch, usajobs
from src.sources._http import retry_request

log = logging.getLogger("collector")

SHADOW_LOG_PATH = Path(__file__).resolve().parent.parent / "shadow_log.jsonl"
ZERO_RUN_ALERT_THRESHOLD = 2


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _is_monday() -> bool:
    return datetime.now(timezone.utc).weekday() == 0


def _shadow_log(event: dict[str, Any]) -> None:
    try:
        with SHADOW_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"t": _today(), **event}, default=str) + "\n")
    except Exception as e:  # noqa: BLE001 — shadow logging must never fail the pipeline
        log.warning("shadow_log write failed: %s", e)


# ──────────────────────────── Source collection ──────────────────────

def collect_sources() -> tuple[list[dict[str, Any]], dict[str, int], list[str], dict[str, Any]]:
    """Run all sources. Each is independently fault-tolerant.
    Returns (all_jobs, per_source_counts, errors, meta).
    """
    all_jobs: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    errors: list[str] = []
    meta: dict[str, Any] = {}

    # JSearch (daily)
    jobs, errs, m = jsearch.fetch(env("JSEARCH_API_KEY"))
    counts["jsearch_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)
    if m.get("quota_remaining") is not None:
        meta["jsearch_quota_remaining"] = m["quota_remaining"]

    # Jooble (daily)
    jobs, errs, _ = jooble.fetch(env("JOOBLE_API_KEY"))
    counts["jooble_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)

    # Adzuna (daily, optional)
    jobs, errs, _ = adzuna.fetch(env("ADZUNA_APP_ID"), env("ADZUNA_APP_KEY"))
    counts["adzuna_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)

    # USAJobs (MONDAYS ONLY, optional)
    if _is_monday():
        jobs, errs, _ = usajobs.fetch(env("USAJOBS_EMAIL"), env("USAJOBS_API_KEY"))
        counts["usajobs_found"] = len(jobs)
        all_jobs.extend(jobs)
        errors.extend(errs)
    else:
        counts["usajobs_found"] = 0
        meta["usajobs_skipped_not_monday"] = True

    # Google Alerts + Talkwalker + SIOP RSS (daily)
    jobs, errs, m = google_alerts.fetch()
    counts["alerts_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)
    if m.get("stale_feeds"):
        meta["stale_feeds"] = m["stale_feeds"]

    return all_jobs, counts, errors, meta


# ──────────────────────────── Filtering + LLM ────────────────────────

def apply_keyword_filter(jobs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Returns (candidates_for_llm_or_publish, rejects). Every job is mutated with keyword fields.
    Rejects are logged to shadow_log for retrospective calibration."""
    candidates: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    for job in jobs:
        try:
            decision = keyword_filter.classify(job)
        except Exception as e:  # noqa: BLE001 — one bad record can't kill the batch
            log.warning("keyword_filter error on %s: %s", job.get("external_id"), e)
            continue
        _shadow_log({
            "stage": "keyword_filter",
            "external_id": job.get("external_id"),
            "title": job.get("title"),
            "company": job.get("company"),
            "score": decision["score"],
            "decision": decision["decision"],
            "matched": decision["matched"],
        })
        if decision["decision"] in ("auto_reject", "low_score"):
            rejects.append(job)
        else:
            candidates.append(job)
    return candidates, rejects


def apply_seniority(jobs: list[dict[str, Any]]) -> None:
    """Extract seniority from title (regex). LLM answer overrides if provided.
    If both fall through as Unknown, infer from salary_min as a last resort and mark
    seniority_confidence='inferred' so the UI can show it as approximate."""
    for job in jobs:
        job["seniority"] = extract_seniority(job.get("title", ""))
        llm_hint = job.get("_llm_seniority")
        if llm_hint and llm_hint != "Unknown":
            job["seniority"] = llm_hint
        # Phase F (R2): salary-based fallback when title/LLM are both Unknown
        if job["seniority"] == "Unknown":
            inferred = infer_seniority_from_salary(job.get("salary_min"))
            if inferred:
                job["seniority"] = inferred
                job["seniority_confidence"] = "inferred"


def apply_category(jobs: list[dict[str, Any]]) -> None:
    """Phase I (R2): assign each job a functional category for UI filtering."""
    for job in jobs:
        job["category"] = classify_category(
            job.get("title", ""),
            job.get("company", ""),
            job.get("description", ""),
        )


def apply_enrichment(jobs: list[dict[str, Any]]) -> dict[str, int]:
    """Fetch source/apply URLs and extract salary/remote/location. Returns stats dict."""
    if not jobs:
        return {"enriched_from_source": 0, "aggregator_only": 0}
    enrichment.enrich_batch(jobs)
    return {
        "enriched_from_source": sum(1 for j in jobs if j.get("enrichment_source") == "source_page"),
        "aggregator_only": sum(1 for j in jobs if j.get("enrichment_source") == "aggregator"),
    }


def apply_llm(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int], list[str]]:
    """Run LLM chain on everything that passed keyword filter. Returns (to_publish, provider_counts, errors)."""
    if not candidates:
        return [], {}, []
    errors, counts = llm_classifier.classify_batch(
        candidates,
        groq_key=env("GROQ_API_KEY"),
        gemini_key=env("GEMINI_API_KEY"),
        openai_key=env("OPENAI_API_KEY"),
    )
    to_publish: list[dict[str, Any]] = []
    for job in candidates:
        decision = llm_classifier.publish_decision(job)
        _shadow_log({
            "stage": "llm_classifier",
            "external_id": job.get("external_id"),
            "title": job.get("title"),
            "company": job.get("company"),
            "classification": job.get("llm_classification"),
            "confidence": job.get("llm_confidence"),
            "provider": job.get("llm_provider"),
            "publish_decision": decision,
        })
        if decision in ("publish", "publish_flag"):
            to_publish.append(job)
    return to_publish, counts, errors


# ──────────────────────────── Healthchecks ping ──────────────────────

def ping_healthcheck(
    url: str,
    *,
    success: bool,
    counts: dict[str, int],
    errors: list[str],
    published: int,
    archived: int,
    duration_s: float,
    provider_counts: dict[str, int],
    meta: dict[str, Any],
) -> None:
    if not url:
        return
    endpoint = url.rstrip("/") + ("" if success else "/fail")
    body = {
        "run_date": _today(),
        **counts,
        "total_published": published,
        "total_archived": archived,
        "errors": errors[:50],  # cap
        "duration_seconds": round(duration_s, 1),
        "llm_providers": provider_counts,
        **meta,
    }
    try:
        retry_request("POST", endpoint, json=body, timeout=10.0, max_attempts=2)
    except Exception as e:  # noqa: BLE001
        log.warning("healthcheck ping failed: %s", e)


# ──────────────────────────── Zero-results canary ────────────────────

def _alert_zero_results(consecutive: int) -> None:
    """Consecutive zero-result runs — send Pushover alert + healthcheck /fail is already sent by caller."""
    user = env("PUSHOVER_USER_KEY")
    token = env("PUSHOVER_APP_TOKEN")
    if not user or not token:
        log.warning("zero-results canary: %d consecutive, but Pushover not configured", consecutive)
        return
    try:
        retry_request("POST", notifier.PUSHOVER_URL, data={
            "token": token,
            "user": user,
            "title": "Job Monitor: ALL SOURCES RETURNED ZERO",
            "message": f"{consecutive} consecutive zero-result runs. Check API credentials & quotas.",
            "priority": 1,
        }, timeout=10.0, max_attempts=2)
    except Exception as e:  # noqa: BLE001
        log.warning("zero-results canary pushover failed: %s", e)


# ──────────────────────────── Main pipeline ──────────────────────────

def run(dry_run: bool = False) -> int:
    """Main entry point. Returns process exit code (0 ok, 1 pre-flight failure)."""
    started = time.monotonic()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    # 1. Pre-flight env validation BEFORE any API calls
    missing = validate_required_env()
    if missing:
        log.error("Missing required env vars: %s", ", ".join(missing))
        return 1

    # 2. Connect + migrate
    conn = db.connect()
    db.migrate(conn)

    # 3. Sources
    log.info("=== Phase 1: sources ===")
    all_jobs, source_counts, errors, source_meta = collect_sources()
    log.info("sources: %s", source_counts)

    # Zero-results canary
    total_found = sum(source_counts.values())
    consecutive_zero = 0
    if total_found == 0:
        consecutive_zero = db.get_consecutive_zero_runs(conn) + 1
        log.warning("ZERO results from all sources. consecutive=%d", consecutive_zero)
        if consecutive_zero >= ZERO_RUN_ALERT_THRESHOLD:
            _alert_zero_results(consecutive_zero)

    # 4. Keyword filter
    log.info("=== Phase 2: keyword filter ===")
    candidates, rejects = apply_keyword_filter(all_jobs)
    log.info("keyword_filter: %d candidates / %d rejects", len(candidates), len(rejects))

    # 5. LLM classifier
    log.info("=== Phase 3: LLM classifier ===")
    to_publish, provider_counts, llm_errors = apply_llm(candidates)
    errors.extend(llm_errors)
    log.info("llm: %d to publish / providers=%s", len(to_publish), provider_counts)

    # 5b. Seniority extraction (regex first, LLM may override via _llm_seniority hint)
    apply_seniority(to_publish)
    # 5c. Category classification (Phase I R2) for UI filtering
    apply_category(to_publish)

    # 6. Deduplicator (batch + DB)
    log.info("=== Phase 4: deduplicator ===")
    active_rows = db.get_active_jobs_for_dedup(conn)
    to_publish, skipped_dupes = deduplicator.deduplicate(to_publish, active_db_rows=active_rows)
    log.info("deduplicator: %d kept / %d skipped as dupes", len(to_publish), len(skipped_dupes))

    # 6.5. Enrichment — fetch source pages to confirm salary/remote/location
    log.info("=== Phase 4.5: enrichment ===")
    enrichment_stats = apply_enrichment(to_publish)
    log.info("enrichment: %s", enrichment_stats)

    # Upsert everything into Turso (even dry-run — local state tracking)
    for job in to_publish:
        try:
            db.upsert_job(conn, job)
        except Exception as e:  # noqa: BLE001
            errors.append(f"db.upsert_job {job.get('external_id')}: {e}")

    published = 0
    if dry_run:
        log.info("DRY-RUN: skipping WordPress publish, notifier, archiver")
    else:
        # 7. WordPress publisher (processes retry_queue first, then new jobs)
        log.info("=== Phase 5: WordPress publish ===")
        retry_result = wordpress.process_retry_queue(
            conn,
            wp_url=env("WP_URL"),
            username=env("WP_USERNAME"),
            app_password=env("WP_APP_PASSWORD"),
        )
        log.info("retry_queue: %s", retry_result)

        pub_result = wordpress.publish(
            to_publish,
            wp_url=env("WP_URL"),
            username=env("WP_USERNAME"),
            app_password=env("WP_APP_PASSWORD"),
            conn=conn,
        )
        log.info("publish: %s", pub_result)
        published = pub_result["created"] + pub_result["updated"]

        # 8. Notifier
        log.info("=== Phase 6: notifier ===")
        notify_result = notifier.notify(
            to_publish,
            pushover_user=env("PUSHOVER_USER_KEY"),
            pushover_token=env("PUSHOVER_APP_TOKEN"),
            brevo_user=env("BREVO_SMTP_USER"),
            brevo_pass=env("BREVO_SMTP_PASS"),
            email_to=env("NOTIFICATION_EMAIL"),
        )
        log.info("notifier: %s", notify_result)

    # 9. Archiver (always runs — doesn't touch WP in dry-run)
    log.info("=== Phase 7: archiver ===")
    arch_result = archiver.archive_stale(conn)
    log.info("archiver: %s", arch_result)

    # 10. Log run + healthcheck ping
    duration = time.monotonic() - started
    db.log_run(conn, {
        "run_date": _today(),
        **source_counts,
        "total_passed_filter": len(candidates),
        "total_published": published,
        "total_archived": arch_result["archived"],
        "errors": "\n".join(errors[:20]),
        "llm_provider_used": ",".join(provider_counts.keys()) or "none",
        "duration_seconds": round(duration, 1),
        "consecutive_zero_runs": consecutive_zero,
    })

    canary_tripped = consecutive_zero >= ZERO_RUN_ALERT_THRESHOLD
    log.info("=== Phase 8: healthcheck ping ===")
    ping_healthcheck(
        env("HEALTHCHECK_URL"),
        success=not canary_tripped,
        counts=source_counts,
        errors=errors,
        published=published,
        archived=arch_result["archived"],
        duration_s=duration,
        provider_counts=provider_counts,
        meta={**source_meta, **enrichment_stats},
    )
    log.info("=== DONE in %.1fs ===", duration)
    # Non-zero exit when canary tripped so the workflow-level ping also fails
    return 1 if canary_tripped else 0


def _cli() -> int:
    parser = argparse.ArgumentParser(description="Job monitor collector")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + filter + classify. Skip WordPress publish and notifications.")
    args = parser.parse_args()
    return run(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(_cli())
