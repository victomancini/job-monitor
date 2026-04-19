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
from src.processors import deduplicator, enrichment, keyword_filter, lifecycle_checker, llm_classifier, stats_aggregator
from src.processors.category import classify_category
from src.processors.seniority import extract_seniority, infer_seniority_from_salary
from src.processors.vendor_extractor import extract_vendors, vendors_to_str
from src.publishers import archiver, notifier, wordpress
from src.shared import env, validate_required_env
from src.sources import (
    adzuna, ashby, google_alerts, greenhouse, included_ai, jobspy_source,
    jooble, jsearch, lever, onemodel, siop, usajobs,
)
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

def _extract_ats_snapshot(
    jobs: list[dict[str, Any]],
    source_name: str,
    successful_slugs: set[str] | None = None,
) -> dict[tuple[str, str], set[str]]:
    """Group freshly-fetched ATS jobs by (source_name, slug) → set of job_ids.
    Mirrors the external_id format built in src/sources/{greenhouse,lever,ashby}.py.
    Used by the lifecycle checker to resolve ATS-sourced jobs without per-URL
    HEAD requests.

    R4-4 / R5-1: `successful_slugs` is the authoritative whitelist of slugs
    whose board API returned a clean 200 this run.
      - When provided (including `set()`), ONLY those slugs get snapshot
        entries. Jobs from failed/skipped slugs don't populate the map — the
        lifecycle checker then falls through to HEAD requests so a flaky
        board doesn't mass-close every job from that company.
      - When `None`, we fall back to the permissive old behavior (populate
        from the jobs list alone). Only kept for callers that predate R4-4.
        Production collector passes an explicit set.
    """
    prefix_map = {"greenhouse": "gh_", "lever": "lever_", "ashby": "ashby_"}
    prefix = prefix_map.get(source_name)
    if not prefix:
        return {}
    out: dict[tuple[str, str], set[str]] = {}

    if successful_slugs is None:
        # Back-compat path: trust every slug that shows up in the jobs list.
        for job in jobs:
            ext = job.get("external_id") or ""
            if not ext.startswith(prefix):
                continue
            rest = ext[len(prefix):]
            slug, _, jid = rest.rpartition("_")
            if not slug or not jid:
                continue
            out.setdefault((source_name, slug), set()).add(jid)
        return out

    # Whitelist path: seed empty sets for every successfully-fetched slug
    # (authoritative "board is empty"), then add job IDs only for slugs in
    # the whitelist. A job whose slug isn't in `successful_slugs` is ignored
    # here — the lifecycle checker will fall through to HEAD for those.
    for slug in successful_slugs:
        out[(source_name, slug)] = set()
    for job in jobs:
        ext = job.get("external_id") or ""
        if not ext.startswith(prefix):
            continue
        rest = ext[len(prefix):]
        slug, _, jid = rest.rpartition("_")
        if not slug or not jid:
            continue
        if slug not in successful_slugs:
            continue
        out[(source_name, slug)].add(jid)
    return out


def collect_sources(conn=None) -> tuple[list[dict[str, Any]], dict[str, int], list[str], dict[str, Any]]:
    """Run all sources. Each is independently fault-tolerant.
    Returns (all_jobs, per_source_counts, errors, meta).

    `conn` (Phase 2 R3): optional Turso connection — enables ATS slug caching so
    we don't re-fetch known-404 boards on every run.

    `meta["ats_snapshots"]` (R-audit Issue 2c): dict keyed by (ats_name, slug)
    → set of job IDs currently on the board. Consumed by lifecycle_checker so
    Greenhouse/Lever/Ashby jobs resolve via board membership (free) instead of
    per-job HEAD requests.
    """
    all_jobs: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    errors: list[str] = []
    meta: dict[str, Any] = {}
    ats_snapshots: dict[tuple[str, str], set[str]] = {}

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

    # Phase 2 (R3): direct ATS sources — Greenhouse / Lever / Ashby (daily).
    # Capture an authoritative snapshot per ATS so the lifecycle checker can
    # resolve yesterday's jobs by set-membership instead of HEAD-checking each.
    # R4-4: only slugs with a clean 200 go into the snapshot; failed slugs
    # fall through to per-job HEAD checks so a flaky board doesn't mass-close.
    jobs, errs, gh_meta = greenhouse.fetch(conn=conn)
    counts["greenhouse_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)
    ats_snapshots.update(_extract_ats_snapshot(
        jobs, "greenhouse", gh_meta.get("successful_slugs") or set(),
    ))

    jobs, errs, lever_meta = lever.fetch(conn=conn)
    counts["lever_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)
    ats_snapshots.update(_extract_ats_snapshot(
        jobs, "lever", lever_meta.get("successful_slugs") or set(),
    ))

    jobs, errs, ashby_meta = ashby.fetch(conn=conn)
    counts["ashby_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)
    ats_snapshots.update(_extract_ats_snapshot(
        jobs, "ashby", ashby_meta.get("successful_slugs") or set(),
    ))

    # Phase 3 (R3): JobSpy (LinkedIn / Indeed / Glassdoor / ZipRecruiter)
    jobs, errs, jm = jobspy_source.fetch()
    counts["jobspy_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)
    if jm.get("available") is False:
        meta["jobspy_unavailable"] = True

    # Phase 4 (R3): niche PA boards (One Model / Included.ai / SIOP)
    jobs, errs, _ = onemodel.fetch(conn=conn)
    counts["onemodel_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)

    jobs, errs, _ = included_ai.fetch(conn=conn)
    counts["included_ai_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)

    jobs, errs, _ = siop.fetch(conn=conn)
    counts["siop_found"] = len(jobs)
    all_jobs.extend(jobs)
    errors.extend(errs)

    meta["ats_snapshots"] = ats_snapshots
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


def apply_vendor_mentions(jobs: list[dict[str, Any]]) -> None:
    """Phase 5 (R3): extract tool/vendor mentions from each job's description
    and store as a comma-separated string."""
    for job in jobs:
        job["vendors_mentioned"] = vendors_to_str(extract_vendors(job.get("description", "")))


def apply_enrichment(jobs: list[dict[str, Any]]) -> dict[str, int]:
    """Fetch source/apply URLs and extract salary/remote/location. Returns stats dict."""
    if not jobs:
        return {"enriched_from_source": 0, "aggregator_only": 0}
    enrichment.enrich_batch(jobs)
    return {
        "enriched_from_source": sum(1 for j in jobs if j.get("enrichment_source") == "source_page"),
        "aggregator_only": sum(1 for j in jobs if j.get("enrichment_source") == "aggregator"),
    }


def apply_defaults(jobs: list[dict[str, Any]]) -> None:
    """Phase 1 (R3) / Phase J fallback: if is_remote is still unknown after all
    extraction passes, default to 'onsite' with confidence='assumed'."""
    for job in jobs:
        if job.get("is_remote") in (None, "", "unknown"):
            job["is_remote"] = "onsite"
            job["remote_confidence"] = "assumed"


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

ERROR_LIST_CAP = 50


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
        # Truncate to keep ping body small, but signal when it happened so the
        # receiving end knows whether len(errors) == total_errors.
        "errors": errors[:ERROR_LIST_CAP],
        "total_errors": len(errors),
        "errors_truncated": len(errors) > ERROR_LIST_CAP,
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
    all_jobs, source_counts, errors, source_meta = collect_sources(conn=conn)
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
    # 5d. Vendor/tool mentions (Phase 5 R3)
    apply_vendor_mentions(to_publish)

    # 6. Deduplicator (batch + DB)
    log.info("=== Phase 4: deduplicator ===")
    active_rows = db.get_active_jobs_for_dedup(conn)
    to_publish, skipped_dupes = deduplicator.deduplicate(to_publish, active_db_rows=active_rows)
    log.info("deduplicator: %d kept / %d skipped as dupes", len(to_publish), len(skipped_dupes))

    # 6a. Apply apply_url upgrades for dropped-as-dupe jobs whose direct URL
    # beats the DB row's aggregator URL (R-audit Issue 1d).
    #
    # R4-7: alongside the DB update, collect a targeted WP payload for each
    # upgraded row so the existing WP post also gets the fresh apply_url this
    # run — otherwise the visible table stays stale until the aggregator drops
    # the job and a new scrape re-publishes it.
    url_upgrade_pushes: list[dict[str, Any]] = []
    for dupe in skipped_dupes:
        upgrade = dupe.get("_apply_url_upgrade")
        if not upgrade:
            continue
        try:
            db.upgrade_apply_url(conn, upgrade["external_id"], upgrade["apply_url"])
            log.info("apply_url upgrade: %s -> %s",
                     upgrade["external_id"], upgrade["apply_url"])
            row = db.get_row_for_wp_push(conn, upgrade["external_id"])
            if row and row.get("title"):
                url_upgrade_pushes.append(row)
        except Exception as e:  # noqa: BLE001
            errors.append(f"db.upgrade_apply_url {upgrade['external_id']}: {e}")

    # 6.5. Enrichment — fetch source pages to confirm salary/remote/location
    log.info("=== Phase 4.5: enrichment ===")
    enrichment_stats = apply_enrichment(to_publish)
    log.info("enrichment: %s", enrichment_stats)
    # Phase 1 (R3): apply assumed defaults after all extraction passes
    apply_defaults(to_publish)

    # Upsert everything into Turso (even dry-run — local state tracking)
    for job in to_publish:
        try:
            db.upsert_job(conn, job)
        except Exception as e:  # noqa: BLE001
            errors.append(f"db.upsert_job {job.get('external_id')}: {e}")

    published = 0
    retry_dropped = 0
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
        retry_dropped = retry_result.get("dropped", 0)

        pub_result = wordpress.publish(
            to_publish,
            wp_url=env("WP_URL"),
            username=env("WP_USERNAME"),
            app_password=env("WP_APP_PASSWORD"),
            conn=conn,
        )
        log.info("publish: %s", pub_result)
        published = pub_result["created"] + pub_result["updated"]

        # R4-7: push targeted apply_url upgrades for DB rows that got promoted
        # during dedup. These posts aren't in to_publish (they dedup-collapsed),
        # so without this push WP would keep showing the old aggregator URL
        # until the next time the job is re-fetched and re-published.
        #
        # R5-16: only push when the main batch went through cleanly. If WP is
        # down (pub_result["queued"] > 0), the URL-upgrade push would also
        # queue and we'd end up with retry-queue rows whose only purpose is a
        # single field update. The DB upgrade is already persisted; the fresh
        # URL will flow to WP on the next successful publish of that job.
        main_publish_healthy = (
            pub_result.get("queued", 0) == 0 and pub_result.get("batches", 0) > 0
        )
        if url_upgrade_pushes and main_publish_healthy:
            upg_result = wordpress.publish(
                url_upgrade_pushes,
                wp_url=env("WP_URL"),
                username=env("WP_USERNAME"),
                app_password=env("WP_APP_PASSWORD"),
                conn=conn,
            )
            log.info("apply_url upgrade WP push: %s (count=%d)",
                     upg_result, len(url_upgrade_pushes))
        elif url_upgrade_pushes:
            log.info(
                "apply_url upgrade WP push skipped (main publish degraded): "
                "%d pending upgrades will flow on next successful publish",
                len(url_upgrade_pushes),
            )

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

    # 8.5. Lifecycle checker (R-audit Issue 2d): verify ACTIVE jobs are still
    # open via source-of-truth (ATS board membership or company-page HEAD).
    # Runs before archiver so ATS-confirmed-closed jobs can be archived
    # immediately in the fast-path instead of waiting 21 days.
    log.info("=== Phase 6.5: lifecycle checker ===")
    try:
        lifecycle_stats = lifecycle_checker.check_lifecycle_batch(
            conn,
            ats_snapshots=source_meta.get("ats_snapshots"),
        )
        log.info("lifecycle_checker: %s", lifecycle_stats)
    except Exception as e:  # noqa: BLE001 — never block the archiver/healthcheck
        log.warning("lifecycle_checker failed: %s", e)
        lifecycle_stats = {"error": str(e)}

    # 9. Archiver (always runs — doesn't touch WP in dry-run)
    log.info("=== Phase 7: archiver ===")
    arch_result = archiver.archive_stale(conn)
    log.info("archiver: %s", arch_result)

    # 10. Log run + healthcheck ping
    duration = time.monotonic() - started
    # run_log only has a fixed set of source-count columns; keep the historical
    # columns, and stash Phase 2 (R3) ATS counts in the healthcheck meta only.
    db.log_run(conn, {
        "run_date": _today(),
        "jsearch_found": source_counts.get("jsearch_found", 0),
        "jooble_found": source_counts.get("jooble_found", 0),
        "adzuna_found": source_counts.get("adzuna_found", 0),
        "usajobs_found": source_counts.get("usajobs_found", 0),
        "alerts_found": source_counts.get("alerts_found", 0),
        "total_passed_filter": len(candidates),
        "total_published": published,
        "total_archived": arch_result["archived"],
        "errors": "\n".join(errors[:20]),
        "llm_provider_used": ",".join(provider_counts.keys()) or "none",
        "duration_seconds": round(duration, 1),
        "consecutive_zero_runs": consecutive_zero,
    })

    # Phase 7 (R3): aggregate today's slice into monthly_stats before pinging
    try:
        stats_agg = stats_aggregator.aggregate_daily_stats(conn)
        log.info("stats_aggregator: %s", stats_agg)
    except Exception as e:  # noqa: BLE001
        log.warning("stats_aggregator failed: %s", e)

    # Phase 8 (R3): push dashboard payload to WordPress (skipped in dry-run)
    if not dry_run:
        try:
            payload = stats_aggregator.build_dashboard_payload(conn)
            dash_result = wordpress.publish_dashboard_stats(
                payload,
                wp_url=env("WP_URL"),
                username=env("WP_USERNAME"),
                app_password=env("WP_APP_PASSWORD"),
            )
            log.info("dashboard_stats: %s", dash_result)
        except Exception as e:  # noqa: BLE001
            log.warning("dashboard_stats publish failed: %s", e)

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
        meta={
            # Drop ats_snapshots from source_meta — it's a large dict of sets,
            # not JSON-friendly and not useful in the healthcheck body.
            **{k: v for k, v in source_meta.items() if k != "ats_snapshots"},
            **enrichment_stats,
            "lifecycle": lifecycle_stats,
            "retry_queue_dropped": retry_dropped,
        },
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
