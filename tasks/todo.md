# Job Monitor Build Checklist

> **v3 Final (April 2026) — three brutal assessments incorporated:**
> US-primary focus. 5 API sources (JSearch, Jooble, Adzuna, USAJobs, RSS alerts).
> 4-tier LLM (Groq → Gemini 2.5 Flash-Lite → GPT-4o-mini → keyword-only).
> Word-boundary regex. Vendor-aware LLM prompt with few-shot examples.
> Self-hosted DataTables. WP transient caching + bulk insert optimization.
> Zero-results canary. Retry queue. Concurrency control. Keepalive workflow.

## Phase 1: Database and Core Infrastructure
- [ ] Create Turso schema (jobs, run_log, retry_queue tables)
- [ ] Write src/db.py utility (connect, migrate, query helpers)
- [ ] Ensure indexes: external_id, company_normalized, last_seen_date
- [ ] Test Turso connection with write/read/delete cycle
- [ ] Create shared types (standardized job dict, YAML config loader)

## Phase 2: Data Sources
- [ ] Build src/sources/jsearch.py (US-primary, num_pages=1, quota tracking)
- [ ] Test: one real query, verify quota header reading
- [ ] Build src/sources/jooble.py (US-primary + bonus UK/CA/AU)
- [ ] Test: one real query
- [ ] Build src/sources/adzuna.py (US endpoint + bonus gb/ca/au)
- [ ] Test: one real query
- [ ] Build src/sources/usajobs.py (weekly, User-Agent auth)
- [ ] Test: one real query for "people analytics"
- [ ] Build src/sources/google_alerts.py (Google + Talkwalker + SIOP RSS)
- [ ] Test: one RSS feed URL
- [ ] Verify all 5 sources return standardized job dicts
- [ ] Verify each source is independently fault-tolerant

## Phase 3: Processors
- [ ] Build src/processors/keyword_filter.py (word-boundary regex)
- [ ] Test: 20 known-good titles score >= 50
- [ ] Test: 20 known-bad titles score < 10
- [ ] Test: "active listening" in title → rejected
- [ ] Test: positive + negative conflict → routes to LLM
- [ ] Build src/processors/llm_classifier.py (4-tier: Groq → Gemini → GPT-4o-mini → keyword)
- [ ] Test: Groq classification with 5 titles
- [ ] Test: simulate Groq 429 → Gemini fallback works
- [ ] Test: simulate both fail → GPT-4o-mini works
- [ ] Test: simulate all 3 fail → keyword-only fallback works
- [ ] Build src/processors/deduplicator.py (RapidFuzz with processor arg)
- [ ] Test: "Sr. Manager People Analytics" / "Netflix Inc" matches "Senior Manager, People Analytics" / "Netflix"
- [ ] Test: "People Analytics Director" / "Google" does NOT match "Customer Analytics Director" / "Google"

## Phase 4: Publishers
- [ ] Build src/publishers/wordpress.py (batch publish + retry queue)
- [ ] Test: publish one job, verify in WP admin
- [ ] Test: same job again → update, not duplicate
- [ ] Test: simulate WP down → job goes to retry_queue
- [ ] Test: next run → retry_queue jobs get published
- [ ] Build src/publishers/notifier.py (Pushover + Brevo)
- [ ] Test: Pushover push notification
- [ ] Test: Brevo email
- [ ] Build src/publishers/archiver.py (7-day staleness, LIMIT 100)
- [ ] Test: mark stale job → archived

## Phase 5: Orchestrator
- [ ] Build src/collector.py (full pipeline with --dry-run)
- [ ] Rich Healthchecks ping (per-source counts in POST body)
- [ ] Zero-results canary (ping /fail + Pushover if all sources return 0)
- [ ] Consecutive-zero tracking in Turso
- [ ] Run locally --dry-run
- [ ] Run locally with real publishing
- [ ] Verify jobs on WordPress

## Phase 6: GitHub Actions
- [ ] Verify collect.yml and keepalive.yml
- [ ] Push to GitHub (public repo)
- [ ] Add ALL secrets (28 total — check .env.yaml.example)
- [ ] Manual trigger, watch logs
- [ ] Verify: jobs on WordPress, Healthchecks ping, no errors

## Phase 7: Testing and Polish
- [ ] Full test suite passes
- [ ] Run twice → no duplicates in WP
- [ ] Archival test: set last_seen 8 days ago → archives
- [ ] Pushover + email both fire for qualifying job
- [ ] WordPress table: search, sort, mobile responsive
- [ ] Update README.md with final setup instructions

## Review
_To be filled after build._
