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

---

# R11 — Field Trust Initiative (April 2026)

**Goal:** Fix two defects Victor flagged in production:
1. Data fields (esp. `is_remote`, location, salary) carry aggregator errors through to WP without validation.
2. "Posted X days ago" is unsortable/unreliable — jobs keep showing as "NEW" on repeat runs.

**Frame:** Three expert angles converged on the same shape:
- **Field provenance** — record source-of-truth per field (from E1: Data Engineer)
- **Consensus voting** — when N sources touch the same job, vote weighted by reliability rather than last-write-wins (from E2: IR/Signal Fusion)
- **Guardrails on any new HTTP** — budget, cache, circuit breakers (from E3: Scraping SRE)

Skip universal canonical-page fetching. Instead: re-derive critical fields from description text, add consensus voting across existing sources, and lazily parse schema.org JobPosting ONLY when enrichment already lands on a cooperative host.

---

## Phase 0 — Diagnose & fix "NEW today" bug (small, contained)

The "NEW" badge (wordpress/job-monitor.php:525) fires when `first_seen_date === today_UTC`.
- Python DB correctly preserves first_seen_date (db.py:360 sets only on INSERT; line 378 excludes from UPDATE).
- Python publisher does NOT send first_seen_date to WP (_WP_FIELDS in publishers/wordpress.py:21-38 omits it).
- WP `jm_batch_update` UPDATE path leaves first_seen_date meta untouched (not in $allowed, line 142).
- WP INSERT path sets first_seen_date = today (line 186).

**Hypotheses to investigate (in order):**
- [ ] H1 — `get_posts` lookup failing for edge-case external_ids (`==`, `+`, `/` in base64 IDs): some sanitize_text_field/meta_value comparison mismatch → WP thinks job is new → re-INSERT with today's first_seen_date
- [ ] H2 — Victor's WP post counts growing: if duplicate WP posts exist, the lookup may return the wrong one; confirm by SQL query on wp_postmeta for one re-marked-NEW job
- [ ] H3 — Something (archival cron, manual deletion, plugin re-activation) is wiping first_seen_date meta; WP INSERT branch then fires on next publish
- [ ] H4 — Sort unreliability is a separate matter from "NEW": the data-order uses `date_posted ?: first_seen` (line 504) — if `date_posted` is missing for some sources, mixed reference dates in the same column produce nonsense sort order

**Fix plan (after confirming which H):**
- [ ] Add `first_seen_date` to _WP_FIELDS so Python ships the authoritative value every run
- [ ] Update WP plugin to accept first_seen_date on UPDATE path ONLY when meta is absent (never overwrite existing)
- [ ] Make freshness sort strictly use Python-computed `days_since_posted` (integer) — ship it pre-computed from Turso, eliminate server-timezone arithmetic in WP
- [ ] Keep `NEW` badge only for jobs with `first_seen_date` of today AND not seen in a previous run (add a boolean `is_brand_new` field from Python based on INSERT vs UPDATE return of upsert_job)
- [ ] Add regression test: upsert same job twice, verify `is_brand_new` True → False

## Phase 1 — Field provenance schema (foundation)

- [ ] Add new columns to `jobs` table: `field_sources TEXT` (JSON), `field_confidence TEXT` (JSON)
- [ ] Introduce light Python helper in src/shared.py: `record_field(job, field, value, source, confidence)` — writes into both `job[field]` and `job["_field_sources"][field] = {source, confidence}`
- [ ] Retrofit each source adapter to call `record_field` for is_remote, location, location_country, salary_min, salary_max, work_arrangement, date_posted
- [ ] Serialize `_field_sources` into `field_sources` column in db.upsert_job
- [ ] Verify no existing behavior breaks; flat fields still work for reads
- [ ] Tests: each source populates provenance; round-trip through DB preserves it

## Phase 2 — Description-text re-derivation (fast accuracy win)

- [ ] New module src/processors/text_classifier.py with functions:
  - `classify_work_arrangement(description) -> (value, confidence)` — keyword patterns for "fully remote" / "100% remote" (high conf remote), "hybrid 3 days" (hybrid high), "must be onsite" / "in-office" (onsite high), mentions of a city without "remote" (weak onsite signal)
  - `extract_location_hints(description)` — regex for "based in {city}", "office in {city}", "{state} residents"
- [ ] Integrate into pipeline as a new processor that runs AFTER sources, BEFORE dedup — so every job gets a text-derived observation in addition to the source's structured field
- [ ] Treat text-classifier output as one more provenance entry with its own reliability prior
- [ ] Tests: curated fixtures for each remote/hybrid/onsite pattern

## Phase 3 — Consensus voting in dedup merge

- [ ] Define SOURCE_RELIABILITY priors (config/source_reliability.yaml):
  - greenhouse/lever/ashby/usajobs: 0.90 (canonical ATS)
  - text_classifier: 0.75 (deterministic, conservative)
  - jsearch: 0.55 (often stale / over-tags remote)
  - adzuna: 0.55
  - jooble: 0.50
  - jobspy_*: 0.60
  - google_alerts: 0.40
- [ ] Extend src/processors/deduplicator.py merge logic: when peers found, don't drop the loser — collect all observations per field, compute weighted vote per field, pick winning value AND record `{value, supporting_sources, confidence}`
- [ ] For `is_remote`/`work_arrangement`: majority wins; tie → most-restrictive (onsite > hybrid > remote) to avoid false-remote
- [ ] For numeric fields (salary): use highest-confidence source's value but record range of observations
- [ ] Expose per-field confidence in the WP payload; render badge (green = high conf + multiple sources agree, yellow = medium, gray = single source)
- [ ] Tests: two sources disagree → majority + confidence correct; three agree → high confidence

## Phase 4 — Schema.org JobPosting opportunistic extraction

- [ ] When `enrichment._head_final_url` resolves to a non-aggregator, non-LinkedIn host: fetch body (GET, not HEAD), parse `<script type="application/ld+json">` for JobPosting
- [ ] Extract: `jobLocationType` (TELECOMMUTE/hybrid), `jobLocation.address`, `baseSalary`, `datePosted`, `directApply`
- [ ] Emit as a new provenance source with reliability 0.85
- [ ] Runs only when cache miss AND budget allows (see Phase 5)
- [ ] Tests: fixture HTML with known JSON-LD → correct extraction; malformed JSON-LD → no crash

## Phase 5 — Guardrails (E3)

- [ ] New Turso table `url_cache`: (url_hash PK, final_url, final_host, jsonld_snippet, resolved_at, success)
- [ ] Enrichment checks cache before fetch; skip if < 30 days old
- [ ] Per-run fetch budget: max 100 new canonical fetches; prioritize by (llm_classification == RELEVANT desc, keyword_score desc, company in Tier 1/2)
- [ ] Per-host circuit breaker: 5 consecutive failures → quarantine host for 24h (store in Turso)
- [ ] ATS adapter whitelist: only attempt JSON-LD on non-blocked hosts
- [ ] Tests: cache hit skips fetch; budget exhaustion defers lower-priority jobs; circuit breaker trips after N failures

## Phase 6 — WP confidence display + sort fix

- [ ] Extend WP payload with `is_remote_confidence`, `location_confidence`, etc. (already partially there for enrichment, reuse)
- [ ] WP table: confidence badge beside each field
- [ ] WP filter chips: add "high confidence only" toggle
- [ ] Freshness sort: strictly use integer `days_since_posted` from Python; drop the date-parsing in PHP
- [ ] Regression test via tests/test_wp_filter_bar.py style source-parse tests

## Sequencing & check-in gates

- Phase 0 lands standalone, first — it's the bug, it's small, it proves the date-handling
- Phases 1+2+3 ship as one bundle (provenance+text-classifier+voting is the real E1+E2 synthesis; splitting them leaks half-done state into production)
- Phase 4+5 ship together (JSON-LD without guardrails = stream timeout risk)
- Phase 6 is cosmetic; after Phases 3 and 5 are working

Victor to confirm: is this sequencing + scope right, or should I narrow first bundle further?

---

## R11 Implementation Log (2026-04-20)

All six phases shipped in a single session with tests between bundles.

### Files added
- `src/processors/text_classifier.py` — description-text work_arrangement classifier
- `src/processors/schema_org.py` — JobPosting ld+json parser
- `tests/test_text_classifier.py` (15)
- `tests/test_consensus.py` (16)
- `tests/test_schema_org.py` (19)
- `tests/test_enrichment_guardrails.py` (15)
- `tests/test_wp_freshness.py` (+4 added after Phase 6)

### Files modified
- `src/db.py` — upsert_job no longer wipes non-None fields with None, earliest-wins on date_posted, stashes first_seen_date/is_brand_new on the job dict, new field_sources column + migration
- `src/shared.py` — SOURCE_RELIABILITY priors, record_field, apply_provenance, days_since_posted helper, build_job auto-applies provenance
- `src/publishers/wordpress.py` — ships first_seen_date, days_since_posted, is_brand_new, remote_vote_* / work_arrangement_vote_*
- `src/processors/enrichment.py` — schema.org extraction on non-aggregator bodies, _CircuitBreaker, _FetchBudget, priority ordering
- `src/processors/deduplicator.py` — compute_consensus, merge_field_sources, apply_consensus; peer merge preserves both sources' observations
- `src/collector.py` — text_classifier between dedup prep and dedup, apply_consensus after dedup, captures upsert status for is_brand_new
- `wordpress/job-monitor.php` — jm_freshness_cell uses Python days integer + is_brand_new, jm_consensus_tooltip, first_seen_date preserved on UPDATE

### What's fixed
- [x] NEW-today bug: WP never re-stamps first_seen_date once Turso has it
- [x] Sort reliability: freshness sort uses Python integer, no timezone drift
- [x] Data wipe on update: non-None Turso values preserved against None incoming
- [x] is_remote false positives: text_classifier votes against aggregator false flags; consensus overrides flat value when ≥ 0.65 confidence
- [x] Field provenance: every tracked field carries {source, value, confidence} history in field_sources column
- [x] Schema.org extraction: non-aggregator pages with JobPosting ld+json emit source='schema_org' at 0.85 reliability
- [x] Enrichment stability: per-host circuit breaker, per-run 300-GET budget, priority ordering so budget cap hits low-signal tail

### Not done (deferred)
- [ ] Persisted URL cache (Turso table) — existing per-job ENRICHMENT_FRESH_DAYS guard covers most re-fetch cases; add only if real re-fetch cost becomes visible
- [ ] Salary consensus voting — categorical voter only handles is_remote + work_arrangement; numeric salary needs different logic
- [ ] Calibration of SOURCE_RELIABILITY priors — starter values only; should be tuned against labeled shadow-log data
- [ ] Canonical ATS page fetching (Workday, iCIMS adapters) — schema.org extraction happens only on pages we already fetched; adding per-ATS canonical fetchers was explicitly deferred as higher-risk/slower-payoff


