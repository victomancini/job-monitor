# Job Monitor

Automated monitoring for employee listening and people analytics job postings. US-primary (international English as bonus). Five API sources, four-tier LLM classification, WordPress publishing, dual-channel alerting.

## Stack

Python 3.12, GitHub Actions (cron), Turso libSQL (cloud DB), Groq Llama 3.3 70B (primary LLM), Gemini 2.5 Flash-Lite (fallback LLM), GPT-4o-mini (safety net), WordPress REST API (KnownHost), Brevo SMTP + Pushover (alerts), Healthchecks.io (monitoring).

## Commands

```bash
pip install -r requirements.txt
python src/collector.py                       # Full pipeline
python src/collector.py --dry-run             # Fetch + filter, skip publish/email
python -m pytest tests/ -v                    # All tests
python -m pytest tests/test_keyword_filter.py # Single test
python -c "import libsql; from google import genai; import rapidfuzz; import feedparser; print('OK')"
```

## Architecture

```
src/
  collector.py              # Orchestrator: sources → filter → classify → dedup → publish → alert
  db.py                     # Turso connection, migration, query helpers
  sources/
    jsearch.py              # JSearch API (Google for Jobs via RapidAPI) — PRIMARY
    jooble.py               # Jooble API — US-primary
    adzuna.py               # Adzuna API — US endpoint
    usajobs.py              # USAJobs API — federal gov HR/analytics (unique coverage, free)
    google_alerts.py        # Google Alerts + Talkwalker RSS — supplementary
  processors/
    keyword_filter.py       # Three-tier keyword scoring with word-boundary regex
    llm_classifier.py       # Multi-provider: Groq → Gemini 2.5 Flash-Lite → GPT-4o-mini → keyword-only
    deduplicator.py         # RapidFuzz composite matching (title + company + city)
  publishers/
    wordpress.py            # Batch publish via custom REST endpoint with retry queue
    notifier.py             # Brevo email + Pushover push for high-value matches
    archiver.py             # Mark stale jobs (7+ days unseen) as archived
config/
  keywords.yaml             # Keyword taxonomy with scores, tiers, negatives
  companies.yaml            # 190+ target companies by sector and tier
  queries.yaml              # All API query definitions per source
wordpress/
  job-monitor.php           # WP plugin: CPT, REST endpoint, shortcodes, archival cron
  assets/js/                # Self-hosted DataTables (DO NOT use CDN)
  assets/css/
tasks/
  todo.md                   # Build checklist
  lessons.md                # Self-improvement log
```

## Standardized Job Dict

The pipeline builds this dict progressively. Sources populate the first block. Processors add scoring/classification. Publishers consume the full dict.
```python
{
    # --- Set by sources ---
    "external_id": str,           # "{source}_{platform_id}"
    "title": str,
    "company": str,
    "location": str,              # "City, State" or "City, Country"
    "location_country": str,      # ISO 3166-1 alpha-2
    "description": str,           # Full text or snippet
    "description_is_snippet": bool,
    "salary_min": float | None,
    "salary_max": float | None,
    "salary_range": str | None,   # "$120K-$180K"
    "source_url": str,            # Direct apply link
    "source_name": str,           # "jsearch", "jooble", "adzuna", "usajobs", "google_alerts"
    "is_remote": str,             # "remote", "hybrid", "onsite", "unknown"
    "work_arrangement": str,
    "date_posted": str | None,    # ISO 8601
    "raw_data": str,              # JSON dump of full API response

    # --- Set by keyword_filter ---
    "keyword_score": int,         # 0-100
    "keywords_matched": str,      # Comma-separated matched terms
    "fit_score": int,             # Same as keyword_score (alias for WP display)

    # --- Set by llm_classifier ---
    "llm_classification": str,    # "RELEVANT", "PARTIALLY_RELEVANT", "NOT_RELEVANT", or "unvalidated"
    "llm_confidence": int,        # 0-100 integer
    "llm_provider": str,          # "groq", "gemini", "openai", "keyword_only"
    "llm_reasoning": str,         # One-sentence from LLM
}
```

## Important Rules

- IMPORTANT: Zero false positives. When in doubt, REJECT.
- IMPORTANT: US-primary. JSearch queries target US. Jooble location="United States". Adzuna country="us". International English (UK, CA, AU) is bonus, not primary.
- IMPORTANT: JSearch = 200 req/month. Use 3 broad queries/day. BILLING NOTE: num_pages parameter may count as 1 request or N requests depending on RapidAPI billing — start with num_pages=1 (10 results per query), monitor X-RapidAPI-Requests-Remaining header for actual consumption, then increase to num_pages=3 only if budget allows. Track quota and alert at 80%.
- IMPORTANT: LLM chain: Groq (free) → Gemini 2.5 Flash-Lite (free) → GPT-4o-mini (~$10/yr) → keyword-only fallback. Each tier catches the one above failing.
- IMPORTANT: Groq is OpenAI-compatible. Endpoint: https://api.groq.com/openai/v1/chat/completions. Model: "llama-3.3-70b-versatile". Use openai package with custom base_url.
- IMPORTANT: Gemini model is "gemini-2.5-flash-lite" (NOT "gemini-2.0-flash-lite" — deprecated June 1 2026). SDK: google-genai. Import: from google import genai.
- IMPORTANT: USAJobs API uses Authorization-Key header with API key AND User-Agent header with email. Register at developer.usajobs.gov. Federal roles are NOT in Google for Jobs — unique coverage.
- IMPORTANT: Turso SDK is libsql. Import: import libsql. Connect: conn = libsql.connect("libsql://...", auth_token="..."). sqlite3-compatible API.
- IMPORTANT: RapidFuzz: always pass processor=rapidfuzz.utils.default_process (no auto-lowercase since v3.0).
- IMPORTANT: Word-boundary regex for ALL keyword matching: re.search(r'\b' + re.escape(term) + r'\b', text, re.IGNORECASE). Never plain "in".
- IMPORTANT: Positive + negative keyword conflict → ALWAYS route to LLM.
- IMPORTANT: "active listening" is #1 false positive. Must be in negatives.
- IMPORTANT: DataTables MUST be self-hosted. CDN hijacked July 29 2025.
- IMPORTANT: Jooble radius only accepts: 0, 4, 8, 16, 26, 40, 80 km.
- IMPORTANT: Zero-results canary: if ALL sources return 0, ping Healthchecks /fail AND Pushover alert. Track consecutive zeros in Turso. Alert after 2+ consecutive.
- NEVER store credentials in code. All from env vars.
- IMPORTANT: Some env vars are OPTIONAL (TALKWALKER_RSS_1-3, GOOGLE_ALERT_SIOP, ADZUNA_APP_ID/KEY). Code must handle empty/missing values gracefully with os.environ.get("VAR", ""), skipping that source if empty.
- IMPORTANT: USAJobs runs WEEKLY, not daily. In collector.py, check day of week: only call usajobs.py on Mondays (datetime.today().weekday() == 0). All other sources run daily.
- WordPress: Basic Auth with Application Password, base64 encoded.
- Each source independently fault-tolerant. try/except with retry (3x, backoff 2s/4s/8s).
- Each job's processing in try/except. One bad record must not crash the batch.
- Rich Healthchecks pings: per-source counts, published count, error count in POST body.
- WordPress publish failures: retry queue in Turso for next run.
- IMPORTANT: Description storage: Sources store text in `description` regardless of length. Set `description_is_snippet=True` for Jooble snippets. Turso stores full text in `description` column. `description_snippet` column = first 300 chars, populated by keyword_filter. When description_is_snippet=True, LLM classifier notes reduced confidence.
- IMPORTANT: Salary range construction: collector.py builds salary_range from salary_min/salary_max after source collection. Format: "$XXK-$XXXK" if both, "$XXK+" if only min, "Up to $XXXK" if only max, None if neither.
- IMPORTANT: Pre-flight validation: collector.py MUST validate all required env vars are non-empty at startup BEFORE any API calls. Required: JSEARCH_API_KEY, JOOBLE_API_KEY, GROQ_API_KEY, GEMINI_API_KEY, TURSO_DB_URL, TURSO_AUTH_TOKEN, WP_URL, WP_USERNAME, WP_APP_PASSWORD, HEALTHCHECK_URL. Optional (skip source if empty): ADZUNA_*, USAJOBS_*, OPENAI_API_KEY, all RSS vars, PUSHOVER_*, BREVO_*. Exit with clear error listing missing required vars.
- IMPORTANT: SQLite has no ON UPDATE trigger. All UPDATE queries on jobs table MUST set updated_at = datetime('now') explicitly.
- IMPORTANT: Brevo SMTP: host="smtp-relay.brevo.com", port=587, STARTTLS. Hardcode in notifier.py.
- IMPORTANT: ALL jobs from source_name="google_alerts" MUST be routed to LLM classification regardless of keyword score. Override auto-include for this source.
- IMPORTANT: Company boost: If company name matches any entry in companies.yaml Tier 1 or Tier 2 vendors AND the job's title has at least one positive keyword match, add 10 points to keyword_score (src/processors/keyword_filter.py:COMPANY_BOOST_POINTS). Originally +15; dropped to +10 in Phase B3 when gated on positive match. Title-level gate added in the R-audit to prevent aggregator self-mentions in the description from triggering boosts on unrelated roles (e.g., "Allbound SDR" at Culture Amp).
- IMPORTANT: Phase B8 gated T3 terms: "employee experience" scores 5 when an analytics-class co-signal (analytics/insights/data/survey/listening) appears in title or first 400 chars of description. "workforce planning" uses an expanded co-signal (adds manager/director/lead/head/analyst/optimization/optimisation/strategy/strategic) and scores 15 points — the llm_review_min floor — because workforce-planning role titles often lead with a seniority word rather than an analytics word. Both are additionally gated by a NEGATIVE co-signal (nurse/shift/schedule/staffing/contact-centre) to exclude retail/hospital scheduling roles. See src/processors/keyword_filter.py B8_COSIGNALS and B8_NEGATIVE_COSIGNALS.

## Workflow

- IMPORTANT: Pipeline ordering is a HARD CONSTRAINT. collector.py MUST execute in this exact order: (1) sources → (2) keyword_filter → (3) llm_classifier → (4) deduplicator → (5) wordpress publisher → (6) notifier → (7) archiver → (8) healthcheck ping. Reordering will cause data integrity bugs (e.g., archiver before publisher = premature archival).
- IMPORTANT: Shadow mode (first 2 weeks): Log ALL candidates including rejects with their scores to run_log or a separate file. This allows retrospective threshold calibration before trusting auto-reject decisions.
- Read tasks/todo.md at session start.
- Plan in todo.md before multi-file changes.
- Run tests after every module. Never mark done without passing tests.
- After any correction: update tasks/lessons.md.
- Minimal working code. No speculative abstractions.

## Git

- Conventional commits: feat:, fix:, test:, docs:, refactor:
- One logical change per commit
- Reference module: feat: add usajobs source for federal roles

## References

See @config/keywords.yaml for keyword taxonomy
See @config/queries.yaml for API query definitions
See @config/companies.yaml for target companies
See @.claude/rules/ for scoped build rules

## Turso Database Schema

```sql
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    company_normalized TEXT,
    location TEXT,
    location_country TEXT,
    work_arrangement TEXT,
    description TEXT,
    description_snippet TEXT,
    salary_min REAL,
    salary_max REAL,
    salary_range TEXT,
    source_url TEXT,
    source_name TEXT NOT NULL,
    is_remote TEXT DEFAULT 'unknown',
    category TEXT,
    seniority TEXT,
    keyword_score INTEGER DEFAULT 0,
    keywords_matched TEXT,
    llm_classification TEXT,
    llm_confidence INTEGER,
    llm_provider TEXT,
    llm_reasoning TEXT,
    fit_score INTEGER DEFAULT 0,
    date_posted TEXT,
    first_seen_date TEXT NOT NULL,
    last_seen_date TEXT NOT NULL,
    is_active INTEGER DEFAULT 1,
    archived_date TEXT,
    days_active INTEGER,
    wp_post_id INTEGER,
    raw_data TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_external_id ON jobs(external_id);
CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);
CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_normalized);
CREATE INDEX IF NOT EXISTS idx_jobs_last_seen ON jobs(last_seen_date);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    jsearch_found INTEGER DEFAULT 0,
    jooble_found INTEGER DEFAULT 0,
    adzuna_found INTEGER DEFAULT 0,
    usajobs_found INTEGER DEFAULT 0,
    alerts_found INTEGER DEFAULT 0,
    total_passed_filter INTEGER DEFAULT 0,
    total_published INTEGER DEFAULT 0,
    total_archived INTEGER DEFAULT 0,
    errors TEXT,
    llm_provider_used TEXT,
    duration_seconds REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS retry_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_json TEXT NOT NULL,
    attempts INTEGER DEFAULT 0,
    last_attempt TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
```
