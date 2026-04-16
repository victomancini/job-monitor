# Job Monitor

Automated daily monitoring for employee listening and people analytics job postings. US-primary focus with international English bonus. Five API sources, four-tier LLM classification, WordPress publishing, dual-channel alerting, zero-results canary.

## Pipeline

Sources → keyword filter → LLM classifier → deduplicator → WordPress → notifier → archiver → Healthchecks ping. Pipeline ordering is a hard constraint.

1. **Collect**: JSearch (primary, Google for Jobs) + Jooble + Adzuna + USAJobs (Mondays only, federal) + Google / Talkwalker / SIOP RSS
2. **Filter**: three-tier keyword scoring with word-boundary regex. Positive + negative conflict → LLM (never auto-decide). Tier 1/2 company in `companies.yaml` → +15 points
3. **Classify**: Groq `llama-3.3-70b-versatile` → Gemini `gemini-2.5-flash-lite` → GPT-4o-mini → keyword-only fallback. Any tier failure (429 / 5xx / bad JSON) falls through to the next
4. **Deduplicate**: RapidFuzz composite (`0.4 × company + 0.4 × title + 0.2 × city`; re-weights to 0.5/0.5 when location is missing). Batch peers + active Turso rows
5. **Publish**: WordPress custom REST endpoint in batches of 20, writes `wp_post_id` back to Turso, failed batches → `retry_queue`
6. **Alert**: Pushover per-job push + Brevo SMTP digest email, triggered on `fit_score >= 50` or LLM `RELEVANT`
7. **Archive**: Turso jobs unseen 7+ days marked inactive. WP plugin's own cron handles post-side archival
8. **Monitor**: POST rich body to Healthchecks.io with per-source counts, LLM provider breakdown, errors, duration. Zero-results canary fires `/fail` + priority-1 Pushover alert after 2+ consecutive zero runs

## Setup

### 1. Credentials

Copy `.env.yaml.example.example` and fill all values. Then add each as a **GitHub Actions secret** (29 total). Pre-flight validation in `collector.py` exits with a clear error if any required value is missing.

**Required** (pipeline won't start without these):
`JSEARCH_API_KEY`, `JOOBLE_API_KEY`, `GROQ_API_KEY`, `GEMINI_API_KEY`, `TURSO_DB_URL`, `TURSO_AUTH_TOKEN`, `WP_URL`, `WP_USERNAME`, `WP_APP_PASSWORD`, `HEALTHCHECK_URL`

**Optional** (source/channel is skipped if empty):
Adzuna, USAJobs, OpenAI (safety-net LLM), Brevo (email), Pushover (push), all RSS feed URLs

### 2. Turso database

1. Create account at app.turso.tech, create a database in region `iad`
2. Copy the libsql URL and auth token into `TURSO_DB_URL` / `TURSO_AUTH_TOKEN`
3. Schema is applied automatically by `db.migrate()` on first run

### 3. WordPress plugin

1. Download DataTables 2.1.8 JS/CSS to `wordpress/assets/{js,css}/` per `wordpress/assets/DOWNLOAD_DATATABLES.md` (self-hosted — do NOT use CDN; it was hijacked July 29, 2025)
2. Upload `wordpress/` directory to the WP `plugins/` folder via SFTP
3. Activate the `Job Monitor` plugin in WP admin
4. Create an Application Password: Users → Profile → Application Passwords. Copy into `WP_APP_PASSWORD`
5. Create two pages with shortcodes: `[job_table]` and `[job_archive_table]`

### 4. Healthchecks.io

Create a check with period 24h / grace 6h. Paste the ping URL into `HEALTHCHECK_URL`.

### 5. GitHub Actions

Push to GitHub. The workflow runs daily at 11:30 UTC (7:30 am ET). Concurrency group blocks parallel runs.

## Local Development

Python 3.12 is required — `libsql` does not yet publish wheels for 3.14.

```bash
pip install -r requirements.txt
python -m pytest tests/ -v            # 163 tests, all with mocked I/O
python src/collector.py --dry-run     # fetch + filter + classify, skip publish/notifications
python src/collector.py               # full pipeline
```

Shadow log (`shadow_log.jsonl`, gitignored) records every candidate's keyword decision and LLM classification for retrospective threshold calibration.

## Key Rules

- **Zero false positives.** When in doubt, REJECT. `active listening` as a soft skill is the #1 false-positive source.
- **US-primary.** International English (UK / CA / AU) only as bonus queries on Jooble and Adzuna.
- **JSearch quota**: 200 requests/month free tier. 3 queries/day × 30 = 90 used. Quota remaining logged every run; warning fires at <40 remaining.
- **USAJobs runs weekly** (Mondays only).
- **google_alerts source → always LLM review**, regardless of keyword score.
- All API sources, WordPress, notifications, and healthcheck pings are independently fault-tolerant. One failure does not crash the pipeline.

## Annual Cost

| Component | Cost |
|---|---|
| WordPress hosting (free tier at KnownHost) | $0 |
| All job APIs (free tiers) | $0 |
| Groq + Gemini (free tiers) | $0 |
| GPT-4o-mini safety net (~3% of traffic) | ~$10 |
| Pushover (one-time purchase) | $5 |
| **Total** | **~$15** |

## Project Structure

See `CLAUDE.md` for full architecture. Scoped build rules in `.claude/rules/`. Target companies in `config/companies.yaml`, keyword taxonomy in `config/keywords.yaml`, API queries in `config/queries.yaml`.
