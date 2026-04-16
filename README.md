# Job Monitor

Automated monitoring for employee listening and people analytics job postings. US-primary focus. Runs daily via GitHub Actions, queries five job API sources, filters through keyword scoring and multi-provider LLM classification, publishes to WordPress.

## How It Works

1. **Collect**: JSearch (Google for Jobs) + Jooble + Adzuna + USAJobs (federal) + Google/Talkwalker/SIOP Alerts
2. **Filter**: Three-tier keyword scoring with word-boundary regex matching
3. **Classify**: Groq → Gemini 2.5 Flash-Lite → GPT-4o-mini → keyword-only fallback
4. **Deduplicate**: RapidFuzz composite matching against Turso DB
5. **Publish**: WordPress REST API with retry queue for failed publishes
6. **Alert**: Pushover mobile push + Brevo email for high-value matches
7. **Archive**: Jobs not seen 7+ days marked as archived
8. **Monitor**: Rich Healthchecks.io pings with per-source counts + zero-results canary

## Setup

1. Fill in `.env.yaml.example` with all credentials
2. Download DataTables JS/CSS (see `wordpress/assets/DOWNLOAD_DATATABLES.md`)
3. Upload `wordpress/` to WP site via SFTP, activate plugin
4. Create two WP pages: `[job_table]` and `[job_archive_table]`
5. Add all credentials as GitHub Secrets (28 total)
6. Push to GitHub — runs daily at 7:30am ET

## Local Development

```bash
pip install -r requirements.txt
python src/collector.py --dry-run
python -m pytest tests/ -v
```

## Annual Cost

| Component | Cost |
|-----------|------|
| KnownHost WordPress (brother, free) | $0 |
| All job APIs (free tiers) | $0 |
| Groq + Gemini (free tiers) | $0 |
| GPT-4o-mini safety net | ~$10 |
| Pushover (one-time) | $5 |
| **Total** | **~$15** |
| Reserve fund | $185 |
