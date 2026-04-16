---
paths:
  - "src/publishers/**/*.py"
  - "wordpress/**"
---
# WordPress Integration Rules

## REST API Authentication
- Basic Auth: base64("{WP_USERNAME}:{WP_APP_PASSWORD}")
- Requires HTTPS (Application Passwords reject HTTP)
- Test connectivity first: GET /wp-json/wp/v2/types should return 200

## Publishing (src/publishers/wordpress.py)
- Custom endpoint: POST {WP_URL}/wp-json/jobmonitor/v1/update-jobs
- Body: {"jobs": [array of job dicts]}
- Response: {"created": int, "updated": int, "errors": int, "post_ids": {"external_id": wp_post_id, ...}}
- IMPORTANT: Parse post_ids from response and store wp_post_id back in Turso for each job. The archiver needs this to update specific WordPress posts.
- Batch size: 20 jobs per request
- 1-second delay between batches
- Retry: 3 attempts with exponential backoff (2s, 4s, 8s) on 5xx
- On WordPress down: store unpublished jobs in Turso retry_queue table for next run
- Check HTTP response: 200 = success. Parse {"created": int, "updated": int, "errors": int}
- On connection error: log warning, do NOT crash pipeline

## Retry Queue
- Turso table: retry_queue (id, job_json TEXT, created_at TEXT, attempts INTEGER DEFAULT 0)
- At start of each publish phase: check retry_queue for jobs with attempts < 3
- On successful publish: delete from retry_queue
- On failure: increment attempts counter
- After 3 failed attempts: log permanently, remove from queue

## Notification (src/publishers/notifier.py)
- Primary: Pushover push notification (instant mobile alert)
  - API: POST https://api.pushover.net/1/messages.json
  - Body: token, user, title, message, url (apply link), priority
  - $5 one-time purchase. 10,000 messages/month included.
- Secondary: Brevo SMTP email (smtp-relay.brevo.com:587 STARTTLS)
  - Send digest of ALL qualifying new jobs, not individual emails
  - Max one email per pipeline run
- Qualifying jobs: fit_score >= 50 OR llm_classification == "RELEVANT"
- If Pushover fails: log warning, continue (email is backup)
- If SMTP fails: log warning, do NOT crash pipeline

## Archival (src/publishers/archiver.py)
- The WordPress plugin handles WP-side archival automatically via WP-Cron (daily). Python archiver only needs to update TURSO.
- Query Turso for active jobs where last_seen_date < (today - 7 days)
- Update Turso: is_active=0, archived_date=today, days_active=calculated
- The WP-Cron in the plugin independently archives stale posts (LIMIT 100 per run)
- Python archiver does NOT need to call WordPress REST API for archival

## WordPress Plugin Key Requirements
- DataTables JS/CSS MUST be self-hosted in wordpress/assets/. CDN was hijacked Jul 2025.
- Shortcode: use transient caching (12-hour TTL) invalidated on new post creation
- Bulk inserts: wrap in wp_defer_term_counting(true) and wp_suspend_cache_addition(true)
- Temporarily unhook Rank Math during programmatic inserts: remove_all_actions('save_post') before loop, re-add after
- Input sanitization: wp_kses_post() for descriptions, sanitize_text_field() + mb_substr($val, 0, 500) for titles, esc_url_raw() for URLs
- Output escaping: esc_html() for text, esc_url() for links, esc_attr() for attributes
- Custom "archived" status: works in WP_Query (shortcodes) but has REST API visibility bug (Trac #44119)
- PHP memory: posts_per_page=-1 hits memory limit at ~15K posts on shared hosting. Plan DataTables server-side processing when post count exceeds 1,000.
