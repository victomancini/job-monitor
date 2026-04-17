# Table Enhancements + Enrichment Pipeline — Task File for Claude Code

Read CLAUDE.md first for project context. Then implement these changes in order.
Do NOT push to GitHub or make commits until I approve. Do NOT call real APIs — use mock responses for testing.
Test after each phase.

---

## PHASE A: Database Schema Updates

Add these new columns to the `jobs` table schema in `src/db.py`. Use ALTER TABLE IF NOT EXISTS pattern so migration is safe to re-run.

```sql
-- New columns
apply_url TEXT,                    -- Best direct application link (company careers page)
seniority TEXT,                    -- Extracted level: Executive, VP, Director, Senior Manager, Manager, Senior IC, IC, Unknown
location_confidence TEXT DEFAULT 'unverified',   -- confirmed, inferred, aggregator_only, unverified
salary_confidence TEXT DEFAULT 'unverified',     -- confirmed, inferred, aggregator_only, unverified  
remote_confidence TEXT DEFAULT 'unverified',     -- confirmed, inferred, aggregator_only, unverified
enrichment_source TEXT,            -- 'source_page' if enriched from URL fetch, 'aggregator' if original data only, 'llm' if LLM-extracted
enrichment_date TEXT,              -- When enrichment was last attempted
```

Add these columns to `_JOB_COLUMNS` list so upsert handles them.

Add migration logic: after CREATE TABLE IF NOT EXISTS, run ALTER TABLE ADD COLUMN IF NOT EXISTS for each new column. SQLite doesn't support IF NOT EXISTS on ALTER TABLE, so wrap each in a try/except that catches "duplicate column name" errors silently.

---

## PHASE B: Seniority Extraction Module

Create `src/processors/seniority.py`:

```python
"""Extract seniority/level from job titles using priority-ordered pattern matching."""

SENIORITY_MAP = [
    # Order matters — first match wins
    # Executive
    (r"\b(chief|c-suite|chro|cpo|cao|cdo|cto)\b", "Executive"),
    (r"\bsvp\b", "Executive"),
    (r"\b(senior|sr\.?)\s+vice\s+president\b", "Executive"),
    # VP
    (r"\bvice\s+president\b", "VP"),
    (r"\bvp\b", "VP"),
    # Director
    (r"\b(senior|sr\.?)\s+director\b", "Senior Director"),
    (r"\bglobal\s+head\s+of\b", "Senior Director"),
    (r"\bhead\s+of\b", "Director"),
    (r"\bdirector\b", "Director"),
    # Senior Manager
    (r"\b(senior|sr\.?)\s+manager\b", "Senior Manager"),
    # Manager
    (r"\bmanager\b", "Manager"),
    # Senior IC
    (r"\bprincipal\b", "Senior IC"),
    (r"\bstaff\b", "Senior IC"),
    (r"\b(senior|sr\.?)\s+(analyst|scientist|engineer|researcher|consultant|associate|specialist)\b", "Senior IC"),
    (r"\blead\b", "Senior IC"),
    # IC
    (r"\b(analyst|scientist|engineer|researcher|specialist|coordinator|associate)\b", "IC"),
    # Consultant (could be any level)
    (r"\bconsultant\b", "IC"),
]
```

Function signature: `def extract_seniority(title: str) -> str` — returns the seniority string or "Unknown".

Use `re.IGNORECASE`. First match wins (priority ordered). Apply `_preprocess()` from keyword_filter before matching (import it or duplicate the lightweight version).

Also add seniority to the LLM classification prompt as an additional output field. In `llm_classifier.py`, update the JSON response schema to include:
```json
{
  "classification": "RELEVANT",
  "confidence": 85,
  "reasoning": "...",
  "seniority": "Senior Manager"
}
```

In the LLM prompt, add: "Also extract the seniority level from the title. Use one of: Executive, VP, Senior Director, Director, Senior Manager, Manager, Senior IC, IC, Unknown."

Logic: regex extracts first, LLM confirms/overrides during classification (costs ~2 extra tokens). If they disagree, prefer the LLM's answer (it has more context about the role).

Write tests: test all seniority levels with realistic titles, test abbreviations (Sr., VP), test compound titles ("Senior Manager, People Analytics"), test edge cases ("Lead People Scientist" = Senior IC not Manager).

---

## PHASE C: Apply URL Extraction

Update each source module to extract the best available direct application URL:

### `src/sources/jsearch.py`
JSearch returns `job_apply_link` (direct company URL) and `job_google_link` (Google redirect).
- Set `apply_url = item.get("job_apply_link") or item.get("job_google_link") or source_url`
- Pass `apply_url` through `build_job()`

### `src/sources/jooble.py`
Jooble returns `link` which goes to Jooble's redirect page.
- Set `apply_url = item.get("link", "")`
- This is the best available — Jooble doesn't provide direct company URLs

### `src/sources/adzuna.py`
Adzuna returns `redirect_url` (to company page).
- Set `apply_url = item.get("redirect_url") or source_url`

### `src/sources/usajobs.py`
USAJobs returns the direct federal listing URL in `PositionURI`.
- Set `apply_url = item.get("PositionURI") or source_url`
- USAJobs URLs are always direct — no redirect

### `src/sources/google_alerts.py`
RSS feed `link` is the actual article/posting URL.
- Set `apply_url = link`

### `src/shared.py`
Add `apply_url` as a parameter to `build_job()`. Default to `source_url` if not provided.

Write tests: verify each source extracts apply_url correctly from mock responses.

---

## PHASE D: Lightweight Enrichment Module

Create `src/processors/enrichment.py`:

This module fetches the source URL for each qualifying job, extracts text, and looks for salary, location, and remote status information that may be more complete than what the aggregator provided.

### When to run
Only run on jobs that:
1. Passed the keyword filter (candidates, not rejects)
2. Have a non-empty `source_url` or `apply_url`
3. Have NOT been enriched before (check `enrichment_date` is NULL or older than 7 days)

### How it works
```python
def enrich_job(job: dict) -> dict:
    """Fetch source URL, extract structured data, update job dict with findings."""
    url = job.get("apply_url") or job.get("source_url")
    if not url:
        job["enrichment_source"] = "aggregator"
        return job
    
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0 ..."})
        if resp.status_code != 200:
            job["enrichment_source"] = "aggregator"
            return job
        
        text = _extract_text(resp.text)  # strip HTML, get plain text
        
        # Extract salary
        salary_found = _extract_salary(text)
        if salary_found and not job.get("salary_min"):
            job["salary_min"] = salary_found["min"]
            job["salary_max"] = salary_found["max"]
            job["salary_range"] = salary_found["range_str"]
            job["salary_confidence"] = "confirmed"
        elif job.get("salary_min"):
            job["salary_confidence"] = "aggregator_only"
        
        # Extract remote status
        remote_found = _extract_remote_status(text)
        if remote_found:
            if job.get("is_remote") == "unknown" or not job.get("is_remote"):
                job["is_remote"] = remote_found
            job["remote_confidence"] = "confirmed"
        elif job.get("is_remote") and job["is_remote"] != "unknown":
            job["remote_confidence"] = "aggregator_only"
        
        # Extract/confirm location
        location_found = _extract_location(text)
        if location_found and not job.get("location"):
            job["location"] = location_found
            job["location_confidence"] = "confirmed"
        elif job.get("location"):
            job["location_confidence"] = "aggregator_only"
        
        job["enrichment_source"] = "source_page"
        job["enrichment_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
    except (requests.RequestException, Exception) as e:
        log.warning("enrichment: failed to fetch %s: %s", url, e)
        job["enrichment_source"] = "aggregator"
    
    return job
```

### Salary extraction regex
```python
_SALARY_PATTERNS = [
    # $120,000 - $180,000
    r'\$\s*([\d,]+)\s*[-–to]+\s*\$\s*([\d,]+)',
    # $120K - $180K
    r'\$\s*(\d+)\s*[kK]\s*[-–to]+\s*\$?\s*(\d+)\s*[kK]',
    # $120,000 - 180,000 (no second $)
    r'\$\s*([\d,]+)\s*[-–to]+\s*([\d,]+)',
    # Pay range: 120000 - 180000
    r'(?:salary|pay|compensation|range)[:\s]*\$?\s*([\d,]+)\s*[-–to]+\s*\$?\s*([\d,]+)',
]
```

Parse both values, normalize to integers. If value < 1000, assume it's in thousands (multiply by 1000). Store as `salary_min` and `salary_max`. Generate `salary_range` string using the existing `format_salary_range()` from shared.py.

### Remote status extraction
```python
_REMOTE_PATTERNS = {
    "remote": [r"\bfully\s+remote\b", r"\b100%\s+remote\b", r"\bremote\s+position\b", r"\bwork\s+from\s+home\b", r"\bremote\s+eligible\b"],
    "hybrid": [r"\bhybrid\b", r"\b\d+\s*days?\s+(?:in\s+)?office\b", r"\bin-office\s+\d+\b"],
    "onsite": [r"\bon[\s-]?site\b", r"\bin[\s-]?office\b", r"\bin[\s-]?person\b", r"\bno\s+remote\b"],
}
```

Priority: if both "remote" and "hybrid" match, prefer "hybrid" (more specific). If both "hybrid" and "onsite" match, prefer "hybrid".

### Location extraction
Look for patterns like:
- "Location: City, ST" 
- "City, State" (match against known US state abbreviations)
- "Multiple locations" → set location to "Multiple Locations"

Don't try to be too clever here — if the aggregator already has a location, keep it. Only extract from source page if aggregator location is empty.

### Rate limiting
Add a 1-second delay between URL fetches to be respectful. Use the existing `_http.py` retry helper if possible, or simple requests.get with timeout.

### Batch function
```python
def enrich_batch(jobs: list[dict]) -> list[dict]:
    """Enrich a list of jobs. Returns the same list with updated fields."""
    enriched = []
    for job in jobs:
        enriched.append(enrich_job(job))
        time.sleep(1)  # rate limit
    return enriched
```

Write tests with mocked HTTP responses: test salary extraction from various formats, test remote status detection, test location extraction, test timeout handling, test non-200 responses, test already-enriched jobs are skipped.

---

## PHASE E: Pipeline Integration

### Update `src/collector.py`

Add enrichment as a new phase between deduplication (Phase 4) and WordPress publish (Phase 5). The pipeline becomes:

```
Sources → Keyword Filter → LLM Classifier → Deduplicator → **Enrichment** → WordPress → Notifier → Archiver → Healthcheck
```

In the orchestrator:
```python
log.info("=== Phase 4.5: enrichment ===")
from src.processors import enrichment
unique_jobs = enrichment.enrich_batch(unique_jobs)
enrichment_stats = {
    "enriched_from_source": sum(1 for j in unique_jobs if j.get("enrichment_source") == "source_page"),
    "aggregator_only": sum(1 for j in unique_jobs if j.get("enrichment_source") == "aggregator"),
}
log.info("enrichment: %s", enrichment_stats)
```

### Update seniority integration

After LLM classification, apply seniority extraction:
```python
from src.processors.seniority import extract_seniority

for job in candidates:
    # Regex first
    job["seniority"] = extract_seniority(job.get("title", ""))
    # LLM may override during classification (if it returned seniority field)
    if job.get("_llm_seniority"):
        job["seniority"] = job["_llm_seniority"]
```

### Update healthcheck ping body
Add enrichment stats to the rich healthcheck ping body.

Write integration tests: verify enrichment runs in correct pipeline position, verify seniority is populated on output jobs, verify enrichment stats are logged.

---

## PHASE F: WordPress Plugin Updates

Update `wordpress/job-monitor.php` to display the new columns and add filtering.

### F1: Add new columns to the job table shortcode

Update the `[job_table]` shortcode output to include these columns:
- **Apply** — clickable link using `apply_url` (falls back to `source_url`). Display as a button or "Apply →" link.
- **Level** — seniority field. Display as text.
- **Remote** — `is_remote` field with confidence badge. Show "Remote ✓", "Hybrid", "On-site", or "Unknown". If `remote_confidence` = "confirmed", show green badge. If "aggregator_only", show gray badge. If "unverified", show no badge.
- **Salary** — `salary_range` field with confidence badge. Same green/gray/none badge pattern.
- **Location** — already displayed, but add confidence badge.

### F2: Column filtering with DataTables

Add per-column filtering using DataTables `initComplete` callback:

```javascript
initComplete: function() {
    this.api().columns().every(function() {
        var column = this;
        var header = $(column.header()).text();
        
        // Dropdown filter for categorical columns
        if (['Level', 'Remote', 'Source'].indexOf(header) >= 0) {
            var select = $('<select><option value="">All</option></select>')
                .appendTo($(column.footer()).empty())
                .on('change', function() {
                    column.search($(this).val()).draw();
                });
            column.data().unique().sort().each(function(d) {
                select.append('<option value="' + d + '">' + d + '</option>');
            });
        }
        
        // Text search for title, company, location
        if (['Title', 'Company', 'Location'].indexOf(header) >= 0) {
            $('<input type="text" placeholder="Filter...">')
                .appendTo($(column.footer()).empty())
                .on('keyup', function() {
                    column.search(this.value).draw();
                });
        }
    });
}
```

Add a `<tfoot>` row to the table HTML for the filter controls.

### F3: Update the REST API endpoint

Update the `/wp-json/jobmonitor/v1/update-jobs` endpoint to accept and store the new fields: `apply_url`, `seniority`, `location_confidence`, `salary_confidence`, `remote_confidence`, `enrichment_source`, `enrichment_date`.

Update the custom post type meta fields to include these.

### F4: Update the archive table shortcode

Apply the same column and filtering changes to `[job_archive_table]`.

### F5: CSS for confidence badges

Add inline styles or a small CSS block:
```css
.confidence-confirmed { 
    background: #d4edda; color: #155724; 
    padding: 2px 6px; border-radius: 3px; font-size: 0.8em; 
}
.confidence-aggregator { 
    background: #e2e3e5; color: #383d41; 
    padding: 2px 6px; border-radius: 3px; font-size: 0.8em; 
}
```

### F6: Update WordPress publisher module

In `src/publishers/wordpress.py`, add the new fields to `_WP_FIELDS` whitelist so they get sent to WordPress:
- apply_url
- seniority
- location_confidence
- salary_confidence
- remote_confidence
- enrichment_source

---

## PHASE G: Tests

### New test files needed:

**tests/test_seniority.py** — Test all seniority levels:
- "Chief People Officer" → Executive
- "VP, People Analytics" → VP
- "Senior Director of People Insights" → Senior Director
- "Head of People Analytics" → Director
- "Director, Employee Listening" → Director
- "Senior Manager, People Analytics" → Senior Manager
- "Manager, Employee Engagement" → Manager
- "Principal People Scientist" → Senior IC
- "Staff People Scientist" → Senior IC  
- "Senior Analyst, People Analytics" → Senior IC
- "Lead People Scientist" → Senior IC
- "People Analytics Analyst" → IC
- "People Data Engineer" → IC
- "Workforce Planning Coordinator" → IC
- "Random Job Title" → Unknown
- Test abbreviations: "Sr. Manager" → Senior Manager, "VP" → VP
- Test that LLM override works when provided

**tests/test_enrichment.py** — Test with mocked HTTP:
- Salary extraction: "$120,000 - $180,000" → (120000, 180000)
- Salary extraction: "$120K-$180K" → (120000, 180000)
- Salary extraction: "Pay range: $95,000 to $130,000" → (95000, 130000)
- Remote extraction: "This is a fully remote position" → "remote"
- Remote extraction: "Hybrid - 3 days in office" → "hybrid"
- Remote extraction: "On-site position in New York" → "onsite"
- Location extraction: "Location: Austin, TX" → "Austin, TX"
- Timeout handling: request times out → enrichment_source = "aggregator"
- 404 handling: page not found → enrichment_source = "aggregator"
- Already enriched: enrichment_date is recent → skip
- Batch function: processes list with delays

**Update tests/test_sources.py** — Verify apply_url extraction for each source.

**Update tests/test_publishers.py** — Verify new fields are included in WordPress payload.

**Update tests/test_collector.py** — Verify enrichment phase runs in correct position.

Run full test suite. All tests must pass.

---

## Summary of new pipeline flow

```
1. Sources (JSearch, Jooble, Adzuna, USAJobs, RSS)
   → Now extracts apply_url per source
2. Keyword Filter
   → No changes this task
3. LLM Classifier  
   → Now also returns seniority in JSON response
4. Deduplicator
   → No changes this task
4.5 Enrichment (NEW)
   → Fetches source URLs for qualifying jobs
   → Extracts salary, remote status, location from page text
   → Sets confidence flags (confirmed/aggregator_only/unverified)
5. WordPress Publish
   → Now sends apply_url, seniority, confidence fields
   → Table displays new columns with filters and badges
6. Notifier
   → No changes this task
7. Archiver
   → No changes this task  
8. Healthcheck
   → Now includes enrichment stats
```
