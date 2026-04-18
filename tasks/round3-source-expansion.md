# Round 3: Source Expansion, Data Quality, and Dashboard Foundation

Read CLAUDE.md first. Implement in order. Test after each phase. Do NOT push or commit until approved.

**Context:** Round 2 (Phases A-I) completed successfully with 488 tests passing. Phase J (description pre-enrichment) was not completed due to API timeout. This task file picks up Phase J and adds source expansion, vendor extraction, lifecycle tracking, and dashboard prep.

---

## PHASE 1: Complete Phase J — Description Pre-Enrichment + Assumed Defaults

This was planned in table-improvements-r2.md Phase J but never built. The enrichment module currently only extracts remote/salary from the SOURCE PAGE fetched via HTTP. But aggregator job descriptions (which we already have) often contain this information.

### 1A: Add `_pre_enrich_from_description()` to `src/processors/enrichment.py`

Before the HTTP fetch in `enrich_job()`, run regex extractors on the job's `description` field:

```python
def _pre_enrich_from_description(job: dict) -> None:
    """Extract remote/salary/location from the aggregator's description text.
    Only fills in fields that are currently empty/unknown."""
    desc = job.get("description") or ""
    if not desc:
        return

    # Remote status from description
    if job.get("is_remote") in (None, "", "unknown"):
        remote = _extract_remote_from_text(desc)
        if remote:
            job["is_remote"] = remote
            job["remote_confidence"] = "inferred"

    # Salary from description
    if not job.get("salary_min"):
        salary = _extract_salary_from_text(desc)
        if salary:
            job["salary_min"] = salary["min"]
            job["salary_max"] = salary.get("max")
            job["salary_range"] = salary["range_str"]
            job["salary_confidence"] = "inferred"
```

Remote extraction patterns (case-insensitive):
- "remote" / "fully remote" / "100% remote" → "remote"
- "hybrid" / "flexible" / "X days in office" / "X days remote" → "hybrid"
- "remote eligible" / "remote optional" / "open to remote" → "hybrid"
- "on-site" / "onsite" / "in-office" / "in office" / "must be located" → "on-site"

Salary extraction patterns:
- "$120,000" / "$120K" / "$120k" — capture as salary_min
- "$120,000 - $180,000" / "$120K-$180K" — capture as min and max
- "salary range: ..." / "compensation: ..." / "pay range: ..." — context anchors

### 1B: Add assumed defaults after enrichment

In `collector.py`, after `apply_enrichment()` returns, apply defaults:

```python
def apply_defaults(jobs: list[dict]) -> None:
    """Phase J: if remote status is still unknown after all extraction passes,
    default to 'on-site' with confidence='assumed'."""
    for job in jobs:
        if job.get("is_remote") in (None, "", "unknown"):
            job["is_remote"] = "on-site"
            job["remote_confidence"] = "assumed"
```

Call this right after `apply_enrichment()` in `run()`.

### 1C: Update confidence badge display

In `wordpress/job-monitor.php`, update `jm_confidence_badge()` to handle the new confidence levels:
```php
case 'inferred': return ' <span class="confidence-inferred" title="Extracted from description">~</span>';
case 'assumed': return ' <span class="confidence-assumed" title="No data found; assumed on-site">?</span>';
```

### 1D: Tests

- Verify description-based remote extraction for each pattern
- Verify description-based salary extraction
- Verify assumed defaults apply when all sources are silent
- Verify pre-enrichment runs before HTTP fetch and HTTP fetch can override
- Verify confidence levels display correctly

---

## PHASE 2: Direct ATS Source Adapters (Greenhouse, Lever, Ashby)

The biggest coverage gap. These three ATS platforms expose free, no-auth JSON APIs. Many PA-relevant companies post here days before aggregators pick them up, with richer structured data (salary, department, location type).

### 2A: Create `src/sources/greenhouse.py`

API endpoint: `https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true`

Returns JSON with `jobs[]` containing: `id`, `title`, `location.name`, `content` (HTML description), `departments[].name`, `updated_at`, `absolute_url` (direct apply link).

```python
GREENHOUSE_COMPANIES = {
    # --- PA/EL Vendors & Platforms ---
    "cultureamp": "Culture Amp",
    "lattice": "Lattice",
    "visier": "Visier",
    "peakon": "Workday (Peakon)",
    "perceptyx": "Perceptyx",
    "qualtrics": "Qualtrics",
    "medallia": "Medallia",
    "confirmit": "Forsta (Confirmit)",
    "glintinc": "Glint (LinkedIn)",
    "surveymonkey": "Momentive (SurveyMonkey)",
    "quantumworkplace": "Quantum Workplace",
    "tinypulse": "TINYpulse",
    "15five": "15Five",
    "betterworks": "BetterWorks",
    "humu": "Humu",
    "orgnostic": "Orgnostic",

    # --- Big Tech ---
    "airbnb": "Airbnb",
    "pinterest": "Pinterest",
    "lyft": "Lyft",
    "stripe": "Stripe",
    "coinbase": "Coinbase",
    "doordash": "DoorDash",
    "instacart": "Instacart",
    "databricks": "Databricks",
    "figma": "Figma",
    "notion": "Notion",
    "airtable": "Airtable",
    "canva": "Canva",
    "plaid": "Plaid",
    "ramp": "Ramp",
    "brex": "Brex",
    "gusto": "Gusto",
    "rippling": "Rippling",
    "deel": "Deel",
    "remotecom": "Remote.com",
    "justworks": "Justworks",
    "duolingo": "Duolingo",
    "discord": "Discord",
    "snap": "Snap",
    "spotify": "Spotify",
    "netflix": "Netflix",
    "squarespace": "Squarespace",
    "etsy": "Etsy",
    "wayfair": "Wayfair",
    "hubspot": "HubSpot",
    "twilio": "Twilio",
    "zoom": "Zoom",
    "dropbox": "Dropbox",
    "asana": "Asana",
    "atlassian": "Atlassian",
    "elastic": "Elastic",
    "hashicorp": "HashiCorp",
    "datadog": "Datadog",
    "pagerduty": "PagerDuty",
    "newrelic": "New Relic",
    "okta": "Okta",
    "cloudflare": "Cloudflare",
    "mongodb": "MongoDB",
    "confluent": "Confluent",
    "supabase": "Supabase",
    "vercel": "Vercel",
    "anthropic": "Anthropic",
    "openai": "OpenAI",

    # --- Consulting ---
    "mckinsey": "McKinsey",
    "bcg": "BCG",
    "bain": "Bain",
    "kincentric": "Kincentric",

    # --- Financial Services ---
    "goldmansachs": "Goldman Sachs",
    "jpmorgan": "JPMorgan Chase",
    "blackstone": "Blackstone",
    "citadel": "Citadel",
    "twosigsigma": "Two Sigma",
    "deshaw": "D.E. Shaw",

    # --- Healthcare/Pharma ---
    "regeneron": "Regeneron",
    "modernatx": "Moderna",

    # --- Large Employers / F500 ---
    "walmart": "Walmart",
    "target": "Target",
    "nike": "Nike",
    "starbucks": "Starbucks",
    "disney": "Disney",
    "comcast": "Comcast (NBCUniversal)",
    "verizon": "Verizon",
    "mastercard": "Mastercard",
    "visa": "Visa",
    "amex": "American Express",
    "fidelity": "Fidelity",
    "capitalone": "Capital One",
    "schwab": "Charles Schwab",
    "wellsfargo": "Wells Fargo",
    "citi": "Citi",
    "bankofamerica": "Bank of America",
    "jnj": "Johnson & Johnson",
    "pfizer": "Pfizer",
    "merck": "Merck",
    "abbvie": "AbbVie",
    "unitedhealth": "UnitedHealth Group",
    "cvs": "CVS Health",
    "kaiserpermanente": "Kaiser Permanente",
    "mayo": "Mayo Clinic",
    "ge": "GE",
    "3m": "3M",
    "boeing": "Boeing",
    "lockheedmartin": "Lockheed Martin",
    "raytheon": "Raytheon",
    "deloitte": "Deloitte",
    "ey": "EY",
    "pwc": "PwC",
    "kpmg": "KPMG",
    "accenture": "Accenture",
    "mercer": "Mercer",
    "kornferry": "Korn Ferry",
    "wtwco": "WTW",
    "aon": "Aon",

    # --- NYC Metro Employers ---
    "macys": "Macy's",
    "esteelauder": "Estee Lauder",
    "loreal": "L'Oreal",
    "marshmclennan": "Marsh McLennan",
    "bloomberg": "Bloomberg",
    "blackrock": "BlackRock",

    # --- PA-Mature Companies (known to have PA functions) ---
    "microsoft": "Microsoft",
    "google": "Google",
    "meta": "Meta",
    "amazon": "Amazon",
    "apple": "Apple",
    "salesforce": "Salesforce",
    "adobe": "Adobe",
    "intuit": "Intuit",
    "workday": "Workday",
    "servicenow": "ServiceNow",
    "snowflake": "Snowflake",
    "palantir": "Palantir",
    "uber": "Uber",
}
```

**IMPORTANT:** Not all of these will have Greenhouse boards. The adapter should:
1. Try each slug
2. If 404 → skip silently (company doesn't use Greenhouse)
3. If 200 → parse jobs, run through keyword filter
4. Cache which slugs return 404 so we don't re-check daily (store in Turso: `ats_company_status` table with `slug`, `ats`, `last_checked`, `status`)

Rate limit: 1 request per second. Greenhouse doesn't document rate limits but is generous for public board APIs.

Each job becomes a `build_job()` call with `source_name="greenhouse"`, `apply_url=absolute_url` (this is always the direct company apply page — huge quality improvement over aggregators).

### 2B: Create `src/sources/lever.py`

API endpoint: `https://api.lever.co/v0/postings/{slug}?mode=json`

Returns JSON array of postings with: `id`, `text` (title), `categories.location`, `categories.team`, `categories.department`, `descriptionPlain`, `hostedUrl` (direct apply link), `createdAt`.

```python
LEVER_COMPANIES = {
    "netflix": "Netflix",
    "figma": "Figma",
    "notion": "Notion",
    "stripe": "Stripe",
    "coinbase": "Coinbase",
    "databricks": "Databricks",
    "ramp": "Ramp",
    "plaid": "Plaid",
    "discord": "Discord",
    "anthropic": "Anthropic",
    "scaleai": "Scale AI",
    "anduril": "Anduril",
    "relativity": "Relativity",
    # Add more as discovered — many overlap with Greenhouse list;
    # companies switch ATS over time, so check both
}
```

Same 404-caching pattern as Greenhouse. `apply_url` = `hostedUrl`.

### 2C: Create `src/sources/ashby.py`

API endpoint: `https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true`

Returns JSON with `jobs[]` containing: `id`, `title`, `location`, `department`, `compensation` (structured: `compensationTierSummary` with `min`, `max`, `currency`, `period`), `descriptionHtml`, `jobUrl` (direct apply link), `publishedAt`.

Ashby is the most valuable ATS to add because it provides structured salary data that aggregators strip.

```python
ASHBY_COMPANIES = {
    "notion": "Notion",
    "linear": "Linear",
    "ramp": "Ramp",
    "plaid": "Plaid",
    "vercel": "Vercel",
    "supabase": "Supabase",
    "posthog": "PostHog",
    "opensea": "OpenSea",
    "ironclad": "Ironclad",
    "retool": "Retool",
    "benchling": "Benchling",
    # Ashby is popular with Series B-D startups
}
```

### 2D: Integrate all three into collector

In `collector.py`, add to `collect_sources()`:
```python
from src.sources import greenhouse, lever, ashby

gh_jobs = greenhouse.fetch()
lever_jobs = lever.fetch()
ashby_jobs = ashby.fetch()
all_jobs.extend(gh_jobs + lever_jobs + ashby_jobs)
```

Add stats to the pipeline meta: `greenhouse_found`, `lever_found`, `ashby_found`.

### 2E: ATS slug discovery + validation

Many of the slugs above are guesses based on company names. Create a one-time discovery script `scripts/discover_ats_slugs.py` that:
1. For each company in the combined list, tries Greenhouse, Lever, and Ashby endpoints
2. Records which slug/ATS combo returns a valid board
3. Outputs a validated JSON file `config/ats_companies.json`
4. The source adapters read from this file rather than hardcoded dicts

This script runs once manually, then periodically (monthly) to catch companies that switch ATS.

### 2F: Tests

- Mock Greenhouse/Lever/Ashby API responses, verify `build_job()` output
- Verify 404 handling (skip silently, no crash)
- Verify Ashby structured compensation flows to `salary_min`/`salary_max`
- Verify `apply_url` is always the direct company URL (not an aggregator redirect)
- Verify rate limiting (1 req/sec)

---

## PHASE 3: JobSpy Integration (LinkedIn, Indeed, Glassdoor, ZipRecruiter)

The `python-jobspy` library (PyPI: `python-jobspy`) scrapes LinkedIn, Indeed, Glassdoor, Google Jobs, and ZipRecruiter concurrently. This adds coverage from platforms the current aggregator APIs don't reach well — especially LinkedIn.

### 3A: Create `src/sources/jobspy_source.py`

```python
from jobspy import scrape_jobs
import pandas as pd

SEARCH_TERMS = [
    "people analytics",
    "employee listening",
    "workforce analytics",
    "HR analytics",
    "talent analytics",
    "people science",
    "employee experience analytics",
]

def fetch() -> list[dict]:
    all_jobs = []
    for term in SEARCH_TERMS:
        try:
            df = scrape_jobs(
                site_name=["linkedin", "indeed", "glassdoor", "zip_recruiter"],
                search_term=term,
                location="United States",  # or leave blank for all
                results_wanted=20,  # per site per term
                hours_old=48,  # only jobs posted in last 48 hours
                country_indeed="USA",
            )
            for _, row in df.iterrows():
                job = build_job(
                    source_name=f"jobspy_{row.get('site', 'unknown')}",
                    external_id=f"jobspy_{row.get('site', '')}_{hash(row.get('job_url', ''))}",
                    title=row.get("title", ""),
                    company=row.get("company", ""),
                    location=row.get("location", ""),
                    description=row.get("description", ""),
                    source_url=row.get("job_url", ""),
                    apply_url=row.get("job_url", ""),
                    salary_min=row.get("min_amount"),
                    salary_max=row.get("max_amount"),
                    is_remote="remote" if row.get("is_remote") else None,
                    date_posted=str(row.get("date_posted", "")),
                )
                all_jobs.append(job)
        except Exception as e:
            log.warning("jobspy: error searching '%s': %s", term, e)
    return all_jobs
```

### 3B: Dependencies

Add to `requirements.txt`: `python-jobspy>=1.1.0`

Note: JobSpy uses `tls-client` internally for anti-bot bypass. This works on GitHub Actions' Ubuntu runner but may need testing.

### 3C: Rate limiting

JobSpy is aggressive by default. Set `results_wanted=20` per site per term to keep volume manageable. With 7 search terms × 4 sites × 20 results = 560 max raw results per run (before dedup). Most will be duplicates of what the aggregators already found — the deduplicator handles this.

### 3D: Tests

- Mock `scrape_jobs` return, verify `build_job()` mapping
- Verify `hours_old=48` filtering
- Verify error handling per search term (one failure doesn't kill the rest)

---

## PHASE 4: Niche Board Scrapers (One Model, Included.ai, SIOP)

### 4A: Create `src/sources/onemodel.py`

Scrape `https://www.onemodel.co/roles-in-people-analytics-hr-technology`

This is a static HTML page with job listings. Parse with BeautifulSoup. One Model updates every two weeks. Cache results and only re-scrape if page content hash changes.

### 4B: Create `src/sources/included_ai.py`

Scrape `https://included.ai/roles-in-people-analytics/`

Similar approach — static HTML page, BeautifulSoup parsing.

### 4C: Create `src/sources/siop.py`

Check `https://jobs.siop.org/jobs/` for RSS feed or parseable HTML. SIOP's job board is the only place specialist I-O psychology positions appear that never make it to aggregators. If RSS available, use feedparser. If HTML only, use BeautifulSoup.

### 4D: Tests

- Mock HTML responses, verify job extraction
- Verify caching (don't re-process unchanged pages)
- Verify these sources flow through the same keyword filter + LLM pipeline

---

## PHASE 5: Vendor/Tool Mention Extraction

### 5A: Create `src/processors/vendor_extractor.py`

Run regex extraction on job description text to identify which platforms, tools, and skills are mentioned.

```python
VENDOR_PATTERNS = {
    # EL/Engagement Platforms
    "Qualtrics": r"\bqualtrics\b",
    "Medallia": r"\bmedallia\b",
    "Glint": r"\bglint\b",
    "Culture Amp": r"\bculture\s*amp\b",
    "Perceptyx": r"\bperceptyx\b",
    "Workday Peakon": r"\b(peakon|workday\s+peakon)\b",
    "Gallup": r"\bgallup\b",
    "Lattice": r"\blattice\b",
    "15Five": r"\b15\s*five\b",
    "BetterUp": r"\bbetterup\b",
    "SurveyMonkey": r"\b(surveymonkey|momentive)\b",
    "Microsoft Viva": r"\b(viva\s+insights?|microsoft\s+viva|workplace\s+analytics)\b",
    "Quantum Workplace": r"\bquantum\s+workplace\b",
    "TINYpulse": r"\btinypulse\b",

    # HRIS / HCM
    "Workday": r"\bworkday\b(?!\s+peakon)",
    "SAP SuccessFactors": r"\b(successfactors|sap\s+sf)\b",
    "Oracle HCM": r"\b(oracle\s+hcm|oracle\s+cloud\s+hcm)\b",
    "ADP": r"\badp\b",
    "UKG": r"\b(ukg|ultimate\s+kronos|ultipro)\b",
    "BambooHR": r"\bbamboohr\b",
    "Dayforce": r"\b(dayforce|ceridian)\b",

    # ATS
    "Greenhouse": r"\bgreenhouse\b",
    "Lever": r"\blever\b",
    "iCIMS": r"\bicims\b",
    "SmartRecruiters": r"\bsmartrecruiters\b",
    "Ashby": r"\bashby\b",

    # BI / Visualization
    "Tableau": r"\btableau\b",
    "Power BI": r"\bpower\s*bi\b",
    "Looker": r"\blooker\b",

    # Analytics / Programming
    "R": r"\bR\b(?!\s*(&|and)\s)",  # careful: "R" alone needs context
    "Python": r"\bpython\b",
    "SQL": r"\bsql\b",
    "SPSS": r"\bspss\b",
    "SAS": r"\bsas\b",
    "Stata": r"\bstata\b",

    # Data platforms
    "Snowflake": r"\bsnowflake\b",
    "Databricks": r"\bdatabricks\b",
    "BigQuery": r"\bbigquery\b",
    "Redshift": r"\bredshift\b",

    # ONA / Specialized
    "Organizational Network Analysis": r"\b(ONA|organizational\s+network\s+analy)\b",
    "Visier": r"\bvisier\b",
    "One Model": r"\bone\s+model\b",
    "Included.ai": r"\bincluded\s*(\.ai)?\b",
    "Crunchr": r"\bcrunchr\b",
    "ChartHop": r"\bcharthop\b",
}

def extract_vendors(description: str) -> list[str]:
    """Return list of vendor/tool names found in the description text."""
    if not description:
        return []
    found = []
    for name, pattern in VENDOR_PATTERNS.items():
        if re.search(pattern, description, re.IGNORECASE):
            found.append(name)
    return found
```

### 5B: Add `vendors_mentioned` column to database

```sql
ALTER TABLE jobs ADD COLUMN vendors_mentioned TEXT;
```

Store as comma-separated string: `"Qualtrics,R,SQL,Tableau"`.

Add to `_UPSERT_FIELDS` in `db.py`.

### 5C: Integrate into collector pipeline

After LLM classification (which already has the description), call vendor extraction:
```python
for job in to_publish:
    job["vendors_mentioned"] = ",".join(extract_vendors(job.get("description", "")))
```

### 5D: Add to WordPress display

Add a "Tools" column to the table (or make vendors available as filter chips). Start with just storing the data — display can be added later.

Add `vendors_mentioned` to `_WP_FIELDS` in `wordpress.py` and to the registered meta fields in the PHP plugin.

### 5E: Tests

- Verify each vendor pattern matches expected strings
- Verify "R" doesn't false-positive on "R&D" or "HR" 
- Verify comma-separated storage format
- Verify vendors flow through to Turso and WordPress

---

## PHASE 6: Job Lifecycle Tracking

### 6A: Add staleness detection

Add a `last_confirmed_active` column to the jobs table. Every time a job is seen in a source during a pipeline run, update this timestamp.

In `db.py` upsert logic: when a job already exists and is re-seen, update `last_confirmed_active = datetime.now()`.

### 6B: Add `scripts/staleness_check.py`

A separate script (can be called from the same GitHub Actions workflow, or a weekly cron) that:
1. Queries all active jobs where `last_confirmed_active` is older than 7 days
2. Marks them as `status = 'likely_closed'` (not archived — they may reappear)
3. Jobs with `status = 'likely_closed'` show in the table with a muted/grayed style
4. Jobs with `status = 'likely_closed'` for 14+ days get auto-archived

### 6C: WordPress display

Jobs with `likely_closed` status show with reduced opacity and a "(may be closed)" label. They still appear in the table but are visually deprioritized.

### 6D: Tests

- Verify `last_confirmed_active` updates on re-seen
- Verify staleness threshold (7 days)
- Verify auto-archive after 14 days stale

---

## PHASE 7: Stats Aggregation Table

### 7A: Create `monthly_stats` table in Turso

```sql
CREATE TABLE IF NOT EXISTS monthly_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stat_date TEXT NOT NULL,        -- YYYY-MM-DD (snapshot date)
    stat_type TEXT NOT NULL,        -- 'category_count', 'vendor_count', 'seniority_count', etc.
    stat_key TEXT NOT NULL,         -- the category/vendor/seniority value
    stat_value INTEGER NOT NULL,    -- the count
    UNIQUE(stat_date, stat_type, stat_key)
);
```

### 7B: Create `src/processors/stats_aggregator.py`

Runs at the end of each pipeline execution. Queries the jobs table and writes summary rows:

```python
def aggregate_daily_stats(conn) -> None:
    today = datetime.now().strftime("%Y-%m-%d")

    # Jobs by category
    rows = conn.execute(
        "SELECT category, COUNT(*) FROM jobs WHERE status='active' GROUP BY category"
    ).fetchall()
    for cat, count in rows:
        _upsert_stat(conn, today, "category_count", cat or "General PA", count)

    # Jobs by seniority
    rows = conn.execute(
        "SELECT seniority, COUNT(*) FROM jobs WHERE status='active' GROUP BY seniority"
    ).fetchall()
    for sen, count in rows:
        _upsert_stat(conn, today, "seniority_count", sen or "Unknown", count)

    # Remote distribution
    rows = conn.execute(
        "SELECT is_remote, COUNT(*) FROM jobs WHERE status='active' GROUP BY is_remote"
    ).fetchall()
    for remote, count in rows:
        _upsert_stat(conn, today, "remote_count", remote or "unknown", count)

    # Vendor mentions (from comma-separated vendors_mentioned column)
    rows = conn.execute(
        "SELECT vendors_mentioned FROM jobs WHERE status='active' AND vendors_mentioned IS NOT NULL AND vendors_mentioned != ''"
    ).fetchall()
    vendor_counts = {}
    for (vendors_str,) in rows:
        for v in vendors_str.split(","):
            v = v.strip()
            if v:
                vendor_counts[v] = vendor_counts.get(v, 0) + 1
    for vendor, count in vendor_counts.items():
        _upsert_stat(conn, today, "vendor_count", vendor, count)

    # Top hiring companies
    rows = conn.execute(
        "SELECT company, COUNT(*) as c FROM jobs WHERE status='active' GROUP BY company ORDER BY c DESC LIMIT 20"
    ).fetchall()
    for company, count in rows:
        _upsert_stat(conn, today, "company_count", company, count)

    # Salary percentiles by seniority
    for seniority in ["IC", "Manager", "Senior Manager", "Director", "VP", "Executive"]:
        rows = conn.execute(
            "SELECT salary_min FROM jobs WHERE status='active' AND seniority=? AND salary_min > 0 ORDER BY salary_min",
            (seniority,)
        ).fetchall()
        if len(rows) >= 3:
            vals = [r[0] for r in rows]
            import statistics
            _upsert_stat(conn, today, "salary_p25", seniority, int(statistics.quantiles(vals, n=4)[0]))
            _upsert_stat(conn, today, "salary_p50", seniority, int(statistics.median(vals)))
            _upsert_stat(conn, today, "salary_p75", seniority, int(statistics.quantiles(vals, n=4)[2]))

    # Total active count for trend line
    total = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='active'").fetchone()[0]
    _upsert_stat(conn, today, "total_active", "all", total)
```

### 7C: Integrate into collector

Call `aggregate_daily_stats(conn)` right before `ping_healthcheck()` at the end of `run()`.

### 7D: Tests

- Verify stat rows are created correctly
- Verify UNIQUE constraint handles re-runs on the same day (upsert)
- Verify vendor counting from comma-separated strings
- Verify salary percentile calculation with edge cases

---

## PHASE 8: WordPress Dashboard Shortcode

### 8A: Add `[job_dashboard]` shortcode to `job-monitor.php`

This shortcode renders a dashboard page with charts. The data comes from the `monthly_stats` table, pushed to WordPress as a JSON transient during the pipeline's WordPress publish step.

### 8B: Publisher update

In `src/publishers/wordpress.py`, after publishing jobs, also push dashboard data:
```python
def publish_dashboard_data(stats: dict, **wp_kwargs):
    """POST aggregated stats to a new WP REST endpoint for the dashboard."""
    # ... POST to /wp-json/job-monitor/v1/dashboard-stats
```

### 8C: PHP dashboard rendering

Use ApexCharts (MIT licensed, CDN hosted) for responsive charts. The shortcode renders:

1. **Jobs by Category** — horizontal bar chart
2. **Seniority Distribution** — donut/pie chart
3. **Remote vs Hybrid vs On-site** — donut chart
4. **Top 10 Hiring Companies** — horizontal bar chart
5. **Posting Volume Over Time** — line chart (from daily total_active stats)
6. **Most Mentioned Tools** — horizontal bar chart (from vendor_count stats)

Each chart is a `<div>` with a unique ID, and a `<script>` block initializing ApexCharts with the data from the transient.

### 8D: Create the Dashboard page in WordPress

Instructions for Victor: create a new WordPress page titled "Dashboard" with the shortcode `[job_dashboard]`. Add it to the navigation menu alongside Jobs and Archive.

### 8E: Tests

- Verify dashboard stats JSON is correctly formatted
- Verify REST endpoint accepts and stores dashboard data
- PHP brace balance check after plugin changes

---

## PHASE 9: Schema.org JobPosting + SEO

### 9A: Add JSON-LD structured data

In the `[job_table]` shortcode, output a `<script type="application/ld+json">` block for each job with Schema.org `JobPosting` markup:

```json
{
  "@context": "https://schema.org",
  "@type": "JobPosting",
  "title": "Senior Manager, People Analytics",
  "hiringOrganization": {"@type": "Organization", "name": "Netflix"},
  "jobLocation": {"@type": "Place", "address": "Los Gatos, CA"},
  "datePosted": "2026-04-14",
  "employmentType": "FULL_TIME",
  "jobLocationType": "TELECOMMUTE",
  "baseSalary": {
    "@type": "MonetaryAmount",
    "currency": "USD",
    "value": {"@type": "QuantitativeValue", "minValue": 120000, "maxValue": 180000, "unitText": "YEAR"}
  }
}
```

Only emit for jobs with sufficient data. Skip jobs missing title or company.

### 9B: Tests

- Verify JSON-LD output is valid JSON
- Verify conditional fields (salary only when present, remote only when confirmed)

---

## PHASE 10: Final Integration + Test Sweep

### 10A: Update GitHub Actions workflow

Add `python-jobspy` to requirements install. Verify all new source modules are imported in collector.

### 10B: Add shadow log artifact upload (if not already done)

```yaml
- name: Upload shadow log
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: shadow-log-${{ github.run_number }}
    path: shadow_log.jsonl
    retention-days: 14
    if-no-files-found: ignore
```

### 10C: Full test suite

Run all tests. Verify count. All must pass.

### 10D: Commit

Single commit: `feat: round 3 — source expansion, vendor extraction, lifecycle, dashboard (Phase 1-10)`

Do NOT push. Wait for review.

---

## Post-deployment steps (for Victor, not Claude Code)

1. Re-upload the WordPress plugin zip (same process as before)
2. Create a "Dashboard" page in WordPress with `[job_dashboard]` shortcode
3. Run the ATS slug discovery script once: `python scripts/discover_ats_slugs.py`
4. Trigger a pipeline run and verify new sources appear
5. Check the dashboard page renders charts
6. Add `category` filter to the Brevo email digest template
