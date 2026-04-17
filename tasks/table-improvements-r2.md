# Table Improvements Round 2 — Task File for Claude Code

Read CLAUDE.md first. Implement in order. Test after each phase. Do NOT push or commit until approved.

---

## PHASE A: Follow Redirects to Get Real Apply URLs

The #1 user complaint: clicking "Apply" lands on Jooble or an aggregator redirect page, not the company careers site.

### Fix in `src/processors/enrichment.py`

In `enrich_job()`, after the HTTP GET, capture the final URL after all redirects:

```python
resp = requests.get(url, timeout=10, headers=HEADERS, allow_redirects=True)
final_url = resp.url  # URL after all 301/302 redirects resolved

# If we followed redirects and landed on a different domain, that's the real company page
if final_url and final_url != url:
    from urllib.parse import urlparse
    orig_domain = urlparse(url).netloc.lower()
    final_domain = urlparse(final_url).netloc.lower()
    # Only update if we actually left the aggregator domain
    if final_domain != orig_domain:
        job["apply_url"] = final_url
```

This is already happening implicitly (requests follows redirects by default), but we're not storing the final URL. The fix is to capture `resp.url` and update `apply_url` when the domain changes.

### Also update source-level apply_url logic

In `src/sources/jooble.py`: Jooble's `link` field is always a redirect. Mark these with a flag so the enrichment step knows to prefer the redirected URL:
```python
job["_apply_url_is_redirect"] = True
```

In `src/sources/adzuna.py`: Same — `redirect_url` is a redirect by definition.

### Deduplicator: prefer better apply_url

In `src/processors/deduplicator.py`, when two jobs are identified as duplicates, keep the one with the better `apply_url`. "Better" means:
1. Direct company URL (domain is not jooble.org, adzuna.com, indeed.com, google.com) > aggregator redirect
2. HTTPS > HTTP
3. Non-empty > empty

Add a helper: `_better_apply_url(a, b) -> dict` that returns the job dict with the better apply_url. Use this when choosing which duplicate to keep.

Write tests: verify redirect following captures final URL, verify deduplicator prefers direct company URLs.

---

## PHASE B: Days Since Posted with Freshness Indicators

### Python-side: populate `date_posted` more reliably

In each source module, ensure `date_posted` is extracted when available:
- JSearch: `job_posted_at_datetime_utc` (already done, verify)
- Jooble: `updated` field — rename internally to `date_posted` but flag as approximate
- Adzuna: `created` (already done, verify)
- USAJobs: `PublicationStartDate` (already done, verify)
- Google Alerts: `published` (already done, verify)

### WordPress plugin: display "X days ago" with color coding

Replace the "First Seen" column with a "Posted" column that shows:
- If `date_posted` exists: "X days ago" (calculated from today - date_posted)
- If `date_posted` is empty: "Seen X days ago" in italic/muted text (using first_seen_date)

Add freshness color coding via CSS classes:
```css
.freshness-hot { color: #155724; font-weight: bold; }    /* 1-3 days: green */
.freshness-warm { color: #856404; }                       /* 4-7 days: yellow/amber */
.freshness-cool { color: #856404; opacity: 0.7; }         /* 8-14 days: faded amber */
.freshness-stale { color: #6c757d; }                      /* 15+ days: gray */
```

PHP logic:
```php
$date_posted = get_post_meta($j->ID, 'date_posted', true);
$first_seen = get_post_meta($j->ID, 'first_seen_date', true);
$ref_date = $date_posted ?: $first_seen;
$label = $date_posted ? '' : 'Seen ';
if ($ref_date) {
    $days = (int)((time() - strtotime($ref_date)) / 86400);
    if ($days <= 0) $days = 0;
    $class = $days <= 3 ? 'freshness-hot' : ($days <= 7 ? 'freshness-warm' : ($days <= 14 ? 'freshness-cool' : 'freshness-stale'));
    $text = $label . ($days === 0 ? 'Today' : ($days === 1 ? '1 day ago' : $days . ' days ago'));
    $cell = '<span class="' . $class . '">' . $text . '</span>';
} else {
    $cell = '<span class="freshness-stale">Unknown</span>';
}
```

Add a "New" badge for jobs first seen in the last 24 hours:
```php
$is_new = $first_seen === date('Y-m-d');
$badge = $is_new ? ' <span class="badge-new">NEW</span>' : '';
```

```css
.badge-new { background: #dc3545; color: #fff; padding: 1px 5px; border-radius: 3px; font-size: 0.7em; vertical-align: middle; }
```

Apply to both `[job_table]` and `[job_archive_table]`.

Write tests: verify date_posted extraction from each source, verify freshness class assignment.

---

## PHASE C: Multi-Select Filters Above the Table

Replace the current `<tfoot>` text/dropdown filters with a filter bar ABOVE the table using checkbox-style multi-select.

### Implementation approach

Use DataTables SearchPanes extension if available, OR build custom filter pills. Since we're self-hosting DataTables and want to keep it lightweight, build custom filter HTML above the table.

### Filter bar HTML (generated in PHP)

```html
<div class="jm-filters">
  <div class="jm-filter-group">
    <label>Level:</label>
    <div class="jm-checkboxes" data-column="3">
      <!-- Populated dynamically by JS from column data -->
    </div>
  </div>
  <div class="jm-filter-group">
    <label>Remote:</label>
    <div class="jm-checkboxes" data-column="4">
    </div>
  </div>
  <div class="jm-filter-group">
    <label>Source:</label>
    <div class="jm-checkboxes" data-column="6">
    </div>
  </div>
  <div class="jm-filter-group">
    <label>Relevance:</label>
    <div class="jm-checkboxes" data-column="7">
    </div>
  </div>
</div>
```

### JavaScript for checkbox multi-select

```javascript
initComplete: function() {
    var api = this.api();
    // For each filter group, populate checkboxes from unique column values
    jQuery('.jm-checkboxes').each(function() {
        var $container = jQuery(this);
        var colIdx = parseInt($container.data('column'));
        var column = api.column(colIdx);
        var values = [];
        column.data().unique().sort().each(function(d) {
            var text = jQuery('<div>').html(d).text().trim();
            if (text && text !== '') values.push(text);
        });
        values.forEach(function(val) {
            var id = 'jm-f-' + colIdx + '-' + val.replace(/\W/g, '_');
            $container.append(
                '<label class="jm-chip"><input type="checkbox" id="' + id + '" value="' + val + '" checked> ' + val + '</label> '
            );
        });
        // On any checkbox change, rebuild the column regex filter
        $container.on('change', 'input[type=checkbox]', function() {
            var checked = [];
            $container.find('input:checked').each(function() {
                checked.push(jQuery.fn.dataTable.util.escapeRegex(jQuery(this).val()));
            });
            if (checked.length === 0) {
                column.search('').draw();
            } else {
                column.search('^(' + checked.join('|') + ')$', true, false).draw();
            }
        });
    });
    // Keep text filters for Title, Company, Location
    // ... (existing text filter logic for freeform columns)
}
```

### CSS for filter chips

```css
.jm-filters { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; padding: 12px; background: #f8f9fa; border-radius: 6px; }
.jm-filter-group { display: flex; align-items: flex-start; gap: 6px; }
.jm-filter-group label:first-child { font-weight: 600; white-space: nowrap; padding-top: 4px; }
.jm-checkboxes { display: flex; flex-wrap: wrap; gap: 4px; }
.jm-chip { display: inline-flex; align-items: center; gap: 3px; padding: 3px 8px; background: #fff; border: 1px solid #dee2e6; border-radius: 4px; font-size: 0.82em; cursor: pointer; }
.jm-chip input { margin: 0; }
.jm-chip:has(input:not(:checked)) { opacity: 0.5; background: #e9ecef; }
```

Remove the `<tfoot>` filter row. Add text search inputs for Title/Company/Location above the table alongside the checkbox filters.

Apply to both shortcodes.

---

## PHASE D: Add LLM Classification Column

### Add "Relevance" column to the table

Display the `llm_classification` field as a human-readable label:
- RELEVANT → "Relevant" with green text
- PARTIALLY_RELEVANT → "Partial" with amber text
- Empty/null → "Auto" (was auto-included by keyword score alone)

Add this as a filterable column in the checkbox filter bar.

### PHP display logic

```php
$classification = get_post_meta($j->ID, 'llm_classification', true);
switch ($classification) {
    case 'RELEVANT': $rel_cell = '<span style="color:#155724">Relevant</span>'; break;
    case 'PARTIALLY_RELEVANT': $rel_cell = '<span style="color:#856404">Partial</span>'; break;
    default: $rel_cell = '<span style="color:#6c757d">Auto</span>'; break;
}
```

### Update REST endpoint

Ensure `llm_classification` is already in the allowed meta fields list (it should be from the original plugin — verify).

---

## PHASE E: Stricter Deduplication for Same-Company Repeats

The table shows 4x "Employee Listening Specialist — Deloitte" and 2x "Employee Experience (EX) Advisor — Forsta Inc". These are the same role posted for multiple locations.

### Lower dedup threshold for exact company match

In `src/processors/deduplicator.py`, when the company names match exactly (after normalization), lower the duplicate threshold from 85 to 75. This catches cases where the same company posts the same title for different locations.

```python
# Current: DUPLICATE_THRESHOLD = 85
# Change: use 75 when company_sim >= 95 (near-exact company match)
effective_threshold = 75 if company_sim >= 95 else DUPLICATE_THRESHOLD
```

### Collapse multi-location dupes

When duplicates are detected with same company + same title but different locations, instead of dropping the duplicate, merge the locations:
- Keep the first job as the primary
- Append unique locations from duplicates: "New York, NY; Chicago, IL; Atlanta, GA"
- Or if 3+ locations: "Multiple Locations (4)"
- Set location to the merged string

### Test: verify Deloitte 4x → 1x with "Multiple Locations (4)"

---

## PHASE F: Improve Seniority Detection Accuracy

The current regex-based extractor returns "Unknown" too often because many PA/EL titles don't contain standard seniority markers.

### Add PA/EL-specific title patterns

```python
# Add to SENIORITY_MAP before the generic patterns:
(r"\b(staff)\s+(program\s+manager|scientist|engineer)\b", "Senior IC"),
(r"\b(fellow)\b", "Senior IC"),  # "AI-Driven People Analytics Fellow"
(r"\bintern\b", "Intern"),
(r"\b(managing\s+director)\b", "Executive"),
(r"\b(senior\s+associate|sr\.?\s+associate)\b", "Senior IC"),
(r"\bassociate\b", "IC"),  # but NOT "senior associate"
```

### LLM fallback for remaining unknowns

The seniority extractor already falls back to the LLM's answer when available. But when the LLM also returns Unknown or null, try one more heuristic: check the job description for salary range. If salary is available:
- $200K+ → likely Director/VP/Executive
- $150K-$200K → likely Senior Manager/Director
- $100K-$150K → likely Manager/Senior IC
- $60K-$100K → likely IC
- Under $60K → likely Intern/Entry

This is imprecise but better than "Unknown" for filtering purposes. Mark these as `seniority_confidence: "inferred"`.

### Add "Intern" level to the seniority taxonomy

The current system has 8 levels (Executive through IC) plus Unknown. Add "Intern" as a distinct level since intern postings appear in results (Palo Alto Networks PhD Intern, Campbell's People Analytics Intern, etc.). Victor probably wants to filter these out.

---

## PHASE G: WordPress Plugin Updates + Final Tests

### Column order for [job_table]

Final column order:
1. Title (text filter, clickable link to source_url)
2. Company (text filter)
3. Location (text filter, with confidence badge)
4. Level (checkbox filter)
5. Remote (checkbox filter, with confidence badge)
6. Salary (with confidence badge)
7. Relevance (checkbox filter: Relevant / Partial / Auto)
8. Source (checkbox filter)
9. Apply (button linking to apply_url)
10. Posted (days ago with freshness color + NEW badge)

### Column order for [job_archive_table]

Same 10 columns plus:
11. Days Active
12. Archived Date

### Update WordPress publisher _WP_FIELDS

Add `date_posted` to the whitelist if not already present (needed for freshness calculation).

### Final test sweep

- Verify redirect following works with mocked 301/302 responses
- Verify freshness class assignment for various day counts
- Verify dedup collapses same-company/same-title across locations
- Verify seniority detects Intern, Fellow, Managing Director, Staff
- Verify classification column displays correctly
- Verify all new fields flow through collector → Turso → WordPress

Run full test suite. All tests must pass.

---

## PHASE H: Salary Sorting Fix

DataTables sorts "$120K-$180K" alphabetically, not numerically. Fix by adding a hidden sort value.

### PHP: add data-order attribute to salary cell

```php
$salary_min = get_post_meta($j->ID, 'salary_min', true);
$salary_display = esc_html(get_post_meta($j->ID, 'salary_range', true));
$sort_val = $salary_min ? (int)$salary_min : 0;
echo '<td data-order="' . $sort_val . '">' . $salary_display . jm_confidence_badge($salary_conf) . '</td>';
```

DataTables automatically uses `data-order` for sorting when present. "$120K-$180K" displays visually, but sorts by 120000.

---

## PHASE I: Job Sub-Category Classification

Add a "Category" column that classifies each job into a functional bucket. This helps Victor filter by function type (e.g., show only Employee Listening roles, hide HRIS/Systems roles).

### Categories (mutually exclusive, first match wins)

```python
JOB_CATEGORIES = [
    # Order matters — first match wins. Check title first, then description.
    # Employee Listening (most specific — check first)
    (r"\b(employee\s+listening|voice\s+of\s+(the\s+)?employee|continuous\s+listening|listening\s+strategy|survey\s+analyst.*listening)\b", "Employee Listening"),
    # HRIS & Systems (check before PA — "HRIS & People Analytics" should be HRIS)
    (r"\bHRIS\b", "HRIS & Systems"),
    # Research / I-O Psychology
    (r"\b(people\s+scien|research\s+scien|I-O\s+psych|industrial.organizational|behavioral\s+scien|psychometri)\b", "Research / I-O"),
    # Data Engineering
    (r"\b(data\s+engineer|analytics\s+engineer|automation\s+engineer|data\s+architect)\b", "Data Engineering"),
    # Pay Equity
    (r"\b(pay\s+equity|workplace\s+equity|compensation\s+analy)\b", "Pay Equity"),
    # Workforce Planning
    (r"\b(workforce\s+planning|SWP|workforce\s+optimi)\b", "Workforce Planning"),
    # Talent Intelligence
    (r"\b(talent\s+intelligen|workforce\s+intelligen|skills\s+intelligen)\b", "Talent Intelligence"),
    # EX / Culture
    (r"\b(employee\s+experience|EX\s+advisor|culture\s+analy|engagement\s+manager|engagement\s+director)\b", "EX & Culture"),
    # People Analytics (broadest — catch-all for PA roles)
    (r"\b(people\s+analy|HR\s+analy|workforce\s+analy|talent\s+analy|human\s+capital\s+analy)\b", "People Analytics"),
    # Consulting (by company, not title)
    # This is handled separately — see below
]

CONSULTING_COMPANIES = {
    "deloitte", "pwc", "mckinsey", "ey", "kpmg", "mercer", "wtw",
    "korn ferry", "kincentric", "bain", "bcg", "accenture",
}
```

### Implementation: `src/processors/category.py`

```python
def classify_category(title: str, company: str, description: str = "") -> str:
    """Return the functional sub-category for a job."""
    text = f"{title} {description[:500]}"
    for pattern, category in JOB_CATEGORIES:
        if re.search(pattern, text, re.IGNORECASE):
            return category
    # Check consulting companies
    if company.lower().strip() in CONSULTING_COMPANIES:
        return "Consulting"
    return "General PA"  # fallback for anything that passed keyword filter
```

### Integration

Call `classify_category()` in `collector.py` after seniority extraction:
```python
from src.processors.category import classify_category
for job in to_publish:
    job["category"] = classify_category(job.get("title", ""), job.get("company", ""), job.get("description", ""))
```

The `category` column already exists in the database schema — it was defined in the original build but never populated.

### Display in WordPress

Add "Category" as a filterable column with checkbox multi-select in the filter bar.

Write tests: verify each category matches expected titles, verify consulting company detection, verify fallback to "General PA".

---

## PHASE J: Description-Based Remote/Salary Extraction (Pre-Enrichment)

The enrichment module currently only extracts remote/salary from the SOURCE PAGE (fetched via HTTP). But the aggregator's job description text (which we already have) often contains this information buried in the body text.

### Two-pass extraction logic

**Pass 1 (free, no HTTP): Extract from aggregator description text**

Before the enrichment HTTP fetch, run the remote/salary/location regex extractors on the job's `description` field. This catches cases like:
- "This is a remote-eligible position"
- "Salary range: $120,000 - $180,000"
- "Hybrid - 3 days in office, 2 remote"
- "Open to remote for exceptional candidates"

```python
def _pre_enrich_from_description(job: dict) -> None:
    """Extract remote/salary/location from the aggregator's description text.
    Only fills in fields that are currently empty/unknown."""
    desc = job.get("description") or ""
    if not desc:
        return
    
    # Remote status from description
    if job.get("is_remote") in (None, "", "unknown"):
        remote = _extract_remote_status(desc)
        if remote:
            job["is_remote"] = remote
            job["remote_confidence"] = "inferred"
    
    # Salary from description
    if not job.get("salary_min"):
        salary = _extract_salary(desc)
        if salary:
            job["salary_min"] = salary["min"]
            job["salary_max"] = salary["max"]
            job["salary_range"] = salary["range_str"]
            job["salary_confidence"] = "inferred"
    
    # Location from description (only if missing)
    if not job.get("location"):
        location = _extract_location(desc)
        if location:
            job["location"] = location
            job["location_confidence"] = "inferred"
```

**Pass 2 (HTTP fetch): Enrich from source page (existing logic)**

The existing enrichment module runs after Pass 1. If the source page provides better data, it overwrites with `confidence: "confirmed"`.

**Pass 3 (default inference): If still unknown, apply defaults**

After both passes, if `is_remote` is still "unknown":
```python
if job.get("is_remote") in (None, "", "unknown"):
    job["is_remote"] = "on-site"
    job["remote_confidence"] = "assumed"
```

This adds a fourth confidence level: "assumed" (displayed with no badge, or a faint "?" badge). The logic: if neither the aggregator, the description text, nor the source page mentions remote/hybrid, the job is almost certainly on-site.

### Also add to LLM prompt

Since the LLM already reads title + company during classification, add remote/salary extraction to the JSON response schema at zero additional cost:

```
Also extract if available:
- "remote_status": "remote" | "hybrid" | "on-site" | "unknown" (based on title and any context you have)
- "salary_hint": "$XXK-$XXK" or null (if salary range is mentioned)
```

This adds ~5 tokens to the response. The LLM's answer serves as a tiebreaker when regex extraction is ambiguous.

### Confidence hierarchy (highest wins)

1. `confirmed` — extracted from source page AND matches aggregator data
2. `aggregator_only` — aggregator provided it but source page didn't confirm
3. `inferred` — extracted from description text by regex or LLM
4. `assumed` — defaulted (e.g., "on-site" when nothing mentions remote)
5. `unverified` — no data from any source

### Update confidence badge display

```css
.confidence-assumed { background: #f8f9fa; color: #adb5bd; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; }
```

Show "?" for assumed values so Victor knows to verify manually if interested.

Write tests: verify description-based extraction runs before HTTP enrichment, verify assumed defaults apply when all sources are silent, verify LLM remote/salary hint is captured.

---

## Summary of display after all changes

```
┌─────────── Filter Bar (above table) ─────────────────────────────────────────────┐
│ Level:     [✓ Manager] [✓ Sr Manager] [✓ Director] [ ] IC [ ] Intern            │
│ Remote:    [✓ remote] [✓ hybrid] [ ] on-site                                    │
│ Category:  [✓ People Analytics] [✓ Employee Listening] [ ] HRIS [ ] Consulting  │
│ Relevance: [✓ Relevant] [✓ Partial] [ ] Auto                                    │
│ Source:    [✓ jsearch] [✓ jooble] [✓ adzuna]                                    │
│ Title: [________] Company: [________] Location: [________]                       │
└──────────────────────────────────────────────────────────────────────────────────┘

| Title ▲▼          | Company   | Location        | Category          | Level     | Remote    | Salary ▲▼      | Relevance | Source  | Apply    | Posted          |
|-------------------|-----------|-----------------|-------------------|-----------|-----------|----------------|-----------|---------|----------|-----------------|
| Survey Analyst... | Netflix   | Los Gatos, CA ✓ | Employee Listening| IC        | hybrid ✓  | $120K-$180K ✓  | Relevant  | jsearch | Apply →  | 2 days ago NEW  |
| Sr Manager, PA    | WWE       | Stamford, CT •  | People Analytics  | Sr Manager| on-site ? |                | Relevant  | jooble  | Apply →  | 5 days ago      |
| EL Specialist     | Deloitte  | Multiple (4)    | Consulting        | IC        | hybrid    |                | Relevant  | adzuna  | Apply →  | 3 days ago      |
| PA Automation Eng | OpenAI    | San Francisco ✓ | Data Engineering  | Senior IC | remote ✓  | $180K-$250K ✓  | Relevant  | jsearch | Apply →  | Today NEW       |
```

Key visual indicators:
- ✓ green badge = confirmed from source page
- • gray badge = aggregator data only  
- ? faint badge = assumed/inferred (no source mentioned it)
- NEW red badge = found today
- Salary column sorts numerically (by salary_min) despite displaying "$120K-$180K"
- Freshness: green text (1-3 days) → amber (4-7) → faded amber (8-14) → gray (15+)
```
