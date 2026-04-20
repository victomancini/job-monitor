<?php
/**
 * Plugin Name: Job Monitor
 * Description: Custom post type, REST endpoint, and archival cron for employee listening job monitoring
 * Version: 2.0
 * Author: Job Monitor Bot
 */

defined('ABSPATH') || exit;

// ── Custom Post Type ──────────────────────────────────────
add_action('init', function() {
    register_post_type('job_listing', [
        'labels' => [
            'name' => 'Job Listings',
            'singular_name' => 'Job Listing',
            'add_new_item' => 'Add New Job Listing',
            'edit_item' => 'Edit Job Listing',
            'all_items' => 'All Job Listings',
            'search_items' => 'Search Job Listings',
        ],
        'public' => true,
        'show_in_rest' => false,  // Custom endpoint handles data ingestion; hide default REST to prevent public data exposure
        'supports' => ['title', 'editor', 'custom-fields'],
        'has_archive' => true,
        'rewrite' => ['slug' => 'jobs'],
        'menu_icon' => 'dashicons-businessman',
    ]);
    register_post_status('archived', [
        'label' => 'Archived',
        'public' => false,
        'internal' => true,
        'exclude_from_search' => true,
        'show_in_admin_all_list' => true,
        'show_in_admin_status_list' => true,
        'label_count' => _n_noop('Archived (%s)', 'Archived (%s)'),
    ]);
});

// ── Meta Fields ───────────────────────────────────────────
add_action('init', function() {
    $fields = ['company','location','location_country','salary_min','salary_max',
        'salary_range','source_url','apply_url','external_id','source_name','category','seniority',
        'fit_score','is_remote','work_arrangement','first_seen_date','last_seen_active',
        'archived_date','days_active','job_status','keywords_matched','description_snippet',
        'keyword_score','llm_classification','llm_confidence','llm_provider',
        // R8-M1: llm_reasoning is one sentence of LLM explanation. Stored in
        // WP post meta for debugging but NOT shown in the default table
        // render — inspect via the admin UI or REST.
        'llm_reasoning',
        // Phase F: enrichment + confidence fields
        'location_confidence','salary_confidence','remote_confidence',
        'enrichment_source','enrichment_date',
        // Phase B/F (R2): date_posted for freshness; seniority_confidence for badge
        'date_posted','seniority_confidence',
        // Phase 5 (R3): comma-separated vendor/tool mentions from description
        'vendors_mentioned',
        // Phase 6 (R3): lifecycle state ('active' | 'likely_closed')
        'lifecycle_status',
        // R11 Phase 0: Python-computed integer days since posting; sortable
        // without re-parsing dates in the site's local timezone. NEW badge
        // trigger sourced from Python's upsert return instead of fragile
        // first_seen_date == today comparison.
        'days_since_posted', 'is_brand_new',
        // R11 Phase 6: consensus-voting transparency. When multiple sources
        // agreed on a value (e.g., greenhouse + text_classifier both say
        // hybrid), we render "verified by N sources" in the cell tooltip.
        'remote_vote_confidence', 'remote_vote_sources', 'remote_vote_agreement',
        'work_arrangement_vote_confidence', 'work_arrangement_vote_sources'];
    foreach ($fields as $field) {
        register_post_meta('job_listing', $field, [
            'show_in_rest' => true,
            'single' => true,
            'type' => 'string',
            'sanitize_callback' => function($val) { return sanitize_text_field(mb_substr($val, 0, 500)); },
            'auth_callback' => function() { return current_user_can('edit_posts'); }
        ]);
    }
});

// Defense-in-depth: when the site defines JM_SHARED_SECRET in wp-config.php,
// writes to the ingestion endpoints also require a matching X-JM-Secret header.
// Falls open (no secret check) when the constant is not defined so existing
// installs keep working until the secret is provisioned on both sides.
// The constant-time compare guards against timing side channels.
function jm_check_shared_secret($request) {
    if (!defined('JM_SHARED_SECRET') || !JM_SHARED_SECRET) {
        return true;  // no secret configured → skip header check
    }
    $sent = $request->get_header('x-jm-secret');
    if (!$sent || !hash_equals(JM_SHARED_SECRET, $sent)) {
        return new WP_Error('jm_bad_secret', 'Invalid or missing X-JM-Secret', ['status' => 403]);
    }
    return true;
}

function jm_write_permission($request) {
    if (!current_user_can('edit_posts')) return false;
    $sec = jm_check_shared_secret($request);
    if ($sec !== true) return $sec;
    return true;
}

// ── REST Endpoints ────────────────────────────────────────
add_action('rest_api_init', function() {
    register_rest_route('jobmonitor/v1', '/update-jobs', [
        'methods' => 'POST',
        'callback' => 'jm_batch_update',
        'permission_callback' => 'jm_write_permission',
    ]);
    register_rest_route('jobmonitor/v1', '/stats', [
        'methods' => 'GET',
        'callback' => 'jm_get_stats',
        'permission_callback' => '__return_true',
    ]);
    // Phase 8 (R3): dashboard stats ingestion endpoint
    register_rest_route('jobmonitor/v1', '/dashboard-stats', [
        'methods' => 'POST',
        'callback' => 'jm_update_dashboard_stats',
        'permission_callback' => 'jm_write_permission',
    ]);
});

function jm_batch_update($request) {
    $data = $request->get_json_params();
    if (empty($data['jobs']) || !is_array($data['jobs'])) {
        return new WP_REST_Response(['error' => 'No jobs array'], 400);
    }
    $results = ['created' => 0, 'updated' => 0, 'errors' => 0, 'post_ids' => []];

    // Performance: defer counting and suspend cache during bulk insert
    wp_defer_term_counting(true);
    wp_suspend_cache_addition(true);
    // Temporarily unhook SEO plugins (Rank Math) to avoid 10-20 extra DB ops per insert
    remove_all_actions('save_post');
    remove_all_actions('wp_insert_post');

    foreach ($data['jobs'] as $job) {
        if (empty($job['external_id']) || empty($job['title'])) {
            $results['errors']++;
            continue;
        }
        // Include 'archived' so a previously-archived job that reappears in a
        // new batch gets re-activated in place instead of duplicated.
        $existing = get_posts([
            'post_type' => 'job_listing',
            'meta_key' => 'external_id',
            'meta_value' => sanitize_text_field($job['external_id']),
            'posts_per_page' => 1,
            'post_status' => ['publish', 'archived'],
        ]);
        $allowed = ['company','location','location_country','salary_min','salary_max',
            'salary_range','source_url','apply_url','external_id','source_name','category','seniority',
            'fit_score','is_remote','work_arrangement','keywords_matched','description_snippet',
            'keyword_score','llm_classification','llm_confidence','llm_provider',
            'llm_reasoning',  // R8-M1: debug-only meta
            // Phase F3: enrichment + confidence fields
            'location_confidence','salary_confidence','remote_confidence',
            'enrichment_source','enrichment_date',
            // Phase B/F (R2): date_posted for freshness; seniority_confidence for badge
            'date_posted','seniority_confidence',
            // Phase 5 (R3): comma-separated vendor/tool mentions from description
            'vendors_mentioned',
            // Phase 6 (R3): lifecycle state ('active' | 'likely_closed')
            'lifecycle_status',
            // R11 Phase 0: integer days-since-posting for reliable numeric
            // sort; is_brand_new flag for NEW badge (1 when Turso created the
            // row this run, 0 otherwise). first_seen_date is handled out of
            // band below — NEVER overwritten with a later date here.
            'days_since_posted','is_brand_new',
            // R11 Phase 6: per-field consensus vote metadata for tooltip.
            'remote_vote_confidence','remote_vote_sources','remote_vote_agreement',
            'work_arrangement_vote_confidence','work_arrangement_vote_sources'];
        $meta = [];
        foreach ($allowed as $k) {
            if (isset($job[$k])) {
                $meta[$k] = sanitize_text_field(mb_substr($job[$k], 0, 500));
            }
        }
        if (isset($job['source_url'])) {
            $meta['source_url'] = esc_url_raw($job['source_url']);
        }
        if (isset($job['apply_url'])) {
            $meta['apply_url'] = esc_url_raw($job['apply_url']);
        }
        // UTC to match Python-side date stamping (see db.py _today()).
        $meta['last_seen_active'] = current_time('Y-m-d', true);
        $meta['job_status'] = 'active';

        if (!empty($existing)) {
            $post_id = $existing[0]->ID;
            // R11 Phase 0: first_seen_date is authoritative from Turso. Only
            // update WP meta if it's missing or the incoming value is earlier
            // (ISO YYYY-MM-DD strings compare correctly via strcmp). Never
            // overwrite with a later date — that was the NEW-today bug: if a
            // WP post was ever lost and recreated, the old INSERT path stamped
            // today's date on a job Turso had seen for weeks.
            if (isset($job['first_seen_date'])) {
                $existing_fsd = get_post_meta($post_id, 'first_seen_date', true);
                $incoming_fsd = sanitize_text_field($job['first_seen_date']);
                if (!$existing_fsd || strcmp($incoming_fsd, $existing_fsd) < 0) {
                    $meta['first_seen_date'] = $incoming_fsd;
                }
            }
            // R8-H3: include post_content so description rewrites from the
            // source (e.g., aggregator re-scrapes a longer description) are
            // not silently dropped after the first insert. Only update when
            // the payload actually carries a description.
            $update_args = ['ID' => $post_id, 'post_status' => 'publish', 'meta_input' => $meta];
            if (isset($job['description']) && $job['description'] !== '') {
                $update_args['post_content'] = wp_kses_post(mb_substr($job['description'], 0, 5000));
            }
            wp_update_post($update_args);
            $results['updated']++;
            $results['post_ids'][$job['external_id']] = $post_id;
        } else {
            // R11 Phase 0: prefer Python-supplied first_seen_date (Turso's
            // system of record). Falls back to today only for payloads
            // predating the change so we don't lose new inserts either way.
            $meta['first_seen_date'] = isset($job['first_seen_date'])
                ? sanitize_text_field($job['first_seen_date'])
                : current_time('Y-m-d', true);
            $pid = wp_insert_post([
                'post_type' => 'job_listing',
                'post_title' => sanitize_text_field(mb_substr($job['title'], 0, 200)),
                'post_content' => wp_kses_post(mb_substr($job['description'] ?? '', 0, 5000)),
                'post_status' => 'publish',
                'meta_input' => $meta,
            ]);
            if (is_wp_error($pid)) $results['errors']++; else { $results['created']++; $results['post_ids'][$job['external_id']] = $pid; }
        }
    }

    // Restore counting and cache
    wp_defer_term_counting(false);
    wp_suspend_cache_addition(false);

    // Invalidate shortcode transient cache
    delete_transient('jm_active_jobs_html');

    return new WP_REST_Response($results, 200);
}

function jm_get_stats($request) {
    $c = wp_count_posts('job_listing');
    return new WP_REST_Response([
        'active' => (int)($c->publish ?? 0),
        'archived' => (int)($c->archived ?? 0),
        'last_updated' => current_time('c'),
    ], 200);
}

// Phase 8 (R3): stash the dashboard payload as a 48-hour transient so the
// [job_dashboard] shortcode can read it without hitting the DB.
function jm_update_dashboard_stats($request) {
    $data = $request->get_json_params();
    if (!is_array($data)) {
        return new WP_REST_Response(['error' => 'invalid payload'], 400);
    }
    set_transient('jm_dashboard_stats', $data, 48 * HOUR_IN_SECONDS);
    return new WP_REST_Response(['ok' => true, 'stored_at' => current_time('c')], 200);
}

// ── Daily Archival Cron ───────────────────────────────────
// Two-step lifecycle mirrors src/publishers/archiver.py (Phase 6 R3):
//   Day 7+  → lifecycle_status='likely_closed' (still visible, muted in [job_table])
//   Day 21+ → post_status='archived' (moves to [job_archive_table])
// Previously this cron archived at day 7, which stomped the Python-side
// likely_closed state so the muted UI never appeared (code-review C1).
define('JM_LIKELY_CLOSED_DAYS', 7);
define('JM_ARCHIVE_DAYS', 21);

add_action('wp', function() {
    if (!wp_next_scheduled('jm_daily_archive')) {
        wp_schedule_event(time(), 'daily', 'jm_daily_archive');
    }
});
add_action('jm_daily_archive', function() {
    global $wpdb;
    // UTC for cutoff math: Python's db.py stores last_seen_date in UTC
    // (`datetime('now')`), so comparing against site-timezone-local dates
    // here caused off-by-one archival on edge timezones. Use gmdate().
    $closed_cutoff = gmdate('Y-m-d', time() - JM_LIKELY_CLOSED_DAYS * 86400);
    $archive_cutoff = gmdate('Y-m-d', time() - JM_ARCHIVE_DAYS * 86400);

    // Step 1: mark day-7+ stale jobs as 'likely_closed' (still post_status=publish,
    // just tagged via meta so the shortcode can render them muted).
    $to_mark = $wpdb->get_col($wpdb->prepare(
        "SELECT DISTINCT p.ID FROM {$wpdb->posts} p
         INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id AND pm.meta_key = 'last_seen_active'
         LEFT JOIN {$wpdb->postmeta} pm2 ON p.ID = pm2.post_id AND pm2.meta_key = 'lifecycle_status'
         WHERE p.post_type = 'job_listing'
           AND p.post_status = 'publish'
           AND pm.meta_value < %s
           AND (pm2.meta_value IS NULL OR pm2.meta_value <> 'likely_closed')
         LIMIT 100",
        $closed_cutoff
    ));
    foreach ($to_mark as $pid) {
        update_post_meta($pid, 'lifecycle_status', 'likely_closed');
    }

    // Step 2: jobs unseen for JM_ARCHIVE_DAYS days get fully archived.
    $to_archive = $wpdb->get_col($wpdb->prepare(
        "SELECT p.ID FROM {$wpdb->posts} p
         INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id AND pm.meta_key = 'last_seen_active'
         WHERE p.post_type = 'job_listing' AND p.post_status = 'publish' AND pm.meta_value < %s
         LIMIT 100",
        $archive_cutoff
    ));
    foreach ($to_archive as $pid) {
        $first = get_post_meta($pid, 'first_seen_date', true);
        $last = get_post_meta($pid, 'last_seen_active', true);
        $days = ($first && $last) ? max(1, round((strtotime($last) - strtotime($first)) / 86400)) : 0;
        wp_update_post(['ID' => $pid, 'post_status' => 'archived']);
        update_post_meta($pid, 'job_status', 'archived');
        update_post_meta($pid, 'archived_date', current_time('Y-m-d', true));
        update_post_meta($pid, 'days_active', (string)$days);
    }

    // Re-seen jobs come through jm_batch_update which explicitly resets
    // lifecycle_status='active' on the Python side, so recoveries flow naturally.

    // Invalidate both caches after lifecycle changes
    delete_transient('jm_active_jobs_html');
    delete_transient('jm_archived_jobs_html');
});
register_deactivation_hook(__FILE__, function() {
    $ts = wp_next_scheduled('jm_daily_archive');
    if ($ts) wp_unschedule_event($ts, 'jm_daily_archive');
});

// R11 Phase 6: consensus tooltip helper. When multiple sources agreed on
// the final value, render a `title=` attribute that surfaces the source
// list on hover — no CSS changes needed, uses the browser's native tooltip.
// Returns a trailing space + attribute (e.g. ' title="verified by 2 sources"')
// or empty string when there's no consensus data to show.
function jm_consensus_tooltip($agreement, $sources, $confidence) {
    $agreement = (int)$agreement;
    if ($agreement < 2) {
        return '';
    }
    $parts = [sprintf('verified by %d sources', $agreement)];
    if (!empty($sources)) {
        $parts[] = $sources;
    }
    if ($confidence !== '' && $confidence !== null) {
        $parts[] = sprintf('confidence %.2f', (float)$confidence);
    }
    return ' title="' . esc_attr(implode(' | ', $parts)) . '"';
}

// ── Phase F5: confidence badge helper ─────────────────────
// Produces a compact inline badge. Green = page-confirmed, gray = aggregator-only,
// empty string = unverified (no badge rendered).
function jm_confidence_badge($confidence) {
    if ($confidence === 'confirmed') {
        return ' <span class="confidence-confirmed" title="Confirmed from source page">&#10003;</span>';
    }
    if ($confidence === 'aggregator_only') {
        return ' <span class="confidence-aggregator" title="Aggregator data, not confirmed">&#8226;</span>';
    }
    // Phase J (R2) / Phase 1 (R3): inferred from description/LLM; assumed default (onsite fallback)
    if ($confidence === 'inferred') {
        return ' <span class="confidence-inferred" title="Extracted from description">~</span>';
    }
    if ($confidence === 'assumed') {
        return ' <span class="confidence-assumed" title="No data found; assumed on-site">?</span>';
    }
    return '';
}

// Inline CSS printed once per page (both shortcodes reuse).
function jm_inline_styles() {
    static $printed = false;
    if ($printed) return '';
    $printed = true;
    return <<<CSS
<style>
.confidence-confirmed { background:#d4edda; color:#155724; padding:2px 6px; border-radius:3px; font-size:0.8em; }
.confidence-aggregator { background:#e2e3e5; color:#383d41; padding:2px 6px; border-radius:3px; font-size:0.8em; }
.confidence-inferred { background:#fff3cd; color:#856404; padding:2px 6px; border-radius:3px; font-size:0.8em; }
.confidence-assumed { background:#f8f9fa; color:#adb5bd; padding:2px 6px; border-radius:3px; font-size:0.8em; }
.jm-apply-btn { display:inline-block; padding:4px 10px; background:#2271b1; color:#fff !important; border-radius:3px; text-decoration:none; font-size:0.85em; }
.jm-apply-btn:hover { background:#135e96; color:#fff; }
tfoot input, tfoot select { width:100%; box-sizing:border-box; font-size:0.85em; padding:2px 4px; }
/* Phase B (R2): freshness + NEW badge */
.freshness-hot { color:#155724; font-weight:bold; }
.freshness-warm { color:#856404; }
.freshness-cool { color:#856404; opacity:0.7; }
.freshness-stale { color:#6c757d; }
.badge-new { background:#dc3545; color:#fff; padding:1px 5px; border-radius:3px; font-size:0.7em; vertical-align:middle; margin-left:4px; }
/* Phase C (R2) + R9-P1-F: multi-select filter bar.
   !important on every layout prop is deliberate — WordPress themes routinely
   inject .screen-reader-text-style rules or `input[type=checkbox]{opacity:0}`
   visual-hiding patterns that nuke our chip checkboxes. We're defending the
   DOM against unknown parent CSS. */
.jm-filters { display:flex!important; flex-wrap:wrap!important; gap:12px!important; margin-bottom:16px!important; padding:12px!important; background:#f8f9fa!important; border-radius:6px!important; }
.jm-filter-group { display:flex!important; align-items:flex-start!important; gap:6px!important; }
.jm-filter-group > label:first-child { font-weight:600!important; white-space:nowrap!important; padding-top:4px!important; }
.jm-checkboxes { display:flex!important; flex-wrap:wrap!important; gap:4px!important; max-width:560px!important; }
.jm-chip { display:inline-flex!important; align-items:center!important; gap:3px!important; padding:3px 8px!important; background:#fff!important; border:1px solid #dee2e6!important; border-radius:4px!important; font-size:0.82em!important; cursor:pointer!important; }
/* Force checkbox visibility against any theme rule that hides them. */
.jm-chip input[type="checkbox"] { position:static!important; display:inline-block!important; opacity:1!important; width:auto!important; height:auto!important; margin:0!important; appearance:auto!important; -webkit-appearance:checkbox!important; -moz-appearance:checkbox!important; }
.jm-chip.jm-chip-off { opacity:0.5!important; background:#e9ecef!important; }
.jm-text-filters { display:flex!important; flex-direction:row!important; flex-wrap:wrap!important; gap:8px!important; align-items:center!important; }
.jm-text-filters input { width:auto!important; max-width:180px!important; padding:4px 8px!important; border:1px solid #dee2e6!important; border-radius:4px!important; font-size:0.85em!important; }
/* Phase 6 (R3) + R-audit Issue 2f: muted styling for likely-closed jobs.
   Opacity 0.5 per spec; "Check →" button is grayed out vs. the normal blue Apply. */
tr.likely-closed { opacity:0.5; }
tr.likely-closed td { font-style:italic; }
.label-likely-closed { color:#6c757d; font-size:0.75em; font-style:italic; margin-left:4px; }
.jm-check-btn { background:#6c757d !important; }
.jm-check-btn:hover { background:#5a6268 !important; }
</style>
CSS;
}

// R9-P1-C: `data-search` is read natively by DataTables when building its
// column search index. Substituting data-filter (custom row-filter) with
// data-search lets us delete the custom filter plumbing entirely — plain
// column.search() does the work. Returns the canonical display string so
// the filter chip values match the cell values exactly.
function jm_relevance_label($classification) {
    switch ($classification) {
        case 'RELEVANT':           return 'Relevant';
        case 'PARTIALLY_RELEVANT': return 'Partial';
        default:                    return 'Auto';
    }
}

function jm_relevance_cell($classification) {
    $label = jm_relevance_label($classification);
    $color = ($label === 'Relevant') ? '#155724' : (($label === 'Partial') ? '#856404' : '#6c757d');
    $weight = ($label === 'Auto') ? '' : 'font-weight:600';
    return '<td data-search="' . esc_attr($label) . '"><span style="color:' . $color . ';' . $weight . '">' . esc_html($label) . '</span></td>';
}

// R9-P1-A: source_name mapping for display + filter. Keeps ATS vendor names
// (Greenhouse/Lever/Ashby) as distinct chips since which ATS a job posts
// through is useful operational info. Collapses scrapers that produce noise
// (onemodel/included_ai/siop → "Niche Board"; google_alerts/talkwalker →
// "RSS Alert"). JobSpy siblings get their real brand names.
function jm_display_source_name($source_name) {
    if (!$source_name) return 'Unknown';
    if (strpos($source_name, 'jobspy_') === 0) {
        $site = substr($source_name, 7);
        $map = [
            'linkedin' => 'LinkedIn',
            'indeed' => 'Indeed',
            'glassdoor' => 'Glassdoor',
            'zip_recruiter' => 'ZipRecruiter',
        ];
        return isset($map[$site]) ? $map[$site] : ucfirst($site);
    }
    $ats = [
        'greenhouse' => 'Greenhouse',
        'lever' => 'Lever',
        'ashby' => 'Ashby',
    ];
    if (isset($ats[$source_name])) return $ats[$source_name];
    if (in_array($source_name, ['onemodel', 'included_ai', 'siop'], true)) {
        return 'Niche Board';
    }
    if (in_array($source_name, ['google_alerts', 'talkwalker'], true)) {
        return 'RSS Alert';
    }
    $canonical = [
        'jsearch' => 'JSearch',
        'jooble' => 'Jooble',
        'adzuna' => 'Adzuna',
        'usajobs' => 'USAJobs',
    ];
    return isset($canonical[$source_name]) ? $canonical[$source_name] : $source_name;
}

// Phase 9 (R3): build a Schema.org JobPosting object for JSON-LD emission.
// Returns an associative array that json_encode()s cleanly. Callers should skip
// emission when the job lacks title or company.
function jm_build_job_posting_jsonld($post_id, $post_title) {
    $company = get_post_meta($post_id, 'company', true);
    if (!$post_title || !$company) return null;

    $doc = [
        '@context' => 'https://schema.org',
        '@type'    => 'JobPosting',
        'title'    => $post_title,
        'hiringOrganization' => [
            '@type' => 'Organization',
            'name'  => $company,
        ],
    ];
    $desc = get_post_meta($post_id, 'description_snippet', true);
    if (!$desc) {
        $desc = get_the_content(null, false, $post_id);
    }
    if ($desc) {
        $doc['description'] = wp_strip_all_tags(mb_substr($desc, 0, 500));
    }
    $date_posted = get_post_meta($post_id, 'date_posted', true);
    if (!$date_posted) $date_posted = get_post_meta($post_id, 'first_seen_date', true);
    if ($date_posted) {
        $doc['datePosted'] = $date_posted;
    }
    // Employment type — default FULL_TIME for PA roles; intern titles get INTERN
    $seniority = get_post_meta($post_id, 'seniority', true);
    $doc['employmentType'] = ($seniority === 'Intern') ? 'INTERN' : 'FULL_TIME';

    // jobLocation / jobLocationType
    $is_remote = get_post_meta($post_id, 'is_remote', true);
    $location = get_post_meta($post_id, 'location', true);
    if ($is_remote === 'remote') {
        $doc['jobLocationType'] = 'TELECOMMUTE';
    }
    if ($location && $is_remote !== 'remote') {
        $doc['jobLocation'] = [
            '@type' => 'Place',
            'address' => [
                '@type' => 'PostalAddress',
                'addressLocality' => $location,
            ],
        ];
    }

    // baseSalary
    $salary_min = get_post_meta($post_id, 'salary_min', true);
    $salary_max = get_post_meta($post_id, 'salary_max', true);
    if ($salary_min) {
        $value = [
            '@type' => 'QuantitativeValue',
            'minValue' => (int) $salary_min,
            'unitText' => 'YEAR',
        ];
        if ($salary_max) {
            $value['maxValue'] = (int) $salary_max;
        }
        $doc['baseSalary'] = [
            '@type' => 'MonetaryAmount',
            'currency' => 'USD',
            'value' => $value,
        ];
    }

    // validThrough: heuristic — 30 days past date_posted / first_seen_date
    if ($date_posted) {
        $ts = strtotime($date_posted);
        if ($ts) {
            $doc['validThrough'] = date('c', $ts + 30 * 86400);
        }
    }

    return $doc;
}

// R11 Phase 0: freshness cell. Prefers Python-computed integer
// `days_since_posted` (no timezone drift) for both sort key and display.
// Falls back to PHP date arithmetic only for legacy posts without the meta.
// NEW badge sourced from `is_brand_new` — true only when Turso created the
// row this run. The previous `first_seen_date === today` check fired on any
// WP post recreated from a lost/cleared state, falsely stamping weeks-old
// jobs as new.
function jm_freshness_cell($date_posted, $first_seen, $days_since_posted = null, $is_brand_new = false) {
    if ($days_since_posted !== null && $days_since_posted !== '') {
        $days = max(0, (int)$days_since_posted);
        $has_ref = true;
    } else {
        $ref_date = $date_posted ?: $first_seen;
        if (!$ref_date) {
            return '<td data-order="99999"><span class="freshness-stale">Unknown</span></td>';
        }
        $ts = strtotime($ref_date . ' UTC');
        if ($ts === false) {
            return '<td data-order="99999"><span class="freshness-stale">Unknown</span></td>';
        }
        $days = max(0, (int)((time() - $ts) / 86400));
        $has_ref = true;
    }
    $label = $date_posted ? '' : 'Seen ';
    if ($days <= 3) { $class = 'freshness-hot'; }
    elseif ($days <= 7) { $class = 'freshness-warm'; }
    elseif ($days <= 14) { $class = 'freshness-cool'; }
    else { $class = 'freshness-stale'; }
    if ($days === 0) { $text = $label . 'Today'; }
    elseif ($days === 1) { $text = $label . '1 day ago'; }
    else { $text = $label . $days . ' days ago'; }
    $badge = $is_brand_new ? ' <span class="badge-new">NEW</span>' : '';
    return '<td data-order="' . $days . '"><span class="' . esc_attr($class) . '">' . esc_html($text) . '</span>' . $badge . '</td>';
}

// R9-P1-B: server-rendered filter bar. `$filter_values` is a map of column
// name → list of unique pre-sorted values the shortcode collected during its
// render loop. We emit real <label><input checkbox> elements for every value
// so the DOM is fully populated at page load — no JS needs to traverse
// DataTables column data to build chips.
//
// Default state: every chip is checked, meaning "show all rows". A user
// unchecking a chip triggers the post-init JS to call column.search() with
// the remaining checked values.
function jm_filter_bar_html($id_suffix, $filter_values) {
    $out = '<div id="jm-filters-' . esc_attr($id_suffix) . '" class="jm-filters">';
    foreach ($filter_values as $col => $values) {
        $out .= '<div class="jm-filter-group">';
        $out .= '<label>' . esc_html($col) . ':</label>';
        $out .= '<div class="jm-checkboxes" data-column="' . esc_attr($col) . '">';
        foreach ($values as $val) {
            if ($val === '' || $val === null) continue;
            $safe = esc_attr((string) $val);
            $txt  = esc_html((string) $val);
            $out .= '<label class="jm-chip"><input type="checkbox" value="' . $safe . '" checked> ' . $txt . '</label>';
        }
        $out .= '</div></div>';
    }
    $out .= '<div class="jm-filter-group"><div class="jm-text-filters">';
    foreach (['Title', 'Company', 'Location'] as $col) {
        $out .= '<input type="text" class="jm-text-filter" data-column="' . esc_attr($col) . '" placeholder="' . esc_attr($col) . '...">';
    }
    $out .= '</div></div>';
    $out .= '</div>';
    return $out;
}

// R9-P1-D: event-only JS emitted AFTER DataTable() is created. No DOM
// population, no custom row-filter function. Pure event wiring on
// already-rendered chips. Reads `data-search` attributes on <td>s
// natively — DataTables indexes them at init time, so column.search()
// with a regex operates against the clean filter values we rendered
// server-side.
//
// Uses nowdoc ('JS') so PHP $ vars and \ escapes are literal inside the JS.
function jm_filter_wire_js($table_selector) {
    // Inline $table_selector as a single-quoted JS string literal.
    $sel_js = "'" . str_replace("'", "\\'", $table_selector) . "'";
    $prefix = "jQuery(function(){var tbl=jQuery($sel_js).DataTable();";
    $body = <<<'JS'
var wrapper = jQuery(tbl.table().container()).parent();
var nameToIdx = {};
tbl.columns().every(function(i){
    nameToIdx[jQuery(this.header()).text().trim()] = i;
});
// Chip (multi-select) filters: build an alternation regex across the currently
// checked values. All checked → empty search (show everything). Zero checked
// → alternation with nothing, which returns no rows (expected).
wrapper.find('.jm-checkboxes').each(function(){
    var container = jQuery(this);
    var colIdx = nameToIdx[container.data('column')];
    if (typeof colIdx !== 'number') return;
    var totalChips = container.find('input[type=checkbox]').length;
    function applyFilter(){
        var checked = [];
        container.find('input:checked').each(function(){
            checked.push(jQuery.fn.dataTable.util.escapeRegex(jQuery(this).val()));
        });
        var col = tbl.column(colIdx);
        if (checked.length === 0 || checked.length === totalChips) {
            if (col.search() !== '') col.search('').draw();
        } else {
            col.search('^(' + checked.join('|') + ')$', true, false).draw();
        }
    }
    container.on('change', 'input[type=checkbox]', function(){
        jQuery(this).closest('.jm-chip').toggleClass('jm-chip-off', !jQuery(this).is(':checked'));
        applyFilter();
    });
});
// Text filters: substring search on the target column.
wrapper.find('.jm-text-filter').each(function(){
    var input = jQuery(this);
    var colIdx = nameToIdx[input.data('column')];
    if (typeof colIdx !== 'number') return;
    input.on('keyup change', function(){
        var col = tbl.column(colIdx);
        if (col.search() !== this.value) col.search(this.value).draw();
    });
});
JS;
    return $prefix . $body . "});";
}

// ── Active Jobs Shortcode [job_table] ─────────────────────
add_shortcode('job_table', function() {
    // Transient cache: 12-hour TTL, invalidated on new post creation
    $cached = get_transient('jm_active_jobs_html');
    if ($cached !== false) {
        jm_enqueue_datatables();
        return $cached;
    }

    jm_enqueue_datatables();

    $jobs = get_posts([
        'post_type' => 'job_listing',
        'post_status' => 'publish',
        'posts_per_page' => 500, // Cap to prevent memory exhaustion; upgrade to server-side processing at scale
        'orderby' => 'date',
        'order' => 'DESC',
    ]);

    // R-audit Issue 2f: likely-closed jobs sort to the bottom by default.
    // R4-8: pre-warm the meta cache with a single SQL round-trip so the
    // subsequent get_post_meta calls (sort + render) read from object cache.
    // Previously the usort callback did 2×N get_post_meta hits which, at 500
    // posts, is 1000 DB queries just to sort.
    $job_ids = wp_list_pluck($jobs, 'ID');
    if (!empty($job_ids)) {
        update_meta_cache('post', $job_ids);
    }
    usort($jobs, function($a, $b) {
        $al = get_post_meta($a->ID, 'lifecycle_status', true);
        $bl = get_post_meta($b->ID, 'lifecycle_status', true);
        $a_closed = ($al === 'likely_closed') ? 1 : 0;
        $b_closed = ($bl === 'likely_closed') ? 1 : 0;
        if ($a_closed !== $b_closed) return $a_closed - $b_closed;
        return strcmp($b->post_date, $a->post_date);
    });

    // R9-P1-E: two-pass render. Pass 1 builds every row's HTML into a string
    // buffer AND collects distinct values for each filterable column. Pass 2
    // emits the filter bar (now populated with real values) followed by the
    // buffered rows. This puts a fully-populated DOM on the page at load and
    // eliminates the JS-side chip-population step that broke 3× in R4-R6.
    $filter_values = [
        'Category' => [],
        'Level'    => [],
        'Remote'   => [],
        'Relevance'=> [],
        'Source'   => [],
    ];
    $rows_html = '';
    $jsonld_docs = [];

    foreach ($jobs as $j) {
        $source_url = esc_url(get_post_meta($j->ID, 'source_url', true));
        $apply_url = esc_url(get_post_meta($j->ID, 'apply_url', true));
        if (!$apply_url) $apply_url = $source_url;
        $title = esc_html($j->post_title);
        $doc = jm_build_job_posting_jsonld($j->ID, $j->post_title);
        if ($doc !== null) $jsonld_docs[] = $doc;
        $t = $source_url ? '<a href="' . $source_url . '" target="_blank" rel="noopener">' . $title . '</a>' : $title;

        $location = esc_html(get_post_meta($j->ID, 'location', true));
        $loc_conf = get_post_meta($j->ID, 'location_confidence', true);
        $remote = esc_html(get_post_meta($j->ID, 'is_remote', true));
        $remote_conf = get_post_meta($j->ID, 'remote_confidence', true);
        $salary = esc_html(get_post_meta($j->ID, 'salary_range', true));
        $salary_conf = get_post_meta($j->ID, 'salary_confidence', true);
        $salary_min = (int) get_post_meta($j->ID, 'salary_min', true);
        $seniority = esc_html(get_post_meta($j->ID, 'seniority', true));
        $lifecycle = get_post_meta($j->ID, 'lifecycle_status', true);
        $tr_class = ($lifecycle === 'likely_closed') ? ' class="likely-closed"' : '';
        $closed_label = ($lifecycle === 'likely_closed')
            ? ' <span class="label-likely-closed">(likely closed)</span>' : '';

        // Collect filter values (logical tokens only — no HTML, no badges)
        $category_v  = get_post_meta($j->ID, 'category', true) ?: 'General PA';
        $level_v     = $seniority ?: 'Unknown';
        $remote_v    = get_post_meta($j->ID, 'is_remote', true) ?: 'unknown';
        $relevance_v = jm_relevance_label(get_post_meta($j->ID, 'llm_classification', true));
        $source_v    = jm_display_source_name(get_post_meta($j->ID, 'source_name', true));
        $filter_values['Category'][]  = $category_v;
        $filter_values['Level'][]     = $level_v;
        $filter_values['Remote'][]    = $remote_v;
        $filter_values['Relevance'][] = $relevance_v;
        $filter_values['Source'][]    = $source_v;

        // R9-P1-C: data-search (native DataTables) on cells whose display HTML
        // differs from the filter value. Category/Level/Apply/Posted have no
        // such wrappers so they don't need data-search.
        $r = '';
        $r .= '<tr' . $tr_class . '>';
        $r .= '<td>' . $t . $closed_label . '</td>';
        $r .= '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        $r .= '<td>' . $location . jm_confidence_badge($loc_conf) . '</td>';
        $r .= '<td>' . esc_html($category_v) . '</td>';
        $r .= '<td>' . esc_html($level_v) . '</td>';
        $remote_tip = jm_consensus_tooltip(
            get_post_meta($j->ID, 'remote_vote_agreement', true),
            get_post_meta($j->ID, 'remote_vote_sources', true),
            get_post_meta($j->ID, 'remote_vote_confidence', true)
        );
        $r .= '<td data-search="' . esc_attr($remote_v) . '"' . $remote_tip . '>' . $remote . jm_confidence_badge($remote_conf) . '</td>';
        $r .= '<td data-order="' . $salary_min . '">' . $salary . jm_confidence_badge($salary_conf) . '</td>';
        $r .= jm_relevance_cell(get_post_meta($j->ID, 'llm_classification', true));
        $r .= '<td data-search="' . esc_attr($source_v) . '">' . esc_html($source_v) . '</td>';
        if ($apply_url) {
            if ($lifecycle === 'likely_closed') {
                $host = parse_url($apply_url, PHP_URL_HOST);
                $careers_root = $host ? 'https://' . $host : $apply_url;
                $apply_cell = '<a class="jm-apply-btn jm-check-btn" href="' . esc_url($careers_root)
                    . '" target="_blank" rel="noopener" title="This posting may be closed">Check &rarr;</a>';
            } else {
                $apply_cell = '<a class="jm-apply-btn" href="' . $apply_url
                    . '" target="_blank" rel="noopener">Apply &rarr;</a>';
            }
        } else {
            $apply_cell = '';
        }
        $r .= '<td>' . $apply_cell . '</td>';
        $r .= jm_freshness_cell(
            get_post_meta($j->ID, 'date_posted', true),
            get_post_meta($j->ID, 'first_seen_date', true),
            get_post_meta($j->ID, 'days_since_posted', true),
            get_post_meta($j->ID, 'is_brand_new', true) === '1'
        );
        $r .= '</tr>';
        $rows_html .= $r;
    }

    // Dedup + sort each filter column
    foreach ($filter_values as $k => $v) {
        $v = array_unique($v);
        sort($v);
        $filter_values[$k] = array_values($v);
    }

    ob_start();
    echo jm_inline_styles();
    echo '<div class="jm-wrapper">';
    echo jm_filter_bar_html('active', $filter_values);
    echo '<table id="jm-table" class="display nowrap" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Category</th><th>Level</th><th>Remote</th><th>Salary</th><th>Relevance</th><th>Source</th><th>Apply</th><th>Posted</th></tr></thead><tbody>';
    echo $rows_html;
    echo '</tbody></table>';
    // Phase 9 (R3): emit one JSON-LD block per job for SEO / Google for Jobs indexing.
    // JSON_HEX_* flags prevent `</script>` / `&` / quotes in job titles from breaking
    // out of the <script> block.
    $jsonld_flags = JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT;
    foreach ($jsonld_docs as $doc) {
        echo '<script type="application/ld+json">' . wp_json_encode($doc, $jsonld_flags) . '</script>';
    }
    // R9-P1-D: DataTable init + separate event-wire script.
    // dom:'lrtip' hides the default DataTables search box — we render our own.
    echo "<script>jQuery(function(){jQuery('#jm-table').DataTable({responsive:true,order:[[10,'asc']],pageLength:50,dom:'lrtip'});});</script>";
    echo '<script>' . jm_filter_wire_js('#jm-table') . '</script>';
    echo '</div>';  // .jm-wrapper
    $html = ob_get_clean();

    set_transient('jm_active_jobs_html', $html, 12 * HOUR_IN_SECONDS);
    return $html;
});

// ── Archive Shortcode [job_archive_table] ─────────────────
add_shortcode('job_archive_table', function() {
    $cached = get_transient('jm_archived_jobs_html');
    if ($cached !== false) {
        jm_enqueue_datatables();
        return $cached;
    }
    jm_enqueue_datatables();
    $jobs = get_posts([
        'post_type' => 'job_listing',
        'post_status' => 'archived',
        'posts_per_page' => 500,
        'orderby' => 'date',
        'order' => 'DESC',
    ]);
    // R4-8: prime the post-meta cache before the render loop so the 12-ish
    // get_post_meta calls per row hit object cache instead of the DB.
    $job_ids = wp_list_pluck($jobs, 'ID');
    if (!empty($job_ids)) {
        update_meta_cache('post', $job_ids);
    }
    // R9-P1-E: two-pass render (mirror of the active shortcode).
    $filter_values = [
        'Category' => [], 'Level' => [], 'Remote' => [],
        'Relevance'=> [], 'Source' => [],
    ];
    $rows_html = '';
    foreach ($jobs as $j) {
        $source_url = esc_url(get_post_meta($j->ID, 'source_url', true));
        $apply_url = esc_url(get_post_meta($j->ID, 'apply_url', true));
        if (!$apply_url) $apply_url = $source_url;
        $title = esc_html($j->post_title);
        $t = $source_url ? '<a href="' . $source_url . '" target="_blank" rel="noopener">' . $title . '</a>' : $title;

        $location = esc_html(get_post_meta($j->ID, 'location', true));
        $loc_conf = get_post_meta($j->ID, 'location_confidence', true);
        $remote = esc_html(get_post_meta($j->ID, 'is_remote', true));
        $remote_conf = get_post_meta($j->ID, 'remote_confidence', true);
        $salary = esc_html(get_post_meta($j->ID, 'salary_range', true));
        $salary_conf = get_post_meta($j->ID, 'salary_confidence', true);
        $salary_min = (int) get_post_meta($j->ID, 'salary_min', true);
        $seniority = esc_html(get_post_meta($j->ID, 'seniority', true));

        $category_v  = get_post_meta($j->ID, 'category', true) ?: 'General PA';
        $level_v     = $seniority ?: 'Unknown';
        $remote_v    = get_post_meta($j->ID, 'is_remote', true) ?: 'unknown';
        $relevance_v = jm_relevance_label(get_post_meta($j->ID, 'llm_classification', true));
        $source_v    = jm_display_source_name(get_post_meta($j->ID, 'source_name', true));
        $filter_values['Category'][]  = $category_v;
        $filter_values['Level'][]     = $level_v;
        $filter_values['Remote'][]    = $remote_v;
        $filter_values['Relevance'][] = $relevance_v;
        $filter_values['Source'][]    = $source_v;

        $apply_cell = $apply_url ? '<a class="jm-apply-btn" href="' . $apply_url . '" target="_blank" rel="noopener">Apply &rarr;</a>' : '';

        $r = '';
        $r .= '<tr>';
        $r .= '<td>' . $t . '</td>';
        $r .= '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        $r .= '<td>' . $location . jm_confidence_badge($loc_conf) . '</td>';
        $r .= '<td>' . esc_html($category_v) . '</td>';
        $r .= '<td>' . esc_html($level_v) . '</td>';
        $remote_tip = jm_consensus_tooltip(
            get_post_meta($j->ID, 'remote_vote_agreement', true),
            get_post_meta($j->ID, 'remote_vote_sources', true),
            get_post_meta($j->ID, 'remote_vote_confidence', true)
        );
        $r .= '<td data-search="' . esc_attr($remote_v) . '"' . $remote_tip . '>' . $remote . jm_confidence_badge($remote_conf) . '</td>';
        $r .= '<td data-order="' . $salary_min . '">' . $salary . jm_confidence_badge($salary_conf) . '</td>';
        $r .= jm_relevance_cell(get_post_meta($j->ID, 'llm_classification', true));
        $r .= '<td data-search="' . esc_attr($source_v) . '">' . esc_html($source_v) . '</td>';
        $r .= '<td>' . $apply_cell . '</td>';
        $r .= jm_freshness_cell(
            get_post_meta($j->ID, 'date_posted', true),
            get_post_meta($j->ID, 'first_seen_date', true),
            get_post_meta($j->ID, 'days_since_posted', true),
            get_post_meta($j->ID, 'is_brand_new', true) === '1'
        );
        $r .= '<td>' . esc_html(get_post_meta($j->ID, 'days_active', true)) . '</td>';
        $r .= '<td>' . esc_html(get_post_meta($j->ID, 'archived_date', true)) . '</td>';
        $r .= '</tr>';
        $rows_html .= $r;
    }
    foreach ($filter_values as $k => $v) {
        $v = array_unique($v); sort($v);
        $filter_values[$k] = array_values($v);
    }

    ob_start();
    echo jm_inline_styles();
    echo '<div class="jm-wrapper">';
    echo jm_filter_bar_html('archive', $filter_values);
    echo '<table id="jm-archive" class="display nowrap" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Category</th><th>Level</th><th>Remote</th><th>Salary</th><th>Relevance</th><th>Source</th><th>Apply</th><th>Posted</th><th>Days Active</th><th>Archived</th></tr></thead><tbody>';
    echo $rows_html;
    echo '</tbody></table>';
    echo "<script>jQuery(function(){jQuery('#jm-archive').DataTable({responsive:true,order:[[12,'desc']],pageLength:50,dom:'lrtip'});});</script>";
    echo '<script>' . jm_filter_wire_js('#jm-archive') . '</script>';
    echo '</div>';
    $html = ob_get_clean();
    set_transient('jm_archived_jobs_html', $html, 12 * HOUR_IN_SECONDS);
    return $html;
});

// ── Phase 8 (R3): [job_dashboard] — ApexCharts-rendered stats ─────
// ApexCharts (MIT) is self-hosted at wordpress/assets/js/apexcharts.min.js.
// Do NOT swap to a CDN — we were burned by the DataTables CDN hijack (Jul 2025).
function jm_enqueue_apexcharts() {
    $plugin_url = plugin_dir_url(__FILE__);
    wp_enqueue_script(
        'jm-apexcharts',
        $plugin_url . 'assets/js/apexcharts.min.js',
        [],
        '3.53.0',
        true
    );
}

add_shortcode('job_dashboard', function() {
    $stats = get_transient('jm_dashboard_stats');
    if (!is_array($stats)) {
        return '<p>Dashboard data not available yet. The next pipeline run will populate it.</p>';
    }
    jm_enqueue_apexcharts();
    // C6 fix: harden against `</script>` / `&` / quote escapes in stat keys
    // that would otherwise close the inline <script> early.
    $payload = wp_json_encode(
        $stats,
        JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT
    );
    ob_start();
    ?>
    <div class="jm-dashboard">
        <p style="margin-bottom:16px;color:#6c757d;font-size:0.9em">
            Snapshot date: <strong><?php echo esc_html($stats['snapshot_date'] ?? 'n/a'); ?></strong>
        </p>
        <div class="jm-dash-grid">
            <div class="jm-dash-card"><h3>Jobs by Category</h3><div id="jm-chart-category"></div></div>
            <div class="jm-dash-card"><h3>Seniority Distribution</h3><div id="jm-chart-seniority"></div></div>
            <div class="jm-dash-card"><h3>Remote / Hybrid / On-site</h3><div id="jm-chart-remote"></div></div>
            <div class="jm-dash-card"><h3>Top Hiring Companies</h3><div id="jm-chart-companies"></div></div>
            <div class="jm-dash-card jm-dash-wide"><h3>Posting Volume Over Time</h3><div id="jm-chart-volume"></div></div>
            <div class="jm-dash-card jm-dash-wide"><h3>Most Mentioned Tools</h3><div id="jm-chart-tools"></div></div>
        </div>
    </div>
    <style>
    .jm-dashboard { max-width:1200px; margin:0 auto; }
    .jm-dash-grid { display:grid; grid-template-columns:1fr 1fr; gap:20px; }
    .jm-dash-wide { grid-column:1 / span 2; }
    .jm-dash-card { background:#fff; border:1px solid #dee2e6; border-radius:8px; padding:16px; }
    .jm-dash-card h3 { margin:0 0 12px; font-size:1em; color:#343a40; font-weight:600; }
    @media (max-width:780px) { .jm-dash-grid { grid-template-columns:1fr; } .jm-dash-wide { grid-column:auto; } }
    </style>
    <script>
    (function() {
        var stats = <?php echo $payload; ?>;
        function kvToXY(obj) {
            var keys = Object.keys(obj || {}).sort(function(a, b) { return obj[b] - obj[a]; });
            return { labels: keys, values: keys.map(function(k) { return obj[k]; }) };
        }
        function listToXY(list, limit) {
            var items = (list || []).slice(0, limit || 10);
            return { labels: items.map(function(i) { return i.name; }),
                     values: items.map(function(i) { return i.count; }) };
        }
        function renderBar(el, title, data) {
            new ApexCharts(document.querySelector(el), {
                chart: { type: 'bar', height: 320, toolbar: { show: false } },
                series: [{ name: title, data: data.values }],
                xaxis: { categories: data.labels },
                plotOptions: { bar: { horizontal: true, borderRadius: 3 } },
                dataLabels: { enabled: true },
                colors: ['#2271b1'],
            }).render();
        }
        function renderDonut(el, data) {
            new ApexCharts(document.querySelector(el), {
                chart: { type: 'donut', height: 320 },
                series: data.values,
                labels: data.labels,
                legend: { position: 'bottom' },
            }).render();
        }
        var cat = kvToXY(stats.category_count);
        renderBar('#jm-chart-category', 'Jobs', cat);
        var sen = kvToXY(stats.seniority_count);
        renderDonut('#jm-chart-seniority', sen);
        var rem = kvToXY(stats.remote_count);
        renderDonut('#jm-chart-remote', rem);
        renderBar('#jm-chart-companies', 'Jobs', listToXY(stats.company_count, 10));
        renderBar('#jm-chart-tools', 'Mentions', listToXY(stats.vendor_count, 15));
        // Volume trend (line)
        var trend = stats.total_active_trend || [];
        new ApexCharts(document.querySelector('#jm-chart-volume'), {
            chart: { type: 'line', height: 300, toolbar: { show: false } },
            series: [{ name: 'Active jobs', data: trend.map(function(p) { return p.count; }) }],
            xaxis: { categories: trend.map(function(p) { return p.date; }) },
            stroke: { curve: 'smooth', width: 3 },
            colors: ['#2271b1'],
            dataLabels: { enabled: false },
        }).render();
    })();
    </script>
    <?php
    return ob_get_clean();
});

// ── Self-hosted DataTables enqueue ────────────────────────
// DO NOT use cdn.datatables.net — it was hijacked via domain theft July 29, 2025
function jm_enqueue_datatables() {
    $plugin_url = plugin_dir_url(__FILE__);
    wp_enqueue_script('jm-datatables', $plugin_url . 'assets/js/dataTables.min.js', ['jquery'], '2.1.8', true);
    wp_enqueue_style('jm-datatables-css', $plugin_url . 'assets/css/dataTables.dataTables.min.css', [], '2.1.8');
    wp_enqueue_script('jm-dt-responsive', $plugin_url . 'assets/js/responsive.dataTables.min.js', ['jm-datatables'], '3.0.3', true);
    wp_enqueue_style('jm-dt-responsive-css', $plugin_url . 'assets/css/responsive.dataTables.min.css', [], '3.0.3');
}
