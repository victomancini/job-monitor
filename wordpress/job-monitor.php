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
        // Phase F: enrichment + confidence fields
        'location_confidence','salary_confidence','remote_confidence',
        'enrichment_source','enrichment_date',
        // Phase B/F (R2): date_posted for freshness; seniority_confidence for badge
        'date_posted','seniority_confidence',
        // Phase 5 (R3): comma-separated vendor/tool mentions from description
        'vendors_mentioned',
        // Phase 6 (R3): lifecycle state ('active' | 'likely_closed')
        'lifecycle_status'];
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

// ── REST Endpoints ────────────────────────────────────────
add_action('rest_api_init', function() {
    register_rest_route('jobmonitor/v1', '/update-jobs', [
        'methods' => 'POST',
        'callback' => 'jm_batch_update',
        'permission_callback' => function() { return current_user_can('edit_posts'); },
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
        'permission_callback' => function() { return current_user_can('edit_posts'); },
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
            // Phase F3: enrichment + confidence fields
            'location_confidence','salary_confidence','remote_confidence',
            'enrichment_source','enrichment_date',
            // Phase B/F (R2): date_posted for freshness; seniority_confidence for badge
            'date_posted','seniority_confidence',
            // Phase 5 (R3): comma-separated vendor/tool mentions from description
            'vendors_mentioned',
            // Phase 6 (R3): lifecycle state ('active' | 'likely_closed')
            'lifecycle_status'];
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
            wp_update_post(['ID' => $post_id, 'post_status' => 'publish', 'meta_input' => $meta]);
            $results['updated']++;
            $results['post_ids'][$job['external_id']] = $post_id;
        } else {
            $meta['first_seen_date'] = current_time('Y-m-d', true);
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
/* Phase C (R2): multi-select filter bar */
.jm-filters { display:flex; flex-wrap:wrap; gap:12px; margin-bottom:16px; padding:12px; background:#f8f9fa; border-radius:6px; }
.jm-filter-group { display:flex; align-items:flex-start; gap:6px; }
.jm-filter-group > label:first-child { font-weight:600; white-space:nowrap; padding-top:4px; }
.jm-checkboxes { display:flex; flex-wrap:wrap; gap:4px; max-width:560px; }
.jm-chip { display:inline-flex; align-items:center; gap:3px; padding:3px 8px; background:#fff; border:1px solid #dee2e6; border-radius:4px; font-size:0.82em; cursor:pointer; }
.jm-chip input { margin:0; }
.jm-chip.jm-chip-off { opacity:0.5; background:#e9ecef; }
.jm-text-filters { display:flex; flex-wrap:wrap; gap:8px; align-items:center; }
.jm-text-filters input { padding:4px 8px; border:1px solid #dee2e6; border-radius:4px; font-size:0.85em; }
/* Phase 6 (R3): muted styling for likely-closed jobs */
tr.likely-closed { opacity:0.55; }
tr.likely-closed td { font-style:italic; }
.label-likely-closed { color:#6c757d; font-size:0.75em; font-style:italic; margin-left:4px; }
</style>
CSS;
}

// Phase D (R2): render the Relevance cell from llm_classification.
// RELEVANT → Relevant (green), PARTIALLY_RELEVANT → Partial (amber),
// anything else (including empty) → Auto (gray, keyword-only).
function jm_relevance_cell($classification) {
    switch ($classification) {
        case 'RELEVANT':
            return '<td><span style="color:#155724;font-weight:600">Relevant</span></td>';
        case 'PARTIALLY_RELEVANT':
            return '<td><span style="color:#856404;font-weight:600">Partial</span></td>';
        default:
            return '<td><span style="color:#6c757d">Auto</span></td>';
    }
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

// Phase B (R2): render the Posted column with freshness color coding.
// Accepts date_posted (preferred) and first_seen_date (fallback, labeled "Seen").
// Returns a <td>...</td> cell with data-order set to the raw days count so
// DataTables sorts numerically.
function jm_freshness_cell($date_posted, $first_seen) {
    $ref_date = $date_posted ?: $first_seen;
    $label = $date_posted ? '' : 'Seen ';
    if (!$ref_date) {
        return '<td data-order="99999"><span class="freshness-stale">Unknown</span></td>';
    }
    $ts = strtotime($ref_date);
    if ($ts === false) {
        return '<td data-order="99999"><span class="freshness-stale">Unknown</span></td>';
    }
    $days = max(0, (int)((time() - $ts) / 86400));
    if ($days <= 3) { $class = 'freshness-hot'; }
    elseif ($days <= 7) { $class = 'freshness-warm'; }
    elseif ($days <= 14) { $class = 'freshness-cool'; }
    else { $class = 'freshness-stale'; }
    if ($days === 0) { $text = $label . 'Today'; }
    elseif ($days === 1) { $text = $label . '1 day ago'; }
    else { $text = $label . $days . ' days ago'; }
    $is_new = ($first_seen && $first_seen === current_time('Y-m-d'));
    $badge = $is_new ? ' <span class="badge-new">NEW</span>' : '';
    return '<td data-order="' . $days . '"><span class="' . esc_attr($class) . '">' . esc_html($text) . '</span>' . $badge . '</td>';
}

// Phase C (R2): multi-select filter bar HTML. Renders above the table. Init-complete
// JS populates checkboxes from unique column values. `$id_suffix` disambiguates the two
// shortcodes; `$categorical_cols` lists the header names that get checkbox filters.
function jm_filter_bar_html($id_suffix, $categorical_cols = ['Level','Remote','Source']) {
    $out = '<div id="jm-filters-' . esc_attr($id_suffix) . '" class="jm-filters">';
    foreach ($categorical_cols as $col) {
        $out .= '<div class="jm-filter-group">';
        $out .= '<label>' . esc_html($col) . ':</label>';
        $out .= '<div class="jm-checkboxes" data-column="' . esc_attr($col) . '"></div>';
        $out .= '</div>';
    }
    $out .= '<div class="jm-filter-group"><div class="jm-text-filters">';
    foreach (['Title', 'Company', 'Location'] as $col) {
        $out .= '<input type="text" class="jm-text-filter" data-column="' . esc_attr($col) . '" placeholder="' . esc_attr($col) . '...">';
    }
    $out .= '</div></div>';
    $out .= '</div>';
    return $out;
}

// Phase C (R2): DataTables initComplete. Populates checkbox filter pills from each
// categorical column's unique values and wires text filters to freeform columns.
// Column names are matched by <th> text so the PHP doesn't care about indices.
// Uses nowdoc ('JS') so $ and \ are literal in the JS body.
function jm_datatables_init_complete_js() {
    return <<<'JS'
initComplete: function() {
    var api = this.api();
    var colIdxByName = {};
    api.columns().every(function(i) {
        colIdxByName[jQuery(this.header()).text().trim()] = i;
    });
    var $wrapper = jQuery(this.table().container()).parent();
    $wrapper.find('.jm-checkboxes').each(function() {
        var $container = jQuery(this);
        var colName = $container.data('column');
        var colIdx = colIdxByName[colName];
        if (typeof colIdx !== 'number') return;
        var column = api.column(colIdx);
        var unique = {};
        column.data().each(function(d) {
            var text = jQuery('<div>').html(d).text().trim();
            if (text) unique[text] = true;
        });
        Object.keys(unique).sort().forEach(function(val) {
            var safeVal = val.replace(/"/g, '&quot;');
            $container.append(
                '<label class="jm-chip"><input type="checkbox" value="' + safeVal + '" checked> ' + val + '</label>'
            );
        });
        $container.on('change', 'input[type=checkbox]', function() {
            var $cb = jQuery(this);
            $cb.closest('.jm-chip').toggleClass('jm-chip-off', !$cb.is(':checked'));
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
    $wrapper.find('.jm-text-filter').each(function() {
        var $input = jQuery(this);
        var colName = $input.data('column');
        var colIdx = colIdxByName[colName];
        if (typeof colIdx !== 'number') return;
        var column = api.column(colIdx);
        $input.on('keyup change', function() {
            if (column.search() !== this.value) {
                column.search(this.value).draw();
            }
        });
    });
}
JS;
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

    ob_start();
    echo jm_inline_styles();
    echo '<div class="jm-wrapper">';
    echo jm_filter_bar_html('active', ['Category', 'Level', 'Remote', 'Relevance', 'Source']);
    echo '<table id="jm-table" class="display nowrap" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Category</th><th>Level</th><th>Remote</th><th>Salary</th><th>Relevance</th><th>Source</th><th>Apply</th><th>Posted</th></tr></thead><tbody>';
    $jsonld_docs = [];  // Phase 9 (R3): accumulate JSON-LD per job
    foreach ($jobs as $j) {
        $source_url = esc_url(get_post_meta($j->ID, 'source_url', true));
        $apply_url = esc_url(get_post_meta($j->ID, 'apply_url', true));
        if (!$apply_url) $apply_url = $source_url;
        $title = esc_html($j->post_title);
        // Phase 9 (R3): build JSON-LD for this posting (skip if it lacks title/company)
        $doc = jm_build_job_posting_jsonld($j->ID, $j->post_title);
        if ($doc !== null) $jsonld_docs[] = $doc;
        $t = $source_url ? '<a href="' . $source_url . '" target="_blank" rel="noopener">' . $title . '</a>' : $title;

        $location = esc_html(get_post_meta($j->ID, 'location', true));
        $loc_conf = get_post_meta($j->ID, 'location_confidence', true);
        $remote = esc_html(get_post_meta($j->ID, 'is_remote', true));
        $remote_conf = get_post_meta($j->ID, 'remote_confidence', true);
        $salary = esc_html(get_post_meta($j->ID, 'salary_range', true));
        $salary_conf = get_post_meta($j->ID, 'salary_confidence', true);
        $salary_min = (int) get_post_meta($j->ID, 'salary_min', true);  // Phase H (R2): numeric sort
        $seniority = esc_html(get_post_meta($j->ID, 'seniority', true));
        // Phase 6 (R3): muted styling + "may be closed" label for stale jobs
        $lifecycle = get_post_meta($j->ID, 'lifecycle_status', true);
        $tr_class = ($lifecycle === 'likely_closed') ? ' class="likely-closed"' : '';
        $closed_label = ($lifecycle === 'likely_closed')
            ? ' <span class="label-likely-closed">(may be closed)</span>' : '';

        echo '<tr' . $tr_class . '>';
        echo '<td>' . $t . $closed_label . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        echo '<td>' . $location . jm_confidence_badge($loc_conf) . '</td>';
        // Phase I (R2): Category column
        echo '<td>' . esc_html(get_post_meta($j->ID, 'category', true) ?: 'General PA') . '</td>';
        echo '<td>' . ($seniority ?: 'Unknown') . '</td>';
        echo '<td>' . $remote . jm_confidence_badge($remote_conf) . '</td>';
        // Phase H (R2): data-order so DataTables sorts by salary_min numerically
        echo '<td data-order="' . $salary_min . '">' . $salary . jm_confidence_badge($salary_conf) . '</td>';
        // Phase D (R2): Relevance column (llm_classification)
        echo jm_relevance_cell(get_post_meta($j->ID, 'llm_classification', true));
        echo '<td>' . esc_html(get_post_meta($j->ID, 'source_name', true)) . '</td>';
        $apply_cell = $apply_url ? '<a class="jm-apply-btn" href="' . $apply_url . '" target="_blank" rel="noopener">Apply &rarr;</a>' : '';
        echo '<td>' . $apply_cell . '</td>';
        // Phase B (R2): Posted column with freshness + NEW badge
        echo jm_freshness_cell(
            get_post_meta($j->ID, 'date_posted', true),
            get_post_meta($j->ID, 'first_seen_date', true)
        );
        echo '</tr>';
    }
    echo '</tbody>';
    echo '</table>';
    // Phase 9 (R3): emit one JSON-LD block per job for SEO / Google for Jobs indexing.
    // JSON_HEX_* flags prevent `</script>` / `&` / quotes in job titles from breaking
    // out of the <script> block (C5 fix).
    $jsonld_flags = JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT;
    foreach ($jsonld_docs as $doc) {
        echo '<script type="application/ld+json">' . wp_json_encode($doc, $jsonld_flags) . '</script>';
    }
    $init = jm_datatables_init_complete_js();
    echo "<script>jQuery(function(\$){\$('#jm-table').DataTable({responsive:true,order:[[10,'asc']],pageLength:50,{$init}});});</script>";
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
    ob_start();
    echo jm_inline_styles();
    echo '<div class="jm-wrapper">';
    echo jm_filter_bar_html('archive', ['Category', 'Level', 'Remote', 'Relevance', 'Source']);
    echo '<table id="jm-archive" class="display nowrap" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Category</th><th>Level</th><th>Remote</th><th>Salary</th><th>Relevance</th><th>Source</th><th>Apply</th><th>Posted</th><th>Days Active</th><th>Archived</th></tr></thead><tbody>';
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
        $salary_min = (int) get_post_meta($j->ID, 'salary_min', true);  // Phase H (R2): numeric sort
        $seniority = esc_html(get_post_meta($j->ID, 'seniority', true));

        echo '<tr>';
        echo '<td>' . $t . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        echo '<td>' . $location . jm_confidence_badge($loc_conf) . '</td>';
        // Phase I (R2): Category column
        echo '<td>' . esc_html(get_post_meta($j->ID, 'category', true) ?: 'General PA') . '</td>';
        echo '<td>' . ($seniority ?: 'Unknown') . '</td>';
        echo '<td>' . $remote . jm_confidence_badge($remote_conf) . '</td>';
        // Phase H (R2): data-order so DataTables sorts by salary_min numerically
        echo '<td data-order="' . $salary_min . '">' . $salary . jm_confidence_badge($salary_conf) . '</td>';
        // Phase D (R2): Relevance column (llm_classification)
        echo jm_relevance_cell(get_post_meta($j->ID, 'llm_classification', true));
        echo '<td>' . esc_html(get_post_meta($j->ID, 'source_name', true)) . '</td>';
        $apply_cell = $apply_url ? '<a class="jm-apply-btn" href="' . $apply_url . '" target="_blank" rel="noopener">Apply &rarr;</a>' : '';
        echo '<td>' . $apply_cell . '</td>';
        // Phase B (R2): Posted column with freshness + NEW badge
        echo jm_freshness_cell(
            get_post_meta($j->ID, 'date_posted', true),
            get_post_meta($j->ID, 'first_seen_date', true)
        );
        echo '<td>' . esc_html(get_post_meta($j->ID, 'days_active', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'archived_date', true)) . '</td>';
        echo '</tr>';
    }
    echo '</tbody>';
    echo '</table>';
    $init = jm_datatables_init_complete_js();
    echo "<script>jQuery(function(\$){\$('#jm-archive').DataTable({responsive:true,order:[[12,'desc']],pageLength:50,{$init}});});</script>";
    echo '</div>';  // .jm-wrapper
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
