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
        'enrichment_source','enrichment_date'];
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
            'enrichment_source','enrichment_date'];
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
        $meta['last_seen_active'] = current_time('Y-m-d');
        $meta['job_status'] = 'active';

        if (!empty($existing)) {
            $post_id = $existing[0]->ID;
            wp_update_post(['ID' => $post_id, 'post_status' => 'publish', 'meta_input' => $meta]);
            $results['updated']++;
            $results['post_ids'][$job['external_id']] = $post_id;
        } else {
            $meta['first_seen_date'] = current_time('Y-m-d');
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

// ── Daily Archival Cron ───────────────────────────────────
add_action('wp', function() {
    if (!wp_next_scheduled('jm_daily_archive')) {
        wp_schedule_event(time(), 'daily', 'jm_daily_archive');
    }
});
add_action('jm_daily_archive', function() {
    global $wpdb;
    $cutoff = date('Y-m-d', strtotime('-7 days'));
    // Use direct SQL with LIMIT to avoid memory issues on large tables
    $stale_ids = $wpdb->get_col($wpdb->prepare(
        "SELECT p.ID FROM {$wpdb->posts} p
         INNER JOIN {$wpdb->postmeta} pm ON p.ID = pm.post_id AND pm.meta_key = 'last_seen_active'
         WHERE p.post_type = 'job_listing' AND p.post_status = 'publish' AND pm.meta_value < %s
         LIMIT 100",
        $cutoff
    ));
    foreach ($stale_ids as $pid) {
        $first = get_post_meta($pid, 'first_seen_date', true);
        $last = get_post_meta($pid, 'last_seen_active', true);
        $days = ($first && $last) ? max(1, round((strtotime($last) - strtotime($first)) / 86400)) : 0;
        wp_update_post(['ID' => $pid, 'post_status' => 'archived']);
        update_post_meta($pid, 'job_status', 'archived');
        update_post_meta($pid, 'archived_date', current_time('Y-m-d'));
        update_post_meta($pid, 'days_active', (string)$days);
    }
    // Invalidate both caches after archival
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
.jm-apply-btn { display:inline-block; padding:4px 10px; background:#2271b1; color:#fff !important; border-radius:3px; text-decoration:none; font-size:0.85em; }
.jm-apply-btn:hover { background:#135e96; color:#fff; }
tfoot input, tfoot select { width:100%; box-sizing:border-box; font-size:0.85em; padding:2px 4px; }
</style>
CSS;
}

// Phase F2: DataTables initComplete boilerplate. Adds dropdown filters to
// categorical columns (Level, Remote, Source) and text filters to freeform
// columns (Title, Company, Location). Column names must match <th> text.
function jm_datatables_init_complete_js() {
    return <<<JS
initComplete: function() {
    var api = this.api();
    api.columns().every(function() {
        var column = this;
        var header = jQuery(column.header()).text().trim();
        var footerCell = jQuery(column.footer()).empty();
        if (['Level','Remote','Source'].indexOf(header) >= 0) {
            var select = jQuery('<select><option value="">All</option></select>')
                .appendTo(footerCell)
                .on('change', function() {
                    var val = jQuery.fn.dataTable.util.escapeRegex(jQuery(this).val());
                    column.search(val ? '^' + val + '$' : '', true, false).draw();
                });
            column.data().unique().sort().each(function(d) {
                var text = jQuery('<div>').html(d).text().trim();
                if (text) select.append('<option value="' + text + '">' + text + '</option>');
            });
        } else if (['Title','Company','Location'].indexOf(header) >= 0) {
            jQuery('<input type="text" placeholder="Filter...">')
                .appendTo(footerCell)
                .on('keyup change clear', function() {
                    if (column.search() !== this.value) {
                        column.search(this.value).draw();
                    }
                });
        }
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
    echo '<table id="jm-table" class="display nowrap" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Level</th><th>Remote</th><th>Salary</th><th>Source</th><th>Apply</th><th>First Seen</th></tr></thead><tbody>';
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
        $seniority = esc_html(get_post_meta($j->ID, 'seniority', true));

        echo '<tr>';
        echo '<td>' . $t . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        echo '<td>' . $location . jm_confidence_badge($loc_conf) . '</td>';
        echo '<td>' . ($seniority ?: 'Unknown') . '</td>';
        echo '<td>' . $remote . jm_confidence_badge($remote_conf) . '</td>';
        echo '<td>' . $salary . jm_confidence_badge($salary_conf) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'source_name', true)) . '</td>';
        $apply_cell = $apply_url ? '<a class="jm-apply-btn" href="' . $apply_url . '" target="_blank" rel="noopener">Apply &rarr;</a>' : '';
        echo '<td>' . $apply_cell . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'first_seen_date', true)) . '</td>';
        echo '</tr>';
    }
    echo '</tbody>';
    // <tfoot> holds filter controls populated by DataTables initComplete
    echo '<tfoot><tr>' . str_repeat('<th></th>', 9) . '</tr></tfoot>';
    echo '</table>';
    $init = jm_datatables_init_complete_js();
    echo "<script>jQuery(function(\$){\$('#jm-table').DataTable({responsive:true,order:[[8,'desc']],pageLength:50,{$init}});});</script>";
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
    echo '<table id="jm-archive" class="display nowrap" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Level</th><th>Remote</th><th>Salary</th><th>Source</th><th>Apply</th><th>Days Active</th><th>Archived</th></tr></thead><tbody>';
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
        $seniority = esc_html(get_post_meta($j->ID, 'seniority', true));

        echo '<tr>';
        echo '<td>' . $t . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        echo '<td>' . $location . jm_confidence_badge($loc_conf) . '</td>';
        echo '<td>' . ($seniority ?: 'Unknown') . '</td>';
        echo '<td>' . $remote . jm_confidence_badge($remote_conf) . '</td>';
        echo '<td>' . $salary . jm_confidence_badge($salary_conf) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'source_name', true)) . '</td>';
        $apply_cell = $apply_url ? '<a class="jm-apply-btn" href="' . $apply_url . '" target="_blank" rel="noopener">Apply &rarr;</a>' : '';
        echo '<td>' . $apply_cell . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'days_active', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'archived_date', true)) . '</td>';
        echo '</tr>';
    }
    echo '</tbody>';
    echo '<tfoot><tr>' . str_repeat('<th></th>', 10) . '</tr></tfoot>';
    echo '</table>';
    $init = jm_datatables_init_complete_js();
    echo "<script>jQuery(function(\$){\$('#jm-archive').DataTable({responsive:true,order:[[9,'desc']],pageLength:50,{$init}});});</script>";
    $html = ob_get_clean();
    set_transient('jm_archived_jobs_html', $html, 12 * HOUR_IN_SECONDS);
    return $html;
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
