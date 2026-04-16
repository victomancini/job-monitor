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
        'salary_range','source_url','external_id','source_name','category','seniority',
        'fit_score','is_remote','work_arrangement','first_seen_date','last_seen_active',
        'archived_date','days_active','job_status','keywords_matched','description_snippet',
        'keyword_score','llm_classification','llm_confidence','llm_provider'];
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
            'salary_range','source_url','external_id','source_name','category','seniority',
            'fit_score','is_remote','work_arrangement','keywords_matched','description_snippet',
            'keyword_score','llm_classification','llm_confidence','llm_provider'];
        $meta = [];
        foreach ($allowed as $k) {
            if (isset($job[$k])) {
                $meta[$k] = sanitize_text_field(mb_substr($job[$k], 0, 500));
            }
        }
        if (isset($job['source_url'])) {
            $meta['source_url'] = esc_url_raw($job['source_url']);
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
    echo '<table id="jm-table" class="display nowrap" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Remote</th><th>Salary</th><th>Source</th><th>First Seen</th></tr></thead><tbody>';
    foreach ($jobs as $j) {
        $url = esc_url(get_post_meta($j->ID, 'source_url', true));
        $title = esc_html($j->post_title);
        $t = $url ? '<a href="' . $url . '" target="_blank" rel="noopener">' . $title . '</a>' : $title;
        echo '<tr>';
        echo '<td>' . $t . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'location', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'is_remote', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'salary_range', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'source_name', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'first_seen_date', true)) . '</td>';
        echo '</tr>';
    }
    echo '</tbody></table>';
    echo '<script>jQuery(function($){$("#jm-table").DataTable({responsive:true,order:[[6,"desc"]],pageLength:50});});</script>';
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
    echo '<table id="jm-archive" class="display" style="width:100%">';
    echo '<thead><tr><th>Title</th><th>Company</th><th>Location</th><th>Days Active</th><th>Archived</th></tr></thead><tbody>';
    foreach ($jobs as $j) {
        $url = esc_url(get_post_meta($j->ID, 'source_url', true));
        $title = esc_html($j->post_title);
        $t = $url ? '<a href="' . $url . '" target="_blank" rel="noopener">' . $title . '</a>' : $title;
        echo '<tr>';
        echo '<td>' . $t . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'company', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'location', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'days_active', true)) . '</td>';
        echo '<td>' . esc_html(get_post_meta($j->ID, 'archived_date', true)) . '</td>';
        echo '</tr>';
    }
    echo '</tbody></table>';
    echo '<script>jQuery(function($){$("#jm-archive").DataTable({order:[[4,"desc"]],pageLength:50});});</script>';
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
