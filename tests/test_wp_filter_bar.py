"""R9-Part-1 regression: verify the shortcode's rendered HTML contains the
server-side filter bar with real chips (not empty containers) and that
filterable cells carry `data-search` attributes.

The plugin PHP isn't executed here — we don't bootstrap WordPress. Instead we
read the PHP source, extract the literal HTML template strings used for chip
rendering and cell attributes, and assert the expected tokens exist. This
catches regressions like "somebody deleted data-search again" or "chip
rendering reverted to empty containers" without needing a PHP interpreter.

If you want a true end-to-end check, run the plugin in a WP test harness —
but 3+ rounds of filter-bar bugs were caused by subtle JS/DOM mismatches that
a textual check would have flagged immediately.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

PHP_PATH = Path(__file__).resolve().parent.parent / "wordpress" / "job-monitor.php"


@pytest.fixture(scope="module")
def plugin_src() -> str:
    return PHP_PATH.read_text(encoding="utf-8")


# ── Server-side chip rendering ─────────────────────────────────────

def test_filter_bar_emits_real_chip_labels(plugin_src):
    """jm_filter_bar_html must render actual <label>...<input type=checkbox>
    elements — not an empty <div class="jm-checkboxes"> container."""
    # Look for the chip template string
    assert "<label class=\"jm-chip\"><input type=\"checkbox\"" in plugin_src, \
        "filter bar no longer emits chip <label>s — check jm_filter_bar_html"


def test_filter_bar_checkboxes_start_checked(plugin_src):
    """All chips render with `checked` so the default state is 'show all'."""
    assert 'type="checkbox" value="\' . $safe . \'" checked' in plugin_src


def test_filter_bar_accepts_value_map(plugin_src):
    """The helper signature must be (id_suffix, filter_values) — the old
    variant took a list of column names and populated chips JS-side."""
    assert "function jm_filter_bar_html($id_suffix, $filter_values)" in plugin_src


# ── Cell rendering uses data-search (native DataTables attr) ───────

def test_remote_cell_has_data_search(plugin_src):
    """Remote column cell: data-search carries the logical value
    ('remote'/'hybrid'/'onsite'/'unknown') so the filter regex matches
    exactly, independent of the confidence badge HTML in the cell."""
    assert "data-search=\"' . esc_attr($remote_v) . '\">" in plugin_src


def test_source_cell_has_data_search(plugin_src):
    """Source column cell carries the mapped display name in data-search."""
    assert "data-search=\"' . esc_attr($source_v) . '\">" in plugin_src


def test_relevance_cell_helper_emits_data_search(plugin_src):
    """jm_relevance_cell returns a <td data-search="Relevant|Partial|Auto">."""
    assert "<td data-search=\"' . esc_attr($label) . '\">" in plugin_src


def test_no_data_filter_attrs_remain(plugin_src):
    """R9-P1-C: we replaced data-filter (custom row filter) with data-search
    (native DataTables). Any residual data-filter would mean the rewrite
    didn't fully land."""
    # Allow the string to appear inside comments for historical context, but
    # strip comments before scanning the code.
    no_comments = re.sub(r"/\*.*?\*/", "", plugin_src, flags=re.DOTALL)
    no_comments = re.sub(r"(?m)//[^\n]*", "", no_comments)
    assert "data-filter=" not in no_comments, \
        "data-filter attribute still present — should be data-search"


# ── JS — the old initComplete helper is gone, replaced by thin wiring ──

def test_initComplete_helper_deleted(plugin_src):
    """R9-P1-D: jm_datatables_init_complete_js is deleted."""
    assert "function jm_datatables_init_complete_js" not in plugin_src


def test_filter_wire_js_helper_exists(plugin_src):
    assert "function jm_filter_wire_js" in plugin_src


def test_no_custom_row_filter_in_wire_js(plugin_src):
    """The new wire JS must NOT push a custom row filter — DataTables'
    native data-search does the column filtering."""
    assert "dataTable.ext.search.push" not in plugin_src


def test_datatable_init_uses_dom_lrtip(plugin_src):
    """dom:'lrtip' hides the default DataTables search box since we render
    our own text filters in the filter bar."""
    assert "dom:'lrtip'" in plugin_src


# ── Source mapping helper ─────────────────────────────────────────

def test_source_display_name_helper_exists(plugin_src):
    """R9-P1-A: renamed helper is jm_display_source_name."""
    assert "function jm_display_source_name($source_name)" in plugin_src


def test_source_display_name_keeps_ats_names_distinct(plugin_src):
    """Per spec, ATS names are preserved (Greenhouse/Lever/Ashby remain
    distinct chips — which ATS a job came from is useful)."""
    # Verify the mapping for ATS entries exists
    for name in ["'greenhouse' => 'Greenhouse'",
                 "'lever' => 'Lever'",
                 "'ashby' => 'Ashby'"]:
        assert name in plugin_src, f"ATS mapping missing: {name}"


def test_source_display_name_collapses_niche_boards(plugin_src):
    """Niche boards consolidate into a single 'Niche Board' chip."""
    assert "['onemodel', 'included_ai', 'siop'], true)" in plugin_src
    assert "return 'Niche Board';" in plugin_src


def test_source_display_name_collapses_rss_alerts(plugin_src):
    assert "['google_alerts', 'talkwalker'], true)" in plugin_src
    assert "return 'RSS Alert';" in plugin_src


# ── CSS hardened with !important ──────────────────────────────────

def test_chip_checkbox_visibility_forced(plugin_src):
    """Theme CSS often sets input[type=checkbox]{opacity:0} for the
    screen-reader-text pattern. !important overrides it."""
    assert "opacity:1!important" in plugin_src
    assert "appearance:auto!important" in plugin_src
    assert "-webkit-appearance:checkbox!important" in plugin_src


def test_text_filters_forced_horizontal(plugin_src):
    """Avoid theme-forced flex-direction:column turning text inputs into a
    vertical stack."""
    assert "flex-direction:row!important" in plugin_src
    assert "max-width:180px!important" in plugin_src


# ── Two-pass render wiring ────────────────────────────────────────

def test_shortcodes_use_two_pass_filter_collection(plugin_src):
    """R9-P1-E: shortcodes accumulate $filter_values during the row loop
    and pass the map to jm_filter_bar_html. Confirm the plumbing is there
    in BOTH shortcodes."""
    # Matches both [job_table] and [job_archive_table]
    assert plugin_src.count("echo jm_filter_bar_html(") == 2
    assert plugin_src.count("$filter_values = [") == 2
    assert plugin_src.count("$rows_html = '';") == 2  # buffer for pass 2


# ── Data-search values are logical tokens, not display HTML ───────

def test_remote_data_search_uses_logical_value(plugin_src):
    """remote_v is the raw meta value (remote/hybrid/onsite/unknown), not
    the HTML that includes the confidence badge."""
    # The variable that feeds data-search must come straight from meta
    assert "$remote_v    = get_post_meta($j->ID, 'is_remote', true) ?: 'unknown';" in plugin_src


def test_source_data_search_uses_display_name(plugin_src):
    """source_v is the mapped display name, matching the chip filter values."""
    assert "$source_v    = jm_display_source_name(get_post_meta($j->ID, 'source_name', true));" in plugin_src
