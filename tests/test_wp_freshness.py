"""R11 Phase 0 regression: verify the PHP plugin's freshness / first_seen_date
handling matches the Python-side invariants.

The plugin PHP isn't executed — we read the source and assert specific
invariants that, if broken, would re-introduce the NEW-today bug (jobs
stamped as new even when Turso has seen them for weeks) or the unreliable
"Posted X days ago" sort.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

PHP_PATH = Path(__file__).resolve().parent.parent / "wordpress" / "job-monitor.php"


@pytest.fixture(scope="module")
def plugin_src() -> str:
    return PHP_PATH.read_text(encoding="utf-8")


def test_freshness_cell_accepts_days_since_posted_param(plugin_src):
    """jm_freshness_cell must take days_since_posted + is_brand_new so it can
    use the Python-computed integer instead of re-parsing dates in the
    site's local timezone (R8-M6 tried to patch that with 'UTC' suffix
    parsing; R11 replaces it with the authoritative integer)."""
    m = re.search(
        r"function\s+jm_freshness_cell\s*\(\s*\$date_posted\s*,\s*\$first_seen"
        r"\s*,\s*\$days_since_posted\s*=\s*null\s*,\s*\$is_brand_new\s*=\s*false\s*\)",
        plugin_src,
    )
    assert m, "jm_freshness_cell signature must accept days_since_posted + is_brand_new"


def test_freshness_cell_prefers_python_days(plugin_src):
    """When days_since_posted is supplied, it's used directly — PHP date math
    is only the fallback for legacy posts missing the meta."""
    assert "(int)$days_since_posted" in plugin_src, \
        "freshness cell must use the Python integer when provided"


def test_new_badge_uses_is_brand_new_not_date_comparison(plugin_src):
    """The NEW badge must NOT compare first_seen_date to today — that's the
    bug that re-stamped weeks-old jobs whenever a WP post was recreated.
    The new trigger is the is_brand_new param, which Python sets only when
    Turso's upsert actually creates a row."""
    # The fragile comparison must be gone
    assert "$first_seen === current_time('Y-m-d', true)" not in plugin_src, (
        "the old NEW-today comparison is back — it causes the re-stamp bug"
    )
    # And the new trigger must be present
    assert "$is_brand_new ?" in plugin_src, \
        "NEW badge must read from is_brand_new param"


def test_freshness_cell_callers_pass_new_meta(plugin_src):
    """Both shortcode renderers (active + archive) must pass the new meta.
    Otherwise the integer days and is_brand_new would be undefined and the
    cell would silently fall back to PHP date math, defeating the fix."""
    # Count jm_freshness_cell invocations; each must span 5 arg lines
    callsites = re.findall(
        r"jm_freshness_cell\(\s*"
        r"get_post_meta\([^,]+,\s*'date_posted',\s*true\)\s*,\s*"
        r"get_post_meta\([^,]+,\s*'first_seen_date',\s*true\)\s*,\s*"
        r"get_post_meta\([^,]+,\s*'days_since_posted',\s*true\)\s*,\s*"
        r"get_post_meta\([^,]+,\s*'is_brand_new',\s*true\)\s*===\s*'1'\s*\)",
        plugin_src,
    )
    assert len(callsites) >= 2, (
        f"expected both shortcode renderers to pass the R11 meta args; found {len(callsites)}"
    )


def test_batch_update_preserves_first_seen_date_on_update(plugin_src):
    """On UPDATE, first_seen_date must only be written when missing OR
    incoming is earlier. Writing unconditionally would re-introduce the
    bug where a recreated post got today's date."""
    # The strcmp-based guard must be present
    m = re.search(
        r"strcmp\(\$incoming_fsd,\s*\$existing_fsd\)\s*<\s*0",
        plugin_src,
    )
    assert m, "UPDATE path must guard first_seen_date with strcmp(incoming, existing) < 0"


def test_batch_update_insert_prefers_python_first_seen_date(plugin_src):
    """On INSERT, use Python's first_seen_date when provided (Turso is
    authoritative). Falls back to today only for payloads predating R11."""
    assert "isset($job['first_seen_date'])" in plugin_src, \
        "INSERT path must check for Python-supplied first_seen_date"
    assert "sanitize_text_field($job['first_seen_date'])" in plugin_src, \
        "INSERT path must use Python's first_seen_date when present"


def test_registered_meta_fields_include_r11_keys(plugin_src):
    """WordPress register_post_meta must list the new fields so the REST
    API and admin UI can see them. Missing from register means meta is
    hidden from REST consumers even though wp_update_post writes it."""
    assert "'days_since_posted'" in plugin_src
    assert "'is_brand_new'" in plugin_src


def test_batch_allowed_list_includes_r11_keys(plugin_src):
    """The $allowed list inside jm_batch_update must carry the new keys or
    the generic copy loop will silently drop them from incoming payloads."""
    # Look specifically near the $allowed declaration inside jm_batch_update
    assert re.search(
        r"\$allowed\s*=\s*\[[^\]]*'days_since_posted'[^\]]*'is_brand_new'",
        plugin_src,
        re.DOTALL,
    ), "R11 keys missing from jm_batch_update $allowed list"


# ─── R11 Phase 6: consensus tooltip ─────────────────────────

def test_consensus_tooltip_helper_defined(plugin_src):
    """jm_consensus_tooltip must exist and accept (agreement, sources, confidence)
    matching the signature the shortcode renderers expect."""
    assert re.search(
        r"function\s+jm_consensus_tooltip\s*\(\s*\$agreement\s*,\s*\$sources\s*,\s*\$confidence\s*\)",
        plugin_src,
    ), "jm_consensus_tooltip signature missing/changed — tooltip won't render"


def test_remote_cell_renders_consensus_tooltip(plugin_src):
    """Both Remote cells (active + archive shortcodes) must pass the vote
    meta through jm_consensus_tooltip so the tooltip appears on hover."""
    # Count occurrences — should be 2 (active shortcode + archive shortcode)
    occurrences = plugin_src.count("jm_consensus_tooltip(")
    # 1 definition + 2 call sites = 3 total occurrences
    assert occurrences >= 3, (
        f"expected jm_consensus_tooltip wired into both shortcodes; found {occurrences}"
    )


def test_consensus_tooltip_ignored_below_two_sources(plugin_src):
    """The helper must return empty string when agreement < 2 — a single-
    source vote isn't consensus and shouldn't clutter the UI."""
    assert "if ($agreement < 2)" in plugin_src, (
        "jm_consensus_tooltip must gate on agreement >= 2"
    )


def test_registered_meta_includes_consensus_keys(plugin_src):
    """R11 Phase 6: vote fields must be registered so they round-trip through
    the REST endpoint and shortcode reads return real values."""
    for key in [
        "remote_vote_confidence",
        "remote_vote_sources",
        "remote_vote_agreement",
        "work_arrangement_vote_confidence",
    ]:
        assert f"'{key}'" in plugin_src, f"{key} not in register_post_meta"
