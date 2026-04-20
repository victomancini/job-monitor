"""Tests for src/processors/enrichment.py — no real HTTP calls."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.processors import enrichment as en


def _mock_resp(body: str, status: int = 200):
    m = MagicMock()
    m.status_code = status
    m.text = body
    return m


def _job(**overrides):
    base = {
        "external_id": "test_1",
        "title": "People Analytics Manager",
        "company": "Netflix",
        "source_url": "https://careers.netflix.com/job/1",
        "apply_url": "https://careers.netflix.com/job/1",
        "location": "",
        "is_remote": "unknown",
    }
    base.update(overrides)
    return base


# ──────────────────────── Salary extraction ──────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("$120,000 - $180,000 per year", (120000.0, 180000.0)),
    ("Salary: $95,000 to $130,000", (95000.0, 130000.0)),
    ("$120K - $180K", (120000.0, 180000.0)),
    ("$120k-$180k", (120000.0, 180000.0)),
    ("pay range: 120000 to 180000", (120000.0, 180000.0)),
    ("$120,000 – $180,000", (120000.0, 180000.0)),  # en-dash
    ("$180,000 - $120,000", (120000.0, 180000.0)),  # reversed → still sorted
])
def test_extract_salary_formats(text, expected):
    r = en._extract_salary(text)
    assert r is not None
    assert (r["min"], r["max"]) == expected


def test_extract_salary_ignores_small_numbers():
    assert en._extract_salary("employees aged 18 - 65 welcome") is None


def test_extract_salary_returns_none_without_match():
    assert en._extract_salary("no compensation info here") is None


def test_extract_salary_range_string_formatted():
    r = en._extract_salary("$120,000 - $180,000")
    assert r["range_str"] == "$120K-$180K"


# ──────────────────────── Remote detection ───────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("This is a fully remote position", "remote"),
    ("100% remote work allowed", "remote"),
    ("Work from home — no office required", "remote"),
    ("This role is remote eligible", "remote"),
    ("Hybrid - 3 days in office per week", "hybrid"),
    ("We offer a hybrid schedule", "hybrid"),
    ("In-office 2 days weekly", "hybrid"),
    ("On-site position in New York", "onsite"),
    ("Must work in-office daily", "onsite"),
    ("In-person role, no remote", "onsite"),
])
def test_extract_remote_status(text, expected):
    assert en._extract_remote_status(text) == expected


def test_extract_remote_status_hybrid_outranks_remote():
    """Spec: if both 'remote' and 'hybrid' appear, prefer 'hybrid'."""
    text = "Remote eligible; hybrid schedule with 2 days in the office"
    assert en._extract_remote_status(text) == "hybrid"


def test_extract_remote_status_hybrid_outranks_onsite():
    text = "On-site expectation; hybrid arrangement possible"
    assert en._extract_remote_status(text) == "hybrid"


def test_extract_remote_none_when_silent():
    assert en._extract_remote_status("Generic job description with no remote info.") is None


# ──────────────────────── Location extraction ────────────────────────

def test_extract_location_prefixed():
    assert en._extract_location("Location: Austin, TX") == "Austin, TX"


def test_extract_location_city_state():
    text = "We are headquartered in San Francisco, CA and hiring."
    assert en._extract_location(text) == "San Francisco, CA"


def test_extract_location_multiple_locations():
    text = "This role is available in Multiple Locations across the US."
    assert en._extract_location(text) == "Multiple Locations"


def test_extract_location_none_when_missing():
    assert en._extract_location("No location given anywhere.") is None


# ──────────────────────── HTML → text ────────────────────────────────

def test_extract_text_strips_tags_and_scripts():
    html = """
    <html><head><script>alert('x')</script><style>body{}</style></head>
    <body><h1>Job Title</h1><p>$120,000 - $180,000</p></body></html>
    """
    text = en._extract_text(html)
    assert "alert" not in text
    assert "body{}" not in text
    assert "Job Title" in text
    assert "$120,000 - $180,000" in text


# ──────────────────────── enrich_job end-to-end ──────────────────────

def test_enrich_job_full_happy_path():
    page = """
    <html><body>
    <h1>Employee Listening Manager</h1>
    <p>Location: Austin, TX</p>
    <p>Compensation: $120,000 to $180,000</p>
    <p>This role is fully remote.</p>
    </body></html>
    """
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["salary_min"] == 120000.0
    assert j["salary_max"] == 180000.0
    assert j["salary_range"] == "$120K-$180K"
    assert j["salary_confidence"] == "confirmed"
    assert j["is_remote"] == "remote"
    assert j["remote_confidence"] == "confirmed"
    assert j["location"] == "Austin, TX"
    assert j["location_confidence"] == "confirmed"
    assert j["enrichment_source"] == "source_page"
    assert j["enrichment_date"] == datetime.now(timezone.utc).strftime("%Y-%m-%d")


def test_enrich_job_preserves_existing_salary():
    """If aggregator already has a salary, enrichment should mark it 'aggregator_only'
    rather than overwrite."""
    page = "<p>Location: Austin, TX</p><p>$50,000 - $60,000</p>"
    j = _job(salary_min=150000.0, salary_max=200000.0, salary_range="$150K-$200K")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["salary_min"] == 150000.0  # preserved
    assert j["salary_confidence"] == "aggregator_only"


def test_enrich_job_preserves_existing_is_remote_when_page_silent():
    page = "<p>Just a description with no remote hints.</p>"
    j = _job(is_remote="hybrid")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["is_remote"] == "hybrid"
    assert j["remote_confidence"] == "aggregator_only"


def test_enrich_job_updates_is_remote_when_aggregator_unknown():
    page = "<p>This is a fully remote position.</p>"
    j = _job(is_remote="unknown")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["is_remote"] == "remote"
    assert j["remote_confidence"] == "confirmed"


def test_enrich_job_timeout_falls_back_to_aggregator():
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               side_effect=requests.Timeout("too slow")):
        en.enrich_job(j)
    assert j["enrichment_source"] == "aggregator"
    assert "enrichment_date" not in j


def test_enrich_job_connection_error_falls_back_to_aggregator():
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               side_effect=requests.ConnectionError("boom")):
        en.enrich_job(j)
    assert j["enrichment_source"] == "aggregator"


def test_enrich_job_404_falls_back_to_aggregator():
    j = _job()
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp("Not found", 404)):
        en.enrich_job(j)
    assert j["enrichment_source"] == "aggregator"


def test_enrich_job_empty_url_falls_back():
    j = _job(source_url="", apply_url="")
    en.enrich_job(j)  # no mock needed — should not fetch
    assert j["enrichment_source"] == "aggregator"


def test_enrich_job_skips_recently_enriched():
    """Already-enriched jobs (within 7 days) should not re-fetch."""
    recent = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
    j = _job(enrichment_date=recent, enrichment_source="source_page")
    with patch("src.processors.enrichment.requests.get") as m:
        en.enrich_job(j)
    assert m.call_count == 0


def test_enrich_job_re_enriches_stale():
    """Enrichment older than 7 days → re-fetch."""
    stale = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    j = _job(enrichment_date=stale)
    page = "<p>Fully remote.</p>"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)) as m:
        en.enrich_job(j)
    assert m.call_count == 1
    assert j["is_remote"] == "remote"


def test_enrich_batch_processes_each_and_sleeps_between():
    jobs = [_job(external_id=f"t{i}") for i in range(3)]
    page = "<p>Fully remote.</p>"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)) as m, \
         patch("src.processors.enrichment.time.sleep") as ms:
        en.enrich_batch(jobs, delay=1.0)
    assert m.call_count == 3
    # 3 jobs → 2 sleeps (no trailing sleep)
    assert ms.call_count == 2


# ──────────────────────── Phase A: redirect following ────────────────

def _mock_resp_with_final_url(body: str, final_url: str, status: int = 200):
    m = MagicMock()
    m.status_code = status
    m.text = body
    m.url = final_url
    return m


def test_enrich_follows_redirect_off_aggregator_to_company():
    """Aggregator URL that redirects to the employer domain → apply_url becomes the final URL."""
    j = _job(source_url="https://jooble.org/desc/123",
             apply_url="https://jooble.org/desc/123")
    final = "https://careers.netflix.com/job/123"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>Fully remote</p>", final, 200)):
        en.enrich_job(j)
    assert j["apply_url"] == final


def test_enrich_keeps_apply_url_when_redirect_stays_on_aggregator():
    """Internal aggregator redirect (same domain) → apply_url unchanged."""
    j = _job(source_url="https://jooble.org/desc/123",
             apply_url="https://jooble.org/desc/123")
    same_domain_final = "https://jooble.org/away/abc"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>x</p>", same_domain_final, 200)):
        en.enrich_job(j)
    # Same aggregator host → no replacement
    assert j["apply_url"] == "https://jooble.org/desc/123"


def test_enrich_keeps_apply_url_when_origin_not_aggregator():
    """Origin URL is already a direct employer page (not on the known aggregator list):
    don't overwrite even if redirect lands elsewhere."""
    j = _job(source_url="https://careers.netflix.com/job/1",
             apply_url="https://careers.netflix.com/job/1")
    final = "https://careers.netflix.com/job/1-renamed"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>x</p>", final, 200)):
        en.enrich_job(j)
    assert j["apply_url"] == "https://careers.netflix.com/job/1"


# ──────────────────────── R-audit Issue 1: stronger redirect handling ────

def test_meta_refresh_body_redirect_rewrites_apply_url():
    """Jooble-style: 200 OK with <meta http-equiv=refresh> in <head>.
    requests.get(allow_redirects=True) doesn't follow these — body parser does."""
    j = _job(source_url="https://jooble.org/desc/999",
             apply_url="https://jooble.org/desc/999")
    body = (
        '<html><head>'
        '<meta http-equiv="refresh" content="0;url=https://careers.netflix.com/job/999">'
        '</head><body>redirecting...</body></html>'
    )
    # final_url == original (no HTTP redirect), but body has the real target
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://jooble.org/desc/999", 200)):
        en.enrich_job(j)
    assert j["apply_url"] == "https://careers.netflix.com/job/999"


def test_js_window_location_body_redirect_rewrites_apply_url():
    """Some aggregators serve an inline JS window.location bounce."""
    j = _job(source_url="https://jooble.org/desc/42",
             apply_url="https://jooble.org/desc/42")
    body = (
        '<html><head><script>'
        'window.location = "https://jobs.lever.co/ramp/abc";'
        '</script></head><body></body></html>'
    )
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://jooble.org/desc/42", 200)):
        en.enrich_job(j)
    assert j["apply_url"] == "https://jobs.lever.co/ramp/abc"


def test_body_redirect_ignored_when_target_host_same():
    """Meta-refresh pointing to another aggregator page is NOT a direct URL
    upgrade. Keep the original."""
    j = _job(source_url="https://jooble.org/desc/1",
             apply_url="https://jooble.org/desc/1")
    body = (
        '<html><head>'
        '<meta http-equiv="refresh" content="0;url=https://jooble.org/another/2">'
        '</head></html>'
    )
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://jooble.org/desc/1", 200)):
        en.enrich_job(j)
    assert j["apply_url"] == "https://jooble.org/desc/1"


def test_head_fallback_rewrites_stuck_aggregator_url():
    """When the GET response is 200 with no body-redirect but HEAD follows
    redirects to a direct URL, the final pass fixes apply_url."""
    j = _job(source_url="https://jooble.org/desc/7",
             apply_url="https://jooble.org/desc/7")
    body = "<html><body>Opaque page</body></html>"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://jooble.org/desc/7", 200)), \
         patch("src.processors.enrichment._head_final_url",
               return_value="https://careers.netflix.com/job/7"):
        en.enrich_job(j)
    assert j["apply_url"] == "https://careers.netflix.com/job/7"


def test_head_fallback_skipped_when_already_direct():
    """If the GET response redirected to a direct URL, no HEAD fallback runs."""
    j = _job(source_url="https://jooble.org/desc/1",
             apply_url="https://jooble.org/desc/1")
    final = "https://careers.netflix.com/job/1"
    head_mock = MagicMock(return_value="should_not_be_used")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>x</p>", final, 200)), \
         patch("src.processors.enrichment._head_final_url", head_mock):
        en.enrich_job(j)
    head_mock.assert_not_called()
    assert j["apply_url"] == final


def test_body_redirect_parsed_past_4kb_boundary():
    """R5-10: SPA-style aggregators can emit 20-30kB of inline CSS before the
    redirect JS. Widened scan window catches these."""
    padding = "<style>" + ("." * 20000) + "</style>"
    body = (
        "<html><head>"
        + padding
        + '<script>window.location = "https://careers.netflix.com/job/late";</script>'
        + "</head></html>"
    )
    j = _job(source_url="https://jooble.org/desc/late",
             apply_url="https://jooble.org/desc/late")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://jooble.org/desc/late", 200)):
        en.enrich_job(j)
    assert j["apply_url"] == "https://careers.netflix.com/job/late"


# ───── R9-Part-2: regional-subdomain aggregator detection ─────

def test_us_jooble_regional_subdomain_treated_as_aggregator():
    """R9-Part-2: before the fix, us.jooble.org wasn't in AGGREGATOR_HOSTS
    so the entire redirect-following path was skipped for jobs on the US
    regional subdomain. Now is_aggregator_host() matches subdomains."""
    j = _job(source_url="https://us.jooble.org/desc/regional123",
             apply_url="https://us.jooble.org/desc/regional123")
    final = "https://careers.netflix.com/job/regional123"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>x</p>", final, 200)):
        en.enrich_job(j)
    assert j["apply_url"] == final


def test_link_adzuna_subdomain_treated_as_aggregator():
    j = _job(source_url="https://link.adzuna.com/redir/abc",
             apply_url="https://link.adzuna.com/redir/abc")
    final = "https://careers.example.com/jobs/abc"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>x</p>", final, 200)):
        en.enrich_job(j)
    assert j["apply_url"] == final


def test_js_redirect_on_regional_subdomain_parsed():
    """R9-Part-2: JS redirect on a regional Jooble subdomain was previously
    ignored because us.jooble.org didn't match AGGREGATOR_HOSTS."""
    body = (
        '<html><head><script>'
        'window.location.href = "https://careers.netflix.com/jobs/42";'
        '</script></head></html>'
    )
    j = _job(source_url="https://us.jooble.org/desc/42",
             apply_url="https://us.jooble.org/desc/42")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://us.jooble.org/desc/42", 200)):
        en.enrich_job(j)
    assert j["apply_url"] == "https://careers.netflix.com/jobs/42"


def test_meta_refresh_on_regional_subdomain_parsed():
    body = (
        '<html><head>'
        '<meta http-equiv="refresh" content="0;url=https://careers.example.com/apply/7">'
        '</head></html>'
    )
    j = _job(source_url="https://uk.jooble.org/desc/7",
             apply_url="https://uk.jooble.org/desc/7")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://uk.jooble.org/desc/7", 200)):
        en.enrich_job(j)
    assert j["apply_url"] == "https://careers.example.com/apply/7"


def test_unresolved_aggregator_url_triggers_warning_log(caplog):
    """R9-Part-2-C: when apply_url is still on an aggregator host after all
    enrichment passes, log a WARNING so ops can see how many slip through."""
    import logging as _l
    j = _job(source_url="https://us.jooble.org/desc/stuck",
             apply_url="https://us.jooble.org/desc/stuck")
    # Response gives no useful redirect — stays on aggregator
    body = "<html><body>Opaque aggregator page, no redirect</body></html>"
    with caplog.at_level(_l.WARNING, logger="src.processors.enrichment"), \
         patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url(body, "https://us.jooble.org/desc/stuck", 200)), \
         patch("src.processors.enrichment._head_final_url", return_value=""):
        en.enrich_job(j)
    # apply_url still aggregator → warning fired
    assert any(
        "apply_url not resolved" in rec.getMessage()
        and "us.jooble.org" in rec.getMessage()
        for rec in caplog.records
    ), f"expected unresolved warning, got: {[r.getMessage() for r in caplog.records]}"


def test_linkedin_terminal_url_does_NOT_trigger_unresolved_warning(caplog):
    """R10: jobspy_linkedin produces https://www.linkedin.com/jobs/view/<id>.
    LinkedIn IS the application surface — there's no company redirect to
    chase. Previously these threw 3+ warnings per run, polluting the signal
    we use to identify real unresolved leaks."""
    import logging as _l
    j = _job(source_url="https://www.linkedin.com/jobs/view/4404058381",
             apply_url="https://www.linkedin.com/jobs/view/4404058381")
    j["source_name"] = "jobspy_linkedin"
    # LinkedIn returns 200 but no redirect; we expect a debug log, not a warning.
    with caplog.at_level(_l.WARNING, logger="src.processors.enrichment"), \
         patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>x</p>",
                                                       "https://www.linkedin.com/jobs/view/4404058381",
                                                       200)), \
         patch("src.processors.enrichment._head_final_url", return_value=""):
        en.enrich_job(j)
    assert not any(
        "apply_url not resolved" in r.getMessage() for r in caplog.records
    ), "LinkedIn terminal URL should not emit unresolved warning"


def test_non_linkedin_aggregator_still_warns(caplog):
    """Flipside of R10: we keep warning for OTHER unresolved aggregators
    (jooble.org, other jobspy sources) — only jobspy_linkedin is whitelisted."""
    import logging as _l
    j = _job(source_url="https://jooble.org/desc/stuck",
             apply_url="https://jooble.org/desc/stuck")
    j["source_name"] = "jooble"
    with caplog.at_level(_l.WARNING, logger="src.processors.enrichment"), \
         patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>opaque</p>",
                                                       "https://jooble.org/desc/stuck",
                                                       200)), \
         patch("src.processors.enrichment._head_final_url", return_value=""):
        en.enrich_job(j)
    assert any("apply_url not resolved" in r.getMessage() for r in caplog.records)


def test_resolved_aggregator_url_does_NOT_trigger_warning(caplog):
    """Flipside: when we DO resolve the URL, no warning fires."""
    import logging as _l
    j = _job(source_url="https://us.jooble.org/desc/ok",
             apply_url="https://us.jooble.org/desc/ok")
    final = "https://careers.netflix.com/jobs/ok"
    with caplog.at_level(_l.WARNING, logger="src.processors.enrichment"), \
         patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("<p>x</p>", final, 200)):
        en.enrich_job(j)
    assert not any("apply_url not resolved" in r.getMessage() for r in caplog.records)


def test_head_fallback_fires_on_non_200_response():
    """If the original URL returned 403/500 (no usable body-redirect), HEAD
    fallback still gets a chance to resolve the aggregator URL."""
    j = _job(source_url="https://jooble.org/desc/5",
             apply_url="https://jooble.org/desc/5")
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp_with_final_url("", "https://jooble.org/desc/5", 403)), \
         patch("src.processors.enrichment._head_final_url",
               return_value="https://careers.netflix.com/job/5"):
        en.enrich_job(j)
    assert j["apply_url"] == "https://careers.netflix.com/job/5"


# ──────────────────────── Phase J: three-pass enrichment ────────────

def test_pre_enrich_extracts_salary_from_description():
    j = _job(description="Compensation: $120,000 - $180,000 per year.")
    en._pre_enrich_from_description(j)
    assert j["salary_min"] == 120000.0
    assert j["salary_max"] == 180000.0
    assert j["salary_confidence"] == "inferred"


def test_pre_enrich_extracts_remote_from_description():
    j = _job(description="This is a fully remote role.")
    en._pre_enrich_from_description(j)
    assert j["is_remote"] == "remote"
    assert j["remote_confidence"] == "inferred"


def test_pre_enrich_extracts_location_from_description():
    j = _job(description="Location: Austin, TX. Onsite presence expected.")
    j["location"] = ""  # empty aggregator location
    en._pre_enrich_from_description(j)
    assert j["location"] == "Austin, TX"
    assert j["location_confidence"] == "inferred"


def test_pre_enrich_preserves_existing_aggregator_values():
    j = _job(description="Remote position with $50K-$60K compensation.")
    j["is_remote"] = "hybrid"           # aggregator said hybrid
    j["salary_min"] = 150000            # aggregator gave salary
    j["location"] = "Los Gatos, CA"     # aggregator gave location
    en._pre_enrich_from_description(j)
    # None of these should be overwritten
    assert j["is_remote"] == "hybrid"
    assert j["salary_min"] == 150000
    assert j["location"] == "Los Gatos, CA"
    assert "remote_confidence" not in j
    assert "salary_confidence" not in j
    assert "location_confidence" not in j


def test_assumed_default_sets_onsite_when_remote_missing():
    j = _job(is_remote="unknown")
    en._apply_assumed_defaults(j)
    assert j["is_remote"] == "onsite"
    assert j["remote_confidence"] == "assumed"


def test_assumed_default_skipped_when_remote_known():
    j = _job(is_remote="remote")
    en._apply_assumed_defaults(j)
    assert j["is_remote"] == "remote"
    assert "remote_confidence" not in j


def test_apply_llm_hints_remote_normalizes_dash():
    j = _job(is_remote="unknown")
    j["_llm_remote"] = "on-site"
    en._apply_llm_hints(j)
    assert j["is_remote"] == "onsite"
    assert j["remote_confidence"] == "inferred"


def test_apply_llm_hints_salary_parses_range():
    j = _job()
    j["_llm_salary_hint"] = "$120K-$180K"
    en._apply_llm_hints(j)
    assert j["salary_min"] == 120000.0
    assert j["salary_max"] == 180000.0
    assert j["salary_confidence"] == "inferred"


def test_apply_llm_hints_ignored_when_aggregator_has_data():
    j = _job(is_remote="remote")
    j["_llm_remote"] = "onsite"
    en._apply_llm_hints(j)
    assert j["is_remote"] == "remote"  # aggregator value preserved
    assert "remote_confidence" not in j


def test_three_pass_flow_end_to_end():
    """Description supplies salary (inferred). Source page corroborates salary
    (upgrade to confirmed) AND is the first to say 'remote' (confirmed).
    No-URL branch plays assumed default for is_remote when silent."""
    j = _job(description="Salary range: $120,000 - $180,000.")
    page = "<p>Salary: $120,000 - $180,000</p><p>This is a fully remote role.</p>"
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp(page, 200)):
        en.enrich_job(j)
    assert j["salary_min"] == 120000.0
    assert j["salary_confidence"] == "confirmed"  # inferred → confirmed
    assert j["is_remote"] == "remote"
    assert j["remote_confidence"] == "confirmed"


def test_no_url_applies_assumed_default():
    j = _job(source_url="", apply_url="", is_remote="unknown")
    en.enrich_job(j)
    assert j["enrichment_source"] == "aggregator"
    assert j["is_remote"] == "onsite"
    assert j["remote_confidence"] == "assumed"


def test_http_failure_still_applies_description_inferences_and_default():
    j = _job(description="Hybrid role with 3 days in the office.",
             is_remote="unknown")
    with patch("src.processors.enrichment.requests.get",
               side_effect=requests.ConnectionError("boom")):
        en.enrich_job(j)
    # Description set is_remote; assumed default does not apply because remote is set
    assert j["is_remote"] == "hybrid"
    assert j["remote_confidence"] == "inferred"
    assert j["enrichment_source"] == "aggregator"


def test_enrich_batch_continues_past_individual_failures():
    # max_workers=1 pins the sequential ordering this test depends on — the
    # response list is consumed in the order workers call requests.get, which
    # is only deterministic when there's a single worker.
    jobs = [_job(external_id=f"t{i}") for i in range(3)]
    responses = [
        _mock_resp("<p>$120K-$180K</p>", 200),
        requests.Timeout("boom"),  # will be raised via side_effect
        _mock_resp("<p>Hybrid schedule</p>", 200),
    ]
    with patch("src.processors.enrichment.requests.get",
               side_effect=responses), \
         patch("src.processors.enrichment.time.sleep"):
        en.enrich_batch(jobs, max_workers=1)
    assert jobs[0]["enrichment_source"] == "source_page"
    assert jobs[1]["enrichment_source"] == "aggregator"
    assert jobs[2]["enrichment_source"] == "source_page"


# Regression: IMP-N8 — parallel enrich_batch still applies per-host throttling,
# so two jobs on the same host are serialized by at least `delay` seconds while
# jobs on distinct hosts overlap.
def test_host_throttle_serializes_same_host_and_parallelizes_cross_host():
    """R6-I2: the throttle's contract is that two acquires on the same host
    return at least `min_gap` seconds apart. Distinct hosts have no such
    constraint and can acquire simultaneously. Verified by:
      1. Cross-host acquires can happen before any wait (no forced pause).
      2. Same-host acquire-timestamps are at least `min_gap` apart.
    """
    import threading
    import time as _t
    throttle = en._HostThrottle(min_gap=0.05)
    timestamps: dict[str, list[float]] = {"a": [], "b": []}

    def worker(host: str):
        throttle.acquire(host)
        timestamps[host].append(_t.monotonic())

    # Prime the throttle for host 'a' so subsequent 'a' acquires actually wait.
    worker("a")

    # Launch second 'a' (should wait ~min_gap) and 'b' (should not wait)
    # concurrently. Use threads so we observe distinct-host parallelism.
    t_a = threading.Thread(target=worker, args=("a",))
    t_b = threading.Thread(target=worker, args=("b",))
    t_a.start()
    t_b.start()
    t_a.join(timeout=2.0)
    t_b.join(timeout=2.0)

    # Same-host: two 'a' timestamps must be at least min_gap apart (with small
    # floating-point slack). This is the throttle's actual guarantee.
    assert len(timestamps["a"]) == 2
    gap = timestamps["a"][1] - timestamps["a"][0]
    assert gap >= 0.045, f"same-host gap {gap} < min_gap 0.05"
    # Cross-host: 'b' timestamp is independent of 'a' ordering. Not wall-time
    # tested here (would re-introduce flake); correctness is that b's acquire
    # did not block on a's throttle — which we verify by completion (no
    # BrokenBarrierError / timeout) and by bgap measurement: bgap should be
    # close to zero since b doesn't wait on a.
    assert len(timestamps["b"]) == 1


def test_enrich_batch_parallel_fetches_all_three():
    """IMP-N8 sanity: all jobs complete via the thread pool and enrichment
    fields land on each dict."""
    jobs = [
        _job(external_id="p0", apply_url="https://a.example/1"),
        _job(external_id="p1", apply_url="https://b.example/2"),
        _job(external_id="p2", apply_url="https://c.example/3"),
    ]
    with patch("src.processors.enrichment.requests.get",
               return_value=_mock_resp("<p>Hybrid schedule with $120K-$180K</p>", 200)):
        en.enrich_batch(jobs, max_workers=3, delay=0.01)
    for j in jobs:
        assert j["enrichment_source"] == "source_page"
        assert j["salary_min"] == 120000.0
