"""Tests for keyword_filter. Spec: zero false positives; word-boundary matching;
conflict → LLM; google_alerts always → LLM; company boost for Tier 1/2 vendors."""
from __future__ import annotations

import pytest

from src.processors import keyword_filter as kf


def job(title, company="NoCo", description="", source_name="jsearch"):
    return {
        "title": title,
        "company": company,
        "description": description,
        "source_name": source_name,
    }


# ───────────── Good titles: 20 must score >= 50 (auto_include) ─────────────

GOOD_TITLES = [
    "Employee Listening Manager",
    "People Analytics Director",
    "Senior Manager, People Analytics",
    "Head of Employee Listening",
    "Voice of Employee Program Manager",
    "Principal People Scientist",
    "People Scientist",
    "People Analytics Lead",
    "VP People Analytics",
    "Senior Director of People Analytics",
    "Employee Listening Strategy Lead",
    "Continuous Employee Listening Manager",  # Phase B7: co-term "employee" promotes to T1
    "Workforce Sensing Lead",
    "Human Capital Analytics Director",
    "Talent Analytics Manager",
    "Employee Listening Principal",
    "Employee Listening Director",
    "People Analytics Manager",
    "People Analytics Senior",
    "Organizational Network Analysis Lead",
]


@pytest.mark.parametrize("title", GOOD_TITLES)
def test_good_titles_auto_include(title):
    r = kf.classify(job(title))
    assert r["score"] >= 50, f"{title}: score {r['score']}"
    assert r["decision"] == "auto_include"


# ───────────── Bad titles: 20 must score < 10 (reject or low_score) ─────────

BAD_TITLES = [
    "Customer Experience Analyst",
    "Marketing Analytics Manager",
    "Social Media Manager",
    "SEO Analyst",
    "Call Center Manager",
    "Patient Experience Manager",
    "Customer Satisfaction Director",
    "Brand Monitoring Specialist",
    "Voice of Customer Manager",
    "NPS Program Manager",
    "Social Listening Manager",
    "Speech Pathologist",
    "Audiologist",
    "Media Monitoring Analyst",
    "Contact Center Lead",
    "Digital Marketing Specialist",
    "Performance Marketing Manager",
    "Demand Generation Director",
    "Customer Service Representative",
    "Threat Intelligence Analyst",
]


@pytest.mark.parametrize("title", BAD_TITLES)
def test_bad_titles_score_low(title):
    r = kf.classify(job(title))
    assert r["score"] < 10, f"{title}: score {r['score']}"
    assert r["decision"] in ("auto_reject", "low_score")


# ───────────── THE #1 false positive: "active listening" ────────────────────

def test_active_listening_in_title_rejected():
    r = kf.classify(job("Call Center Agent with Active Listening Skills"))
    assert r["decision"] == "auto_reject"
    assert r["score"] == -100


def test_active_listening_in_description_rejected_when_no_positives():
    r = kf.classify(job("Customer Service Rep",
                        description="Requires active listening skills and empathy"))
    assert r["decision"] == "auto_reject"


# ───── R8-shadow-A: auto_reject logs the triggering negative term ─────
# 2026-04-19 shadow log had 547 auto_reject rows with matched=[] — unattributable.

def test_auto_reject_records_triggering_negative_in_matched():
    """The matched list must now include a -REJECT:<term> entry so ops can
    see which negative_auto_reject keyword vetoed the job."""
    r = kf.classify(job("Call Center Agent with Active Listening Skills"))
    assert r["decision"] == "auto_reject"
    # At least one -REJECT:<term> entry, naming the negative that fired
    reject_entries = [m for m in r["matched"] if m.startswith("-REJECT:")]
    assert reject_entries, f"no -REJECT:<term> in matched: {r['matched']}"
    # The triggering term should mention "active listening"
    assert any("active listening" in m.lower() for m in reject_entries)


def test_auto_reject_from_description_records_negative():
    r = kf.classify(job("Customer Service Rep",
                        description="Requires active listening skills."))
    assert r["decision"] == "auto_reject"
    assert any(m.startswith("-REJECT:") for m in r["matched"])


# ───── R8-shadow-B1: synonym dedup in cross_field_dedup ─────

def test_culture_amp_synonym_pair_scores_once():
    """Before the fix, a description mentioning both 'Culture Amp' and
    'CultureAmp' (common boilerplate) scored 30+30 — doubling a single
    semantic hit. After the fix, the two synonyms collapse to one hit at 30.

    Uses Acme Engineering (NOT a boost-list company) so the B3 vendor cap
    doesn't fire and mask the dedup result we're testing."""
    r = kf.classify(job(
        "Random Job",  # no title positives
        company="Acme Engineering",  # not in companies.yaml → no B3 cap
        description=(
            "We partner with Culture Amp and CultureAmp for our engagement strategy."
        ),
    ))
    # Expect a single T1 desc hit (30pts) for the Culture Amp synonym pair
    ca_hits = [m for m in r["matched"] if "culture amp" in m.lower() or "cultureamp" in m.lower()]
    assert len(ca_hits) == 1, f"synonym pair not deduped: {r['matched']}"
    assert r["score"] == 30


# ───── R8-shadow-B2: company self-mention suppression ─────

def test_self_mention_of_culture_amp_at_culture_amp_suppressed():
    """A Culture Amp posting with 'About Culture Amp' boilerplate should NOT
    score tier1_description points for its own company name."""
    r = kf.classify(job(
        "Allbound SDR",
        company="Culture Amp",
        description="About Culture Amp: we build employee experience software.",
    ))
    # The Culture Amp hit is suppressed. "employee experience" in desc is a
    # T3 (B8-gated) term in title only, so no points from desc. Net: 0.
    # Only way this scores is via some other signal — and there isn't one.
    assert r["score"] == 0, f"self-mention not suppressed: score={r['score']} matched={r['matched']}"


def test_self_mention_only_suppresses_exact_company_name():
    """A posting mentioning 'Microsoft Viva Glint' (product name, not company
    name) still scores — the tier1_description hit survives B2 because the
    product name isn't equal to the company name.

    Uses a non-boost company for the test so B3 vendor cap doesn't fire.
    The point being verified is B2's narrowness, not B3."""
    r = kf.classify(job(
        "Random Role",
        company="Acme Engineering",  # non-vendor, no B3 cap
        description="Experience with Microsoft Viva Glint required.",
    ))
    # "Microsoft Viva Glint" is a tier1_description term (score 30). Not
    # suppressed because it doesn't match the company name.
    assert r["score"] == 30


def test_self_mention_handles_company_suffix_stripping():
    """Normalized compare: 'Culture Amp Inc' → 'culture amp'; 'Culture Amp'
    term → 'culture amp'. They match after suffix strip — still suppressed.

    Description uses no other scoring terms so the suppression result is
    visible as score=0."""
    r = kf.classify(job(
        "Executive Assistant",
        company="Culture Amp Inc",
        description="About Culture Amp. Handle calendars and travel.",
    ))
    assert r["score"] == 0


def test_non_self_mention_still_scored():
    """A Perceptyx posting mentioning 'Culture Amp' scores — that's a
    genuine cross-vendor mention (competitor analysis, integration, etc.)."""
    r = kf.classify(job(
        "Random Role",
        company="Perceptyx",  # different vendor
        description="We compete with Culture Amp in the engagement space.",
    ))
    # Culture Amp hit is kept (not self-mention). Perceptyx mention in desc
    # would also be a self-mention — suppressed. Net: 30 from Culture Amp.
    # But then B3 vendor cap kicks in (Perceptyx is boost-list, no title
    # positive), capping to 14.
    assert r["score"] == 14
    assert any("VENDOR_CAP" in m for m in r["matched"])


# ───── R8-shadow-B3: vendor-boilerplate cap ─────

def test_vendor_boilerplate_capped_when_title_has_no_positive():
    """Culture Amp posting with 'people analytics' in desc but no title
    positive: B3 caps score below llm_review_min, preventing a wasted LLM
    call on what is almost certainly a vendor self-mention."""
    r = kf.classify(job(
        "Allbound SDR",  # no positive title keyword
        company="Culture Amp",
        description=(
            "About Culture Amp: we're the people analytics platform. "
            "Help us grow by qualifying leads in our engagement software."
        ),
    ))
    # Score before cap: people analytics desc (+30), Culture Amp self-mention
    # suppressed (B2). So 30. Then B3 fires: 30 → 14.
    assert r["score"] == 14
    assert r["decision"] == "low_score"
    assert any("VENDOR_CAP" in m for m in r["matched"])


def test_vendor_posting_with_title_positive_NOT_capped():
    """Legit PA role at a vendor — title has a positive → B3 doesn't fire."""
    r = kf.classify(job(
        "Principal People Scientist",  # tier1_title match (+50)
        company="Culture Amp",
        description="Lead research for Culture Amp's product.",
    ))
    # T1 title +50, company boost +10 (title_has_positive=True) = 60
    assert r["score"] >= 50
    assert r["decision"] == "auto_include"
    assert not any("VENDOR_CAP" in m for m in r["matched"])


def test_non_vendor_company_desc_only_NOT_capped():
    """Non-boost-list company — desc-only signals flow through to LLM review
    as before. B3 is scoped narrowly to vendor boilerplate."""
    r = kf.classify(job(
        "Random Analyst",  # no title positive
        company="Acme Engineering",  # not in companies.yaml
        description="Our team uses people analytics to drive decisions.",
    ))
    # T1 desc "people analytics" (+30). B3 doesn't apply (not a boost company).
    assert r["score"] == 30
    assert r["decision"] == "llm_review"


# ───────────── Conflict: positive + negative → LLM, never auto-decide ───────

def test_positive_plus_negative_routes_to_llm():
    """Listening bar + people analytics mention → conflict → LLM, not auto-include."""
    r = kf.classify(job(
        "People Analytics Director at our new Listening Bar",
        description="Drive people analytics strategy. Also help with active listening events.",
    ))
    assert r["decision"] == "llm_review", f"score={r['score']}, matched={r['matched']}"


def test_word_boundary_avoids_substring_false_positives():
    """'I-O psychologist' must not fire on 'psychology' alone? Actually those are separate terms.
    Test: 'organizational psychologist' does NOT substring-match 'organisational psychologist' (different spelling)."""
    r = kf.classify(job("Software Engineer at psychologyapp"))
    assert r["score"] < 10


def test_hyphenated_term_matches_hyphen():
    """'industrial-organizational' in title should match."""
    r = kf.classify(job("Industrial-Organizational Psychologist"))
    assert r["score"] >= 10  # at minimum tier2


# ───────────── Company boost (Tier 1/2 vendor adds +15) ─────────────────────

def test_company_boost_tier1_vendor():
    """Positive-keyword role at Perceptyx gets +10 (B3: boost gated on positive match)."""
    r = kf.classify(job("People Analytics Manager", company="Perceptyx"))
    # T1 title (50) + boost (10) = 60
    assert r["score"] >= 60
    assert r["decision"] in ("llm_review", "auto_include")


def test_company_boost_handles_corp_suffix():
    """'Perceptyx Inc' still matches Perceptyx entry and applies boost."""
    r = kf.classify(job("People Analytics Manager", company="Perceptyx Inc"))
    assert r["score"] >= 60


def test_company_boost_requires_positive_keyword():
    """Phase B3: 'Senior Manager' at Perceptyx gets NO boost (no positive match)."""
    r = kf.classify(job("Senior Manager", company="Perceptyx"))
    assert r["score"] == 0
    assert r["decision"] == "low_score"


def test_no_boost_for_unknown_company():
    r = kf.classify(job("Senior Manager", company="Random Startup LLC"))
    assert r["score"] < 10


# ───── R-audit Issue 1: boost requires TITLE-level positive match ─────────
# Shadow log 2026-04-18 showed 45 jobs at Culture Amp / Qualtrics routed to
# LLM review with only the company boost contributing (title unrelated, desc
# matched the vendor self-mention). Tightening the gate to title_has_positive
# keeps the boost aligned with role relevance.

def test_issue1_account_executive_at_cultureamp_no_boost():
    """Non-PA title at a boost-listed vendor: boost must NOT apply."""
    r = kf.classify(job("Account Executive", company="Culture Amp"))
    assert r["score"] == 0
    assert r["decision"] == "low_score"
    assert not any("+:company:" in m for m in r["matched"])


def test_issue1_people_analytics_lead_at_cultureamp_gets_boost():
    """PA title + boost-listed vendor: T1 title (50) + boost (10) = 60."""
    r = kf.classify(job("People Analytics Lead", company="Culture Amp"))
    assert r["score"] == 60
    assert r["decision"] == "auto_include"
    assert any("+:company:Culture Amp" in m for m in r["matched"])


def test_issue1_employee_listening_manager_at_netflix_gets_boost():
    """EL title at Tier-1 employer: T1 title (50) + boost (10) = 60."""
    r = kf.classify(job("Employee Listening Manager", company="Netflix"))
    assert r["score"] == 60
    assert r["decision"] == "auto_include"
    assert any("+:company:Netflix" in m for m in r["matched"])


def test_issue1_desc_only_match_at_vendor_does_not_trigger_boost():
    """Vendor self-mention in description alone must not pull the company boost.
    This was the actual shadow-log pattern: SDR-style titles whose desc
    contained 'Culture Amp' boilerplate picked up +30 (T1 desc) and the guard
    flipped true because any_positive was True. Under the tighter title-only
    gate, no boost applies."""
    r = kf.classify(job("Allbound SDR", company="Culture Amp",
                        description="About Culture Amp — we build employee experience software."))
    # Desc still contributes T1 desc for "Culture Amp", but boost must NOT apply.
    assert not any("+:company:" in m for m in r["matched"])


# ───── R-audit Issue 2: previously-missed PA/EL titles ─────────────────────

def test_issue2_insights_analyst_employee_engagement_survey_analytics():
    """Bare 'employee engagement' gated by cosignal (insights/survey/analytics
    all present in the title) → T2 match (25), reaches LLM review."""
    r = kf.classify(job(
        "Insights Analyst - Employee Engagement & Survey Analytics",
        company="Courseific",
    ))
    assert r["score"] >= 15, f"score {r['score']}, matched {r['matched']}"
    assert r["decision"] == "llm_review"
    assert "employee engagement" in r["matched"]


def test_issue2_senior_workforce_planning_manager_americas():
    """Matches via B8 fallback: cosignal 'manager' passes, no negative cosignal
    present → 15 points (llm_review floor)."""
    r = kf.classify(job(
        "Senior Workforce Planning Manager - Americas",
        company="Arcadis",
    ))
    assert r["score"] >= 15, f"score {r['score']}, matched {r['matched']}"
    assert r["decision"] == "llm_review"
    assert "workforce planning" in r["matched"]


def test_issue2_workforce_planning_optimisation_manager():
    """No specific T2 term matches contiguously, but B8 fallback with expanded
    cosignal (manager / optimisation) yields 15 points — the llm_review floor."""
    r = kf.classify(job(
        "Workforce Planning Optimisation Manager",
        company="VetPartners",
    ))
    assert r["score"] >= 15, f"score {r['score']}, matched {r['matched']}"
    assert r["decision"] == "llm_review"
    assert "workforce planning" in r["matched"]


def test_issue2_workforce_planning_and_analytics_manager():
    """Specific T2 term 'workforce planning & analytics' matches (covers the
    shadow-log title that previously scored only 5)."""
    r = kf.classify(job(
        "Workforce Planning & Analytics Manager",
        company="Registers of Scotland",
    ))
    assert r["score"] >= 15, f"score {r['score']}, matched {r['matched']}"
    assert r["decision"] == "llm_review"


# ───── Guardrails: false positives must stay rejected ──────────────────────

def test_employee_engagement_coordinator_event_planning_still_rejected():
    """CLAUDE.md edge case: 'Employee Engagement Coordinator' at a hospitality
    company is event planning, not analytics. Gate must reject without the
    analytics/leadership cosignal."""
    r = kf.classify(job("Employee Engagement Coordinator", company="Marriott"))
    assert r["score"] == 0
    assert r["decision"] == "low_score"


def test_workforce_planning_scheduler_retail_still_rejected():
    """Retail/call-center workforce scheduling — no B8 cosignal matches."""
    r = kf.classify(job("Workforce Planning Scheduler", company="Random Retailer"))
    assert r["score"] == 0


def test_workforce_planning_manager_hospital_neg_cosignal_still_rejected():
    """R-audit guardrail: a 'Workforce Planning Manager' title with nurse-shift
    context in the description must still score 0. The expanded B8 positive
    cosignal (now including 'manager') would otherwise let hospital scheduling
    roles in — the negative cosignal check catches them."""
    r = kf.classify(job(
        "Workforce Planning Manager",
        description="Build nurse shift schedules and manage staffing ratios.",
    ))
    assert r["score"] == 0, f"score {r['score']}, matched {r['matched']}"
    assert r["decision"] == "low_score"  # no positive matches, no auto-reject negatives


def test_workforce_planning_call_center_neg_cosignal_still_rejected():
    r = kf.classify(job(
        "Senior Workforce Planning Manager",
        description="Forecast agent volume for our contact centre staffing.",
    ))
    assert r["score"] == 0


# ───────────── google_alerts: always LLM ────────────────────────────────────

def test_google_alerts_high_score_still_llm():
    r = kf.classify(job("People Analytics Manager", company="Netflix", source_name="google_alerts"))
    assert r["decision"] == "llm_review"


def test_google_alerts_low_score_still_llm():
    r = kf.classify(job("Software Engineer", company="X", source_name="google_alerts"))
    # Low score from google_alerts goes to LLM, not silent reject
    assert r["decision"] == "llm_review"


# ───────────── Score cap and fit_score alias ────────────────────────────────

def test_score_capped_at_100():
    """Many matches shouldn't exceed 100."""
    r = kf.classify(job(
        "Employee Listening Manager People Analytics Director Voice of Employee Workforce Sensing Lead",
        description="employee listening people analytics voice of employee workforce sensing continuous listening",
    ))
    assert r["score"] <= 100


def test_fit_score_mirrors_keyword_score():
    j = job("People Analytics Manager")
    kf.classify(j)
    assert j["fit_score"] == j["keyword_score"]


def test_description_snippet_populated():
    desc = "x" * 500
    j = job("People Analytics Manager", description=desc)
    kf.classify(j)
    assert len(j["description_snippet"]) == 300


def test_job_mutation_adds_keyword_fields():
    j = job("People Analytics Manager")
    kf.classify(j)
    assert "keyword_score" in j
    assert "keywords_matched" in j
    assert "fit_score" in j


# ───────────── HRIS admin in reducer list (not auto-reject, just penalized) ─

def test_hris_admin_penalty():
    """HRIS admin is a hard reducer — no positives, score is 0 after reducer.
    Decision should be low_score (no negative_auto_reject matched)."""
    r = kf.classify(job("HRIS Analyst"))
    assert r["decision"] == "low_score"


# ───────────── Preprocessing tests (Phase A) ───────────────────────────────

def test_preprocess_html_entities():
    r = kf.classify(job("Employee Listening Manager &amp; Director",
                        description="People analytics team&nbsp;mission"))
    assert r["score"] >= 50


def test_preprocess_smart_quotes():
    """Smart quotes should be normalized so matching still works."""
    r = kf.classify(job("\u201cPeople Analytics\u201d Manager"))
    assert r["score"] >= 50


def test_preprocess_non_breaking_hyphen():
    """Non-breaking hyphen U+2011 between words should normalize to ASCII hyphen."""
    r = kf.classify(job("Industrial\u2011Organizational Psychologist"))
    assert r["score"] >= 10


def test_preprocess_html_tags_stripped():
    r = kf.classify(job("<b>People Analytics</b> Manager",
                        description="<p>Lead our <span>employee listening</span> team</p>"))
    assert r["score"] >= 50


def test_preprocess_possessive_stripped():
    """Possessive 's should be stripped so 'Google's people analytics team' still matches.
    (This test asserts possessives don't block matching downstream terms.)"""
    r = kf.classify(job("People Analytics Manager",
                        description="Join Netflix's people analytics team"))
    assert r["score"] >= 50


def test_preprocess_em_dash():
    """Em-dash U+2014 should be normalized."""
    r = kf.classify(job("People Analytics \u2014 Senior Manager"))
    assert r["score"] >= 10


def test_preprocess_collapses_whitespace():
    r = kf.classify(job("People   Analytics\t\tManager"))
    assert r["score"] >= 50


# ───────────── Phase B: scoring recalibration ──────────────────────────────

def test_b1_t2_desc_cap_at_16():
    """Five T2 desc matches at 10 pts each would be 50 — must cap at 16."""
    desc = ("pulse survey. engagement survey. employee sentiment. "
            "eNPS program. turnover analytics across teams.")
    # Title has no positives, so score = 0 + capped T2 desc (16)
    r = kf.classify(job("Random Role", description=desc))
    assert r["score"] == 16


def test_b2_desc_only_50_routes_to_llm_review_not_auto_include():
    """Phase B2: score >= 50 from description alone must route to llm_review, not auto_include."""
    # Stack T1 desc (30) + T1 desc (30) via two distinct T1 desc terms
    desc = "We are a voice of employee analytics team using Workday Peakon."
    r = kf.classify(job("Random Role", description=desc))
    assert r["score"] >= 50
    assert r["decision"] == "llm_review"


def test_b3_company_boost_only_with_positive_match():
    # With positive match → boost applies
    r1 = kf.classify(job("People Analytics Manager", company="Culture Amp"))
    # Without positive match → no boost
    r2 = kf.classify(job("Senior Manager", company="Culture Amp"))
    assert r1["score"] >= 60  # 50 + 10
    assert r2["score"] == 0


def test_b4_ai_review_floor_is_15():
    """T3-alone (5-7 pts) with co-signal no longer reaches AI review."""
    # "people operations analyst" T3 (7) with co-signal in desc → 7 pts
    r = kf.classify(job("People Operations Analyst",
                        description="Work with analytics and data."))
    assert r["score"] == 7
    assert r["decision"] == "low_score"  # 7 < 15


def test_b5_t3_title_requires_cosignal():
    """T3 term without co-signal in description scores 0."""
    # No analytics/insights/etc. co-signal
    r = kf.classify(job("People Operations Analyst",
                        description="Help with onboarding paperwork."))
    assert r["score"] == 0


def test_b5_t3_title_with_cosignal():
    r = kf.classify(job("HR Reporting Manager",
                        description="Build dashboards with Python and SQL."))
    assert r["score"] == 7


def test_b6_hard_reducer_40():
    r = kf.classify(job("HRIS Analyst"))
    # -40 hard reducer, no positives
    assert r["score"] == -40
    assert r["decision"] == "low_score"


def test_b6_medium_reducer_25():
    r = kf.classify(job("Market Research Director"))
    assert r["score"] == -25


def test_b6_standard_reducer_20():
    r = kf.classify(job("Digital Marketing Specialist"))
    assert r["score"] == -20


def test_b6_cmc_gate_with_cotern_skips_penalty():
    """'Change management consultant' with listening/analytics co-term: no penalty applied."""
    r = kf.classify(job("Change Management Consultant",
                        description="Lead employee listening and analytics programs."))
    # No penalty applied (gate satisfied). T1 desc "employee listening" → 30 pts.
    assert r["score"] >= 30


def test_b6_cmc_without_coterm_gets_penalty():
    r = kf.classify(job("Change Management Consultant"))
    assert r["score"] == -25


def test_b7_continuous_listening_bare_is_t2():
    """'Continuous Listening Manager' alone is T2 (25), not T1 (50)."""
    r = kf.classify(job("Continuous Listening Manager"))
    assert r["score"] == 25
    assert r["decision"] == "llm_review"


def test_b7_continuous_listening_with_employee_is_t1():
    """Proximity bumps score to 50."""
    r = kf.classify(job("Continuous Employee Listening Manager"))
    # Also matches "employee listening manager" in T1 — but continuous listening bump also fires.
    # Either way score >= 50 and auto_include (title has positive).
    assert r["score"] >= 50
    assert r["decision"] == "auto_include"


def test_b7_continuous_listening_with_workforce_is_t1():
    r = kf.classify(job("Workforce Continuous Listening Lead"))
    assert r["score"] >= 50


def test_b8_employee_experience_requires_cosignal():
    """'Employee Experience Manager' alone: no co-signal → 0 pts."""
    r = kf.classify(job("Employee Experience Manager",
                        description="Plan company events and swag."))
    assert r["score"] == 0


def test_b8_employee_experience_with_cosignal_scores_5():
    r = kf.classify(job("Employee Experience Manager",
                        description="Run pulse surveys and analytics."))
    assert r["score"] == 5
    assert r["decision"] == "low_score"  # 5 < 15


def test_b8_workforce_planning_requires_cosignal():
    r = kf.classify(job("Workforce Planning Manager",
                        description="Manage nurse shift schedules."))
    assert r["score"] == 0


def test_b8_workforce_planning_with_cosignal_scores_5():
    r = kf.classify(job("Workforce Planning Analyst",
                        description="People analytics and data-driven insights."))
    # "workforce planning" hits B8 (5) since co-signal in scope
    assert r["score"] >= 5


# ───────────── Phase C: keyword gating ─────────────────────────────────────

def test_c1_bare_ona_no_longer_matches():
    """Bare 'ONA' in desc should not trigger match."""
    r = kf.classify(job("Generic Role", description="ONA is our favorite acronym."))
    # No "organizational network analysis", so no positive match
    assert r["score"] == 0


def test_c1_organizational_network_analysis_matches():
    r = kf.classify(job("Team Lead",
                        description="We use organizational network analysis for ONA projects."))
    # T1 desc: "organizational network analysis" = 30
    assert r["score"] == 30


def test_c1_british_spelling_matches():
    r = kf.classify(job("Team Lead",
                        description="We run organisational network analysis quarterly."))
    assert r["score"] >= 30


def test_c2_bare_glint_no_longer_matches():
    r = kf.classify(job("Random Role", description="Glint was her nickname in school."))
    assert r["score"] == 0


def test_c2_viva_glint_matches():
    r = kf.classify(job("Random Role",
                        description="We administer Viva Glint surveys."))
    assert r["score"] == 30


def test_c2_microsoft_viva_glint_matches():
    r = kf.classify(job("Random Role",
                        description="Microsoft Viva Glint is our platform."))
    assert r["score"] >= 30


def test_c3_bare_medallia_no_longer_matches():
    r = kf.classify(job("Random Role",
                        description="Medallia has offices worldwide."))
    # Should not match bare Medallia
    assert r["score"] == 0


def test_c3_medallia_ex_matches():
    r = kf.classify(job("Random Role",
                        description="We use Medallia EX for engagement surveys."))
    assert r["score"] >= 30


def test_c3_medallia_employee_matches():
    r = kf.classify(job("Random Role",
                        description="Our Medallia employee listening program."))
    assert r["score"] >= 30


def test_c3_qualtrics_employee_experience_matches():
    r = kf.classify(job("Random Role",
                        description="We run Qualtrics Employee Experience studies."))
    assert r["score"] >= 30


def test_c4_cultureamp_alias_matches():
    r = kf.classify(job("Random Role",
                        description="CultureAmp is our engagement platform."))
    assert r["score"] >= 30


def test_c4_culture_amp_matches():
    r = kf.classify(job("Random Role",
                        description="We use Culture Amp for pulse surveys."))
    assert r["score"] >= 30


def test_c5_xm_scientist_without_coterm_no_score():
    r = kf.classify(job("XM Scientist",
                        description="Drive customer experience research across channels."))
    # No employee/EX/EE co-term → no score from XM scientist
    assert r["score"] == 0


def test_c5_xm_scientist_with_employee_coterm_scores_t2():
    r = kf.classify(job("XM Scientist, Employee Experience",
                        description="Lead employee experience measurement."))
    # XM scientist T2 title = 25 (plus possibly employee experience T3 — but EE is T3 with co-signal gate)
    assert r["score"] >= 25


def test_c5_xm_scientist_with_desc_coterm_scores():
    r = kf.classify(job("XM Scientist",
                        description="Our employee listening team needs a researcher."))
    # Co-term 'employee' in desc triggers gate; XM scientist T2 (25)
    # Plus T1 desc "employee listening" (30); cross-field no overlap → 55
    assert r["score"] >= 25


def test_c6_cross_field_dedup_same_term():
    """Phase C6: 'people analytics' in both title and desc counted once at higher score."""
    r = kf.classify(job("People Analytics Manager",
                        description="Lead people analytics initiatives."))
    # Without dedup: 50 (t1_title "people analytics manager") + 50 (t1_title "people analytics")
    #   + 30 (t1_desc "people analytics") = 130 (capped 100)
    # With dedup:
    #   maximal-munch in title: "people analytics manager" (50) covers "people analytics" (50) → keep 50
    #   cross-field: "people analytics" desc (30) is a DIFFERENT term from "people analytics manager",
    #                so both survive → 50 + 30 = 80
    assert r["score"] == 80


def test_c6_cross_field_dedup_exact_same_term():
    """Same exact term in title and desc → keep only highest."""
    r = kf.classify(job("People Analytics",
                        description="people analytics is our focus area."))
    # Title T1 "people analytics" (50). Desc T1 "people analytics" (30). Dedup → 50.
    assert r["score"] == 50


def test_c7_maximal_munch_title():
    """'People Analytics Manager' matches both 'people analytics manager' and 'people analytics' in title.
    Maximal-munch keeps only the longer span."""
    r = kf.classify(job("People Analytics Manager"))
    # Only 50, not 100
    assert r["score"] == 50


def test_c7_maximal_munch_with_director():
    r = kf.classify(job("People Analytics Director"))
    assert r["score"] == 50


# ───────────── Phase D: positive keyword additions ────────────────────────

@pytest.mark.parametrize("title", [
    "Head of People Analytics",
    "VP People Analytics",
    "Chief People Analytics Officer",
    "Global Head of People Analytics",
    "Head of People Insights",
    "Principal People Scientist",
    "Senior People Scientist",
    "Staff People Scientist",
    "Lead People Scientist",
    "People Research Scientist",
    "Employee Listening Program Manager",
    "Continuous Listening Program Manager",
    "Listening Strategy Lead",
    "Listening Architect",
    "Pay Equity Analyst",
    "Workplace Equity Analyst",
    "Strategic Workforce Planning Manager",
    "Talent Intelligence Analyst",
    "Talent Intelligence Lead",
])
def test_d1_t1_title_additions(title):
    r = kf.classify(job(title))
    assert r["score"] >= 50
    assert r["decision"] == "auto_include"


def test_d1_research_scientist_people_with_comma():
    r = kf.classify(job("Research Scientist, People Insights Team"))
    assert r["score"] >= 50


def test_d1_research_scientist_hr_with_comma():
    r = kf.classify(job("Research Scientist, HR"))
    assert r["score"] >= 50


def test_d1_decision_scientist_people():
    r = kf.classify(job("Decision Scientist, People"))
    assert r["score"] >= 50


def test_d2_behavioral_scientist_gate_without_coterm():
    r = kf.classify(job("Behavioral Scientist",
                        description="Design consumer experiments."))
    # No HR/people/employee/workforce co-term → gated out
    assert r["score"] == 0


def test_d2_behavioral_scientist_gate_with_coterm():
    r = kf.classify(job("Behavioral Scientist, People Team",
                        description="Employee experience research."))
    assert r["score"] >= 25


def test_d2_survey_methodologist_matches():
    r = kf.classify(job("Survey Methodologist"))
    assert r["score"] >= 25


def test_d2_labor_economist_gate_fails_without_context():
    r = kf.classify(job("Labor Economist",
                        description="Academic research on wage trends."))
    assert r["score"] == 0


def test_d2_labor_economist_gate_passes_with_context():
    r = kf.classify(job("Labor Economist",
                        description="Join our HR analytics team to model workforce trends."))
    assert r["score"] >= 25


def test_d2_partner_experience_analyst_gate_starbucks():
    r = kf.classify(job("Partner Experience Analyst", company="Starbucks",
                        description="Starbucks partner engagement analytics."))
    assert r["score"] >= 25


def test_d2_partner_experience_analyst_gate_fails_generic():
    r = kf.classify(job("Partner Experience Analyst",
                        description="Channel partner program analytics."))
    assert r["score"] == 0


def test_d2_colleague_experience_manager_matches():
    r = kf.classify(job("Colleague Experience Manager"))
    assert r["score"] >= 25


@pytest.mark.parametrize("desc_term", [
    "Workday Peakon Employee Voice",
    "Viva Pulse",
    "Viva Insights",
    "Perceptyx Ask",
    "Perceptyx Listen",
    "Perceptyx Activate",
    "Achievers Listen",
    "WorkTango",
    "Energage",
    "Workhuman",
    "Quantum Workplace",
    "DecisionWise",
    "Gallup Q12",
    "Workvivo",
    "Effectory",
])
def test_d3_t1_desc_vendor_additions(desc_term):
    r = kf.classify(job("Random Role", description=f"We use {desc_term} daily."))
    assert r["score"] >= 30


@pytest.mark.parametrize("desc_term", [
    "driver analysis",
    "linkage research",
    "always-on listening",
    "passive listening",
    "feedback intelligence",
    "conversational surveys",
    "moments that matter",
    "pay equity",
    "people intelligence",
    "manager effectiveness",
    "Syndio",
    "Trusaic",
    "Lightcast",
    "Eightfold",
    "Humanyze",
    "Polinode",
])
def test_d4_t2_desc_additions(desc_term):
    r = kf.classify(job("Random Role", description=f"Our team uses {desc_term}."))
    # One T2 desc match = 10 points
    assert r["score"] == 10


# ───────────── Phase E: negative keyword additions ────────────────────────

@pytest.mark.parametrize("title", [
    "Voice of the Patient Program Manager",
    "Voice of the Citizen Lead",
    "Voice of the Veteran Coordinator",
    "Patient Experience Analyst",
    "Patient Experience Director",
    "CX Analyst",
    "CX Manager",
    "CX Strategy Lead",
    "Clinical Psychologist",
    "Counseling Psychologist",
    "Forensic Psychologist",
    "School Psychologist",
    "Neuropsychologist",
    "Psychometrist",
    "Speech-Language Pathologist",
    "UX Researcher",
    "Event Coordinator",
    "Hospitality Coordinator",
    "Customer Service Manager",
    "Revenue Operations Analyst",
])
def test_e1_auto_reject_titles(title):
    r = kf.classify(job(title))
    assert r["decision"] == "auto_reject"
    assert r["score"] == -100


def test_e1_sirius_xm_company_rejected():
    r = kf.classify(job("XM Scientist, Employee Experience",
                        description="Radio broadcast strategy at Sirius XM."))
    # Sirius XM in desc → auto-reject even with XM scientist match
    # Note: positive match present, so goes to llm_review (conflict)
    assert r["decision"] == "llm_review"


def test_e2_hard_reducer_40_successfactors_administrator():
    r = kf.classify(job("SuccessFactors Administrator"))
    assert r["score"] == -40


def test_e2_hard_reducer_40_assessment_scientist():
    r = kf.classify(job("Assessment Scientist"))
    assert r["score"] == -40


def test_e2_hard_reducer_40_selection_scientist():
    r = kf.classify(job("Selection Scientist"))
    assert r["score"] == -40


@pytest.mark.parametrize("title,expected", [
    ("Instructional Designer", -20),
    ("Curriculum Developer", -20),
    ("Training Facilitator", -20),
    ("LMS Administrator", -20),
    ("Internal Communications Manager", -20),
    ("Newsletter Manager", -20),
    ("Compensation Consultant", -20),
    ("Talent Acquisition Consultant", -20),
])
def test_e2_standard_reducer_20(title, expected):
    r = kf.classify(job(title))
    assert r["score"] == expected


# ───────────── Phase G: canonical cases + integration ─────────────────────

def test_g_staff_accountant_at_perceptyx_not_boosted():
    """Canonical B3 case from the taxonomy audit: generic-role at EL vendor
    should NOT reach AI review without a positive keyword."""
    r = kf.classify(job("Staff Accountant", company="Perceptyx"))
    assert r["score"] == 0
    assert r["decision"] == "low_score"


def test_g_people_analytics_mgr_at_perceptyx_boosted():
    """Positive-kw role at EL vendor: T1 title (50) + boost (10) = 60, auto_include."""
    r = kf.classify(job("People Analytics Manager", company="Perceptyx"))
    assert r["score"] == 60
    assert r["decision"] == "auto_include"


def test_g_hospital_wfp_nurse_scheduler_not_matched():
    """Phase B8 kills hospital workforce planning false positive."""
    r = kf.classify(job("Workforce Planning Manager",
                        description="Build nurse shift schedules and manage staffing ratios."))
    assert r["score"] == 0
    assert r["decision"] == "low_score"


def test_g_realistic_employee_listening_job_auto_includes():
    r = kf.classify(job(
        "Senior Manager, Employee Listening",
        company="Netflix",
        description=(
            "Build our employee listening program. You'll design pulse surveys, analyze sentiment, "
            "run driver analysis, and partner with people analytics to drive action planning."
        ),
    ))
    assert r["decision"] == "auto_include"
    assert r["score"] >= 50


def test_g_summary_scoring_row_counts():
    """Sanity: key rows of the scoring summary table produce expected scores."""
    # T1 Title alone
    assert kf.classify(job("People Analytics Manager"))["score"] == 50
    # T2 Title alone
    assert kf.classify(job("Employee Engagement Manager"))["score"] == 25
    # T3 Title alone (with co-signal) scores 7
    assert kf.classify(job("HR Reporting Analyst",
                           description="analytics dashboards in Python"))["score"] == 7
    # Hard reducer
    assert kf.classify(job("HRIS Analyst"))["score"] == -40
    # Medium reducer
    assert kf.classify(job("Market Research Lead"))["score"] == -25
    # Standard reducer
    assert kf.classify(job("Digital Marketing Specialist"))["score"] == -20
