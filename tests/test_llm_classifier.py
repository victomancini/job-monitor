"""Tests for llm_classifier — mocks each provider. No real API calls."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.processors import llm_classifier as lc


def job(title="People Analytics Manager", company="Netflix"):
    return {
        "title": title,
        "company": company,
        "description": "Lead people analytics team",
        "location": "Los Gatos, CA",
        "keyword_score": 50,
    }


# ──────────────────────────── JSON parsing ──────────────────────────

def test_parse_json_clean():
    r = lc._parse_json('{"classification": "RELEVANT", "confidence": 90, "reasoning": "core EL role"}')
    assert r["classification"] == "RELEVANT"
    assert r["confidence"] == 90


def test_parse_json_strips_markdown():
    r = lc._parse_json('```json\n{"classification": "RELEVANT", "confidence": 85, "reasoning": "x"}\n```')
    assert r["classification"] == "RELEVANT"


def test_parse_json_rejects_invalid_classification():
    assert lc._parse_json('{"classification": "MAYBE", "confidence": 50, "reasoning": "x"}') is None


def test_parse_json_rejects_non_json():
    assert lc._parse_json("not json at all") is None


def test_parse_json_clamps_confidence():
    r = lc._parse_json('{"classification": "RELEVANT", "confidence": 150, "reasoning": "x"}')
    assert r["confidence"] == 100


def test_parse_json_confidence_non_int_rejected():
    assert lc._parse_json('{"classification": "RELEVANT", "confidence": "high", "reasoning": "x"}') is None


def test_parse_json_extracts_seniority():
    r = lc._parse_json(
        '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x", "seniority": "Senior Manager"}'
    )
    assert r["seniority"] == "Senior Manager"


def test_parse_json_seniority_absent_is_none():
    r = lc._parse_json('{"classification": "RELEVANT", "confidence": 90, "reasoning": "x"}')
    assert r["seniority"] is None


def test_parse_json_seniority_invalid_value_is_none():
    r = lc._parse_json(
        '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x", "seniority": "Emperor"}'
    )
    assert r["seniority"] is None


def test_classify_job_sets_llm_seniority_hint():
    j = job()
    payload = '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x", "seniority": "Director"}'
    with patch("openai.OpenAI", return_value=_mock_groq_result(payload)):
        lc.classify_job(j, groq_key="g", gemini_key="", openai_key="")
    assert j.get("_llm_seniority") == "Director"


def test_classify_job_skips_llm_seniority_hint_when_absent():
    j = job()
    payload = '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x"}'
    with patch("openai.OpenAI", return_value=_mock_groq_result(payload)):
        lc.classify_job(j, groq_key="g", gemini_key="", openai_key="")
    assert "_llm_seniority" not in j


# ───────────── Phase J (R2): remote_status + salary_hint ───────────

def test_parse_json_extracts_remote_status_and_salary_hint():
    r = lc._parse_json(
        '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x",'
        ' "remote_status": "remote", "salary_hint": "$120K-$180K"}'
    )
    assert r["remote_status"] == "remote"
    assert r["salary_hint"] == "$120K-$180K"


def test_parse_json_remote_status_normalizes_dashes():
    r = lc._parse_json(
        '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x",'
        ' "remote_status": "on-site"}'
    )
    assert r["remote_status"] == "onsite"


def test_parse_json_remote_status_invalid_is_none():
    r = lc._parse_json(
        '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x",'
        ' "remote_status": "maybe"}'
    )
    assert r["remote_status"] is None


# ───────────── Per-provider delay (M7) ────────────────────

def test_classify_batch_uses_groq_delay_when_groq_serves():
    groq_client = MagicMock()
    groq_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(
            content='{"classification":"RELEVANT","confidence":90,"reasoning":"x"}'))]
    )
    jobs = [job(title=f"j{i}") for i in range(3)]
    with patch("openai.OpenAI", return_value=groq_client), \
         patch("src.processors.llm_classifier.time.sleep") as ms:
        lc.classify_batch(jobs, groq_key="g", gemini_key="", openai_key="")
    # 3 jobs → 2 delays; should use Groq's 2.5s
    assert ms.call_count == 2
    for call in ms.call_args_list:
        assert call.args[0] == lc.GROQ_CALL_DELAY_SEC


def test_classify_batch_uses_gemini_delay_when_gemini_serves():
    """When Groq fails and Gemini takes over, subsequent sleeps use 4s."""
    from google import genai  # noqa: F401
    # Groq always fails; Gemini always succeeds
    groq_client = MagicMock()
    groq_client.chat.completions.create.side_effect = Exception("429 groq down")
    gemini_client = MagicMock()
    gemini_client.models.generate_content.return_value = MagicMock(
        text='{"classification":"RELEVANT","confidence":80,"reasoning":"x"}'
    )

    jobs = [job(title=f"j{i}") for i in range(3)]
    with patch("openai.OpenAI", return_value=groq_client), \
         patch("google.genai.Client", return_value=gemini_client), \
         patch("src.processors.llm_classifier.time.sleep") as ms:
        errors, counts = lc.classify_batch(jobs, groq_key="g", gemini_key="gm", openai_key="")
    assert counts.get("gemini") == 3
    # All inter-call sleeps should be the Gemini delay, not the Groq one
    assert ms.call_count == 2
    for call in ms.call_args_list:
        assert call.args[0] == lc.GEMINI_CALL_DELAY_SEC


def test_classify_batch_explicit_delay_overrides_per_provider():
    groq_client = MagicMock()
    groq_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(
            content='{"classification":"RELEVANT","confidence":90,"reasoning":"x"}'))]
    )
    jobs = [job(title=f"j{i}") for i in range(2)]
    with patch("openai.OpenAI", return_value=groq_client), \
         patch("src.processors.llm_classifier.time.sleep") as ms:
        lc.classify_batch(jobs, groq_key="g", gemini_key="", openai_key="", delay=0.5)
    assert ms.call_args_list == [((0.5,),)]


def test_classify_job_stashes_llm_remote_and_salary_hint():
    j = job()
    payload = (
        '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x",'
        ' "remote_status": "hybrid", "salary_hint": "$150K-$200K"}'
    )
    with patch("openai.OpenAI", return_value=_mock_groq_result(payload)):
        lc.classify_job(j, groq_key="g", gemini_key="", openai_key="")
    assert j["_llm_remote"] == "hybrid"
    assert j["_llm_salary_hint"] == "$150K-$200K"


# ──────────────────────────── 4-tier fallback chain ─────────────────

def _mock_groq_result(text):
    client = MagicMock()
    client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content=text))]
    )
    return client


def test_groq_success_first_call():
    j = job()
    ok = '{"classification": "RELEVANT", "confidence": 90, "reasoning": "core role"}'
    with patch("openai.OpenAI", return_value=_mock_groq_result(ok)):
        r = lc.classify_job(j, groq_key="g", gemini_key="", openai_key="")
    assert r["provider"] == "groq"
    assert j["llm_classification"] == "RELEVANT"
    assert j["llm_provider"] == "groq"
    assert j["llm_confidence"] == 90


def test_groq_429_falls_through_to_gemini():
    j = job()
    from google import genai  # noqa: F401
    groq_client = MagicMock()
    groq_client.chat.completions.create.side_effect = Exception("429 rate limit")

    gemini_client = MagicMock()
    gemini_client.models.generate_content.return_value = MagicMock(
        text='{"classification": "RELEVANT", "confidence": 80, "reasoning": "x"}'
    )

    with patch("openai.OpenAI", return_value=groq_client), \
         patch("google.genai.Client", return_value=gemini_client):
        r = lc.classify_job(j, groq_key="g", gemini_key="gem", openai_key="")
    assert r["provider"] == "gemini"


def test_both_groq_and_gemini_fail_openai_wins():
    j = job()
    # Groq raises; Gemini returns non-JSON; OpenAI succeeds
    groq_client = MagicMock()
    groq_client.chat.completions.create.side_effect = Exception("groq down")
    openai_client = MagicMock()
    openai_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content='{"classification": "RELEVANT", "confidence": 75, "reasoning": "x"}'))]
    )
    # Patch openai.OpenAI to return groq then openai clients
    openai_returns = [groq_client, openai_client]

    gemini_client = MagicMock()
    gemini_client.models.generate_content.return_value = MagicMock(text="not json")

    with patch("openai.OpenAI", side_effect=openai_returns), \
         patch("google.genai.Client", return_value=gemini_client):
        r = lc.classify_job(j, groq_key="g", gemini_key="gem", openai_key="o")
    assert r["provider"] == "openai"
    assert j["llm_classification"] == "RELEVANT"


def test_all_three_fail_keyword_fallback():
    j = job(title="Customer Experience Analyst")
    j["keyword_score"] = 0

    groq_client = MagicMock()
    groq_client.chat.completions.create.side_effect = Exception("groq down")
    openai_client = MagicMock()
    openai_client.chat.completions.create.side_effect = Exception("openai down")
    gemini_client = MagicMock()
    gemini_client.models.generate_content.side_effect = Exception("gemini down")

    with patch("openai.OpenAI", side_effect=[groq_client, openai_client]), \
         patch("google.genai.Client", return_value=gemini_client):
        r = lc.classify_job(j, groq_key="g", gemini_key="gem", openai_key="o")
    assert r["provider"] == "keyword_only"
    assert j["llm_classification"] == "NOT_RELEVANT"


def test_keyword_fallback_direct_high_score():
    j = job()
    j["keyword_score"] = 70
    r = lc._keyword_fallback(j)
    assert r["classification"] == "RELEVANT"


def test_keyword_fallback_direct_partial():
    j = job()
    j["keyword_score"] = 30
    r = lc._keyword_fallback(j)
    assert r["classification"] == "PARTIALLY_RELEVANT"


def test_no_api_keys_configured_uses_keyword_fallback():
    j = job()
    j["keyword_score"] = 80
    r = lc.classify_job(j, groq_key="", gemini_key="", openai_key="")
    assert r["provider"] == "keyword_only"
    assert j["llm_classification"] == "RELEVANT"


def test_snippet_haircut_reduces_confidence():
    j = job()
    j["description_is_snippet"] = True
    ok = '{"classification": "RELEVANT", "confidence": 90, "reasoning": "x"}'
    with patch("openai.OpenAI", return_value=_mock_groq_result(ok)):
        lc.classify_job(j, groq_key="g", gemini_key="", openai_key="")
    assert j["llm_confidence"] == 80  # 90 - 10 haircut


# ──────────────────────────── Publish routing ───────────────────────

def test_publish_decision_relevant_high_conf():
    assert lc.publish_decision({"llm_classification": "RELEVANT", "llm_confidence": 90}) == "publish"


def test_publish_decision_partial_flag():
    assert lc.publish_decision({"llm_classification": "PARTIALLY_RELEVANT", "llm_confidence": 55}) == "publish_flag"


def test_publish_decision_partial_high_publishes():
    assert lc.publish_decision({"llm_classification": "PARTIALLY_RELEVANT", "llm_confidence": 80}) == "publish"


def test_publish_decision_not_relevant_rejects():
    assert lc.publish_decision({"llm_classification": "NOT_RELEVANT", "llm_confidence": 95}) == "reject"


def test_publish_decision_low_conf_rejects():
    assert lc.publish_decision({"llm_classification": "RELEVANT", "llm_confidence": 50}) == "reject"


# Regression: C1 — keyword-only fallback must emit conf values that survive
# publish_decision. Previously RELEVANT/conf=60 was silently rejected (60 < 70).
def test_keyword_fallback_high_score_reaches_publish():
    j = job()
    j["keyword_score"] = 80
    r = lc._keyword_fallback(j)
    j["llm_classification"] = r["classification"]
    j["llm_confidence"] = r["confidence"]
    assert lc.publish_decision(j) == "publish"


def test_keyword_fallback_partial_reaches_publish_flag():
    j = job()
    j["keyword_score"] = 30
    r = lc._keyword_fallback(j)
    j["llm_classification"] = r["classification"]
    j["llm_confidence"] = r["confidence"]
    assert lc.publish_decision(j) == "publish_flag"


def test_keyword_fallback_low_score_rejects():
    j = job()
    j["keyword_score"] = 10
    r = lc._keyword_fallback(j)
    j["llm_classification"] = r["classification"]
    j["llm_confidence"] = r["confidence"]
    assert lc.publish_decision(j) == "reject"


# Regression: IMP7 — after a keyword_only fall-through, the next job's delay
# should still use the previous network provider's cadence, not Groq's 0s.
def test_classify_batch_keyword_only_preserves_prior_delay():
    # First job: Groq succeeds. Second job: every provider raises → keyword_only.
    # Inter-call delay should stay at Groq's 2.5s, not reset to 0.
    groq_client = MagicMock()
    ok_then_fail = [
        MagicMock(choices=[MagicMock(message=MagicMock(
            content='{"classification":"RELEVANT","confidence":90,"reasoning":"x"}'))]),
        Exception("groq down"),
        Exception("groq down"),
    ]
    groq_client.chat.completions.create.side_effect = ok_then_fail

    jobs = [job(title="j0"), job(title="j1"), job(title="j2")]
    for j in jobs:
        j["keyword_score"] = 80
    with patch("openai.OpenAI", return_value=groq_client), \
         patch("src.processors.llm_classifier.time.sleep") as ms:
        lc.classify_batch(jobs, groq_key="g", gemini_key="", openai_key="")
    # 3 jobs → 2 inter-call sleeps. Both should be Groq's cadence even though
    # jobs 1+2 fell through to keyword_only.
    assert ms.call_count == 2
    for call in ms.call_args_list:
        assert call.args[0] == lc.GROQ_CALL_DELAY_SEC


# ──────────────────────────── Batch API ─────────────────────────────

def test_classify_batch_counts_providers():
    jobs = [job(title=f"Job {i}") for i in range(3)]
    ok = '{"classification": "RELEVANT", "confidence": 80, "reasoning": "x"}'
    client = MagicMock()
    client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content=ok))]
    )
    with patch("openai.OpenAI", return_value=client), patch("src.processors.llm_classifier.time.sleep"):
        errors, counts = lc.classify_batch(jobs, groq_key="g", gemini_key="", openai_key="", delay=0)
    assert errors == []
    assert counts == {"groq": 3}
