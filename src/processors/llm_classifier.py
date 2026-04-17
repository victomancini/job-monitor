"""Four-tier LLM classifier: Groq → Gemini → GPT-4o-mini → keyword-only fallback.

Each provider uses the same JSON-output prompt. On failure (error, non-JSON, bad schema),
we fall through to the next tier. Final fallback uses keyword_score to decide.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from src.shared import env

log = logging.getLogger(__name__)

GROQ_MODEL = "llama-3.3-70b-versatile"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GEMINI_MODEL = "gemini-2.5-flash-lite"
OPENAI_MODEL = "gpt-4o-mini"

GROQ_CALL_DELAY_SEC = 2.5
GEMINI_CALL_DELAY_SEC = 4.0  # 15 RPM → 4s between calls
OPENAI_CALL_DELAY_SEC = 1.0

_VALID_CLASSIFICATIONS = {"RELEVANT", "PARTIALLY_RELEVANT", "NOT_RELEVANT"}

PROMPT_TEMPLATE = """You are classifying job postings for the employee listening and people analytics field. US job market focus.

WHAT THIS FIELD IS:
Employee listening = systematic collection and analysis of employee feedback through surveys (engagement, pulse, lifecycle, 360), text analytics on open-ended responses, organizational network analysis, or continuous listening platforms. People analytics = using quantitative employee data to improve HR decisions: workforce planning, attrition modeling, engagement measurement, talent analytics, human capital analytics.

KNOWN VENDORS IN THIS SPACE (roles at these companies touching employee data are likely RELEVANT):
Perceptyx, Qualtrics (EmployeeXM/EX), Culture Amp, Workday (Peakon), Microsoft (Viva Glint, Viva Pulse, Viva Insights), Medallia (employee side), Gallup (workplace division), Lattice, 15Five, Leapsome, Betterworks, Quantum Workplace, DecisionWise, Workhuman, Achievers, WorkTango, Workvivo, Energage, Effectory, WorkBuzz, TINYpulse, Syndio, Trusaic, Worklytics, Polinode, Visier, One Model, Crunchr, Confirm, Orgnostic, Kincentric, Mercer (employee listening practice), Insight222. Adjacent / talent-intelligence vendors: Lightcast, Eightfold AI, Gloat, Revelio Labs, TechWolf, SkyHive, Fuel50, Dayforce.

RENAME / ACQUISITION NOTES (historical names you may see):
- Humu → now part of Perceptyx
- Glint → now Microsoft Viva Glint
- Peakon → now Workday Peakon Employee Voice
- Kazoo → now WorkTango
- Orgnostic → now part of Culture Amp

CLASSIFY as one of:
- RELEVANT: Primary function is employee listening, people analytics, or closely related. Includes: survey methodology for employee surveys, employee experience research, workforce insights, HR data science, I-O psychologist at an EL vendor, people scientist roles.
- PARTIALLY_RELEVANT: Significant EL/PA responsibilities but not primary focus. Includes: HRBP with analytics responsibilities, VP People overseeing listening programs, OD specialist running culture surveys, consulting roles in human capital practices.
- NOT_RELEVANT: Not meaningfully related. Includes: customer experience, marketing analytics, IT helpdesk "digital employee experience", HRIS admin, compensation-only analytics, vendor sales/engineering (non-research), event-planning "engagement coordinator", generic data scientist, call center, market research survey firms (Pew, Nielsen, Ipsos — unless employee-focused).

CRITICAL EDGE CASES:
- "Active listening" as soft skill != "employee listening" as job function -> NOT_RELEVANT
- "Employee engagement coordinator" doing event planning -> NOT_RELEVANT
- "Employee engagement analyst" doing survey analytics -> RELEVANT
- Qualtrics sales engineer -> NOT_RELEVANT. Qualtrics research scientist -> RELEVANT.
- "People Partner" / "People Ops" without analytics -> NOT_RELEVANT
- "Survey Methodologist" at opinion polling firm -> NOT_RELEVANT
- "Digital Employee Experience" about IT/helpdesk -> NOT_RELEVANT
- "Chief People Officer" -> PARTIALLY_RELEVANT (oversees but doesn't do the work)
- Deloitte/PwC/McKinsey "Human Capital" consultant -> PARTIALLY_RELEVANT

FEW-SHOT EXAMPLES:
Title: "Employee Listening Manager" | Company: Netflix -> RELEVANT (confidence: 95) — Core EL role at major employer
Title: "Customer Experience Analyst" | Company: Nike -> NOT_RELEVANT (confidence: 98) — Customer, not employee
Title: "Data Scientist — People Team" | Company: Culture Amp -> RELEVANT (confidence: 90) — EL vendor, people team
Title: "Employee Engagement Coordinator" | Company: Marriott -> NOT_RELEVANT (confidence: 80) — Likely event planning at hospitality company
Title: "Senior Associate — Workforce Transformation" | Company: PwC -> PARTIALLY_RELEVANT (confidence: 70) — Consulting human capital practice
Title: "People Analytics & Total Rewards Manager" | Company: Visa -> PARTIALLY_RELEVANT (confidence: 65) — Hybrid role, PA is partial focus
Title: "NLP Engineer" | Company: Startup Inc -> NOT_RELEVANT (confidence: 75) — NLP without employee-feedback context is likely product engineering
Title: "Senior Manager" | Company: Perceptyx -> RELEVANT (confidence: 85) — Known EL vendor, any manager-level role likely touches employee data
Title: "People Research Scientist, Future of Work" | Company: Meta -> RELEVANT (confidence: 92) — People-science research role at major employer
Title: "Principal People Scientist" | Company: Workvivo -> RELEVANT (confidence: 95) — Principal role at core EL/EX vendor
Title: "Staff Program Manager, Employee Listening & Performance Cycles" | Company: Intuit -> RELEVANT (confidence: 85) — Senior EL program lead at enterprise
Title: "Employee Experience Coordinator" | Company: Panorama Mountain Resort -> NOT_RELEVANT (confidence: 30) — Hospitality coordinator, not analytics/listening
Title: "Patient Experience Program Manager" | Company: Hennepin Healthcare -> NOT_RELEVANT (confidence: 15) — Patient-focused, not employee-focused
Title: "Account Executive, Employee Experience" | Company: Qualtrics -> NOT_RELEVANT (confidence: 40) — EL vendor sales role, not research/analytics

Respond ONLY with JSON — no markdown, no explanation:
{{"classification": "RELEVANT|PARTIALLY_RELEVANT|NOT_RELEVANT", "confidence": 0-100, "reasoning": "one sentence"}}

JOB TO CLASSIFY:
Title: "{title}"
Company: "{company}"
Location: "{location}"
Description (may be snippet): "{description}"
"""


def _build_prompt(job: dict[str, Any]) -> str:
    desc = (job.get("description") or "")[:2000]
    return PROMPT_TEMPLATE.format(
        title=(job.get("title") or "").replace('"', "'"),
        company=(job.get("company") or "").replace('"', "'"),
        location=(job.get("location") or "").replace('"', "'"),
        description=desc.replace('"', "'"),
    )


def _parse_json(text: str) -> dict[str, Any] | None:
    """Strip markdown fences, parse JSON, validate shape. None on any failure."""
    if not text:
        return None
    cleaned = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        data = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    cls = data.get("classification")
    if cls not in _VALID_CLASSIFICATIONS:
        return None
    try:
        conf = int(data.get("confidence", 0))
    except (TypeError, ValueError):
        return None
    conf = max(0, min(100, conf))
    reasoning = str(data.get("reasoning", ""))[:500]
    return {"classification": cls, "confidence": conf, "reasoning": reasoning}


# ──────────────────────────── Providers ──────────────────────────

def _classify_groq(prompt: str, api_key: str) -> dict[str, Any] | None:
    import openai
    client = openai.OpenAI(base_url=GROQ_BASE_URL, api_key=api_key, timeout=30.0)
    resp = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=300,
    )
    content = resp.choices[0].message.content
    return _parse_json(content or "")


def _classify_gemini(prompt: str, api_key: str) -> dict[str, Any] | None:
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=api_key)
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.0,
            max_output_tokens=300,
        ),
    )
    return _parse_json(getattr(resp, "text", "") or "")


def _classify_openai(prompt: str, api_key: str) -> dict[str, Any] | None:
    import openai
    client = openai.OpenAI(api_key=api_key, timeout=30.0)
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.0,
        max_tokens=300,
    )
    content = resp.choices[0].message.content
    return _parse_json(content or "")


def _keyword_fallback(job: dict[str, Any]) -> dict[str, Any]:
    """Final tier: decide from keyword_score alone."""
    score = int(job.get("keyword_score", 0))
    if score >= 50:
        cls, conf = "RELEVANT", 60
    elif score >= 25:
        cls, conf = "PARTIALLY_RELEVANT", 50
    else:
        cls, conf = "NOT_RELEVANT", 50
    return {
        "classification": cls,
        "confidence": conf,
        "reasoning": f"keyword-only fallback (score={score})",
    }


# ──────────────────────────── Public API ─────────────────────────

def classify_job(
    job: dict[str, Any],
    *,
    groq_key: str | None = None,
    gemini_key: str | None = None,
    openai_key: str | None = None,
) -> dict[str, Any]:
    """Mutates `job` with llm_classification, llm_confidence, llm_provider, llm_reasoning.
    Returns the result dict. Walks the 4-tier chain; first success wins.
    """
    groq_key = groq_key if groq_key is not None else env("GROQ_API_KEY")
    gemini_key = gemini_key if gemini_key is not None else env("GEMINI_API_KEY")
    openai_key = openai_key if openai_key is not None else env("OPENAI_API_KEY")

    prompt = _build_prompt(job)

    providers = []
    if groq_key:
        providers.append(("groq", lambda: _classify_groq(prompt, groq_key)))
    if gemini_key:
        providers.append(("gemini", lambda: _classify_gemini(prompt, gemini_key)))
    if openai_key:
        providers.append(("openai", lambda: _classify_openai(prompt, openai_key)))

    result: dict[str, Any] | None = None
    provider_used = "keyword_only"
    for name, call in providers:
        try:
            result = call()
        except Exception as e:  # noqa: BLE001 — every provider can raise different errors; always fall through
            log.warning("%s classification failed: %s", name, e)
            result = None
        if result is not None:
            provider_used = name
            break

    if result is None:
        result = _keyword_fallback(job)

    # Snippet confidence haircut — if description came in as snippet, LLM sees less context
    if job.get("description_is_snippet") and provider_used != "keyword_only":
        result["confidence"] = max(0, int(result["confidence"]) - 10)

    job["llm_classification"] = result["classification"]
    job["llm_confidence"] = int(result["confidence"])
    job["llm_provider"] = provider_used
    job["llm_reasoning"] = result["reasoning"]
    return {**result, "provider": provider_used}


def publish_decision(job: dict[str, Any]) -> str:
    """Per filtering.md confidence routing. Returns 'publish' / 'publish_flag' / 'reject'."""
    cls = job.get("llm_classification", "NOT_RELEVANT")
    conf = int(job.get("llm_confidence") or 0)
    if cls == "RELEVANT" and conf >= 70:
        return "publish"
    if cls == "PARTIALLY_RELEVANT" and conf >= 70:
        return "publish"
    if cls == "PARTIALLY_RELEVANT" and 40 <= conf < 70:
        return "publish_flag"
    return "reject"


def classify_batch(
    jobs: list[dict[str, Any]],
    *,
    groq_key: str | None = None,
    gemini_key: str | None = None,
    openai_key: str | None = None,
    delay: float = GROQ_CALL_DELAY_SEC,
) -> tuple[list[str], dict[str, int]]:
    """Classify a batch. Returns (errors, {provider_name: count})."""
    errors: list[str] = []
    counts: dict[str, int] = {}
    for i, job in enumerate(jobs):
        try:
            r = classify_job(job, groq_key=groq_key, gemini_key=gemini_key, openai_key=openai_key)
            counts[r["provider"]] = counts.get(r["provider"], 0) + 1
        except Exception as e:  # noqa: BLE001
            errors.append(f"llm: failed on {job.get('external_id','?')}: {e}")
        if i < len(jobs) - 1:
            time.sleep(delay)
    return errors, counts
