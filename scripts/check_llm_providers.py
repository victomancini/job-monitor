"""Probe each LLM provider with a tiny classification call. Reports success
or a short failure reason per provider so you can tell which ones are healthy
without running the full pipeline.

Usage:
    export GROQ_API_KEY=...
    export GEMINI_API_KEY=...
    export OPENAI_API_KEY=...   # optional, paid
    python scripts/check_llm_providers.py
"""
from __future__ import annotations

import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.processors import llm_classifier as lc

PROBE_JOB = {
    "title": "People Analytics Manager",
    "company": "Netflix",
    "location": "Los Gatos, CA",
    "description": "Lead the people analytics team on workforce insights.",
    "keyword_score": 60,
}


def probe(label: str, fn, key: str) -> None:
    if not key:
        print(f"[{label}] SKIPPED — env var empty")
        return
    try:
        prompt = lc._build_prompt(PROBE_JOB)
        result = fn(prompt, key)
    except Exception as e:  # noqa: BLE001
        # Show the full exception trace for debugging. The sanitizer in
        # llm_classifier._sanitize_err runs in production — here we want the
        # raw message so you can diagnose.
        print(f"[{label}] FAILED — {type(e).__name__}: {e}")
        if "--verbose" in sys.argv:
            traceback.print_exc()
        return
    if result is None:
        print(f"[{label}] UNHEALTHY — call returned but _parse_json rejected the response "
              "(bad schema or non-JSON). Re-run with --verbose to see raw text.")
        return
    print(f"[{label}] OK — classification={result.get('classification')} "
          f"confidence={result.get('confidence')}")


def main() -> int:
    print("Probing Groq, Gemini, OpenAI with a single 'Netflix PA Manager' job...\n")
    probe("groq", lc._classify_groq, os.environ.get("GROQ_API_KEY", ""))
    probe("gemini", lc._classify_gemini, os.environ.get("GEMINI_API_KEY", ""))
    probe("openai", lc._classify_openai, os.environ.get("OPENAI_API_KEY", ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
