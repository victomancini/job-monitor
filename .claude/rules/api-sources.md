---
paths:
  - "src/sources/**/*.py"
  - "src/processors/llm_classifier.py"
---
# API Source and LLM Rules

## JSearch API (src/sources/jsearch.py) — PRIMARY
- Endpoint: GET https://jsearch.p.rapidapi.com/search
- Headers: X-RapidAPI-Key, X-RapidAPI-Host: jsearch.p.rapidapi.com
- Free tier: 200 req/month. Start with num_pages=1 (10 results per query). Monitor X-RapidAPI-Requests-Remaining to determine if num_pages counts as 1 or N requests. Increase to num_pages=3 only after confirming billing.
- Budget: 3 queries/day * 30 days = 90 req/month target, 55% buffer.
- READ and LOG X-RapidAPI-Requests-Remaining header every call. Alert when < 40 remaining.
- date_posted: "today", "3days", "week", "month"
- 1-second delay between queries
- Map job_id to external_id as "jsearch_{job_id}"
- Supports country filter: add country=us parameter for US-primary focus

## Jooble API (src/sources/jooble.py) — SECONDARY
- Endpoint: POST https://jooble.org/api/{JOOBLE_API_KEY}
- Body: {"keywords": str, "location": str, "page": int}
- US-PRIMARY: set location="United States" on all queries
- Bonus: run one additional query each for "United Kingdom", "Canada", "Australia"
- radius only accepts: 0, 4, 8, 16, 26, 40, 80 km
- Returns snippets not full descriptions. Set description_is_snippet=True.
- Map id to external_id as "jooble_{id}"
- 1-second delay

## Adzuna API (src/sources/adzuna.py) — TERTIARY
- Endpoint: GET https://api.adzuna.com/v1/api/jobs/{country}/search/{page}
- Auth: app_id + app_key as query params
- US-PRIMARY: use country="us" for main queries
- Bonus: run one query each for "gb", "ca", "au"
- Returns: results array with title, company.display_name, location.display_name, description, redirect_url, salary_min, salary_max, created
- Map id to external_id as "adzuna_{id}"
- 1-second delay

## USAJobs API (src/sources/usajobs.py) — FEDERAL GOVERNMENT (UNIQUE COVERAGE)
- Endpoint: GET https://data.usajobs.gov/api/search
- Auth: Two headers required: `Authorization-Key: {USAJOBS_API_KEY}` AND `User-Agent: {USAJOBS_EMAIL}`
- Register at developer.usajobs.gov to get both
- No rate limit concerns for this volume (federal open data API)
- IMPORTANT: Federal jobs are NOT indexed by Google for Jobs. This is the only source for these roles.
- Target OPM Series: 0180 (Psychology), 0343 (Management Analysis), 1515 (Operations Research)
- Keywords: "people analytics", "organizational development", "HR analytics", "survey research", "industrial organizational", "employee engagement", "workforce analytics"
- Run WEEKLY, not daily (federal postings change slowly, typically open 5-30 days)
- Map MatchedObjectId to external_id as "usajobs_{MatchedObjectId}"
- Response: SearchResult.SearchResultItems array. Each has MatchedObjectId, PositionTitle, OrganizationName, PositionLocationDisplay, PositionURI, QualificationSummary, MinimumRange, MaximumRange

## Google Alerts + Talkwalker RSS (src/sources/google_alerts.py) — SUPPLEMENTARY
- Use feedparser to parse RSS feed URLs from env vars
- Parse BOTH Google Alerts (GOOGLE_ALERT_RSS_1-5) and Talkwalker Alerts (TALKWALKER_RSS_1-3)
- Also parse SIOP career center alert (GOOGLE_ALERT_SIOP)
- Google Alerts: max 20 items per feed, known to silently stop updating
- Track <updated> timestamps. If no new entries for 7+ days, flag in healthcheck ping.
- Heuristic filter: SKIP if title contains "blog", "article", "guide", "how to", "opinion", "review", "podcast", "webinar"
- ALL results get mandatory LLM review regardless of keyword score
- Generate external_id as "galert_{sha256(link)[:12]}"

## LLM Classifier — Multi-Provider Chain (src/processors/llm_classifier.py)
Priority: Groq → Gemini 2.5 Flash-Lite → GPT-4o-mini → keyword-only fallback

### Tier 1: Groq (free)
- Endpoint: https://api.groq.com/openai/v1/chat/completions (OpenAI-compatible)
- Model: "llama-3.3-70b-versatile"
- Use openai package: client = openai.OpenAI(base_url="https://api.groq.com/openai/v1", api_key=GROQ_API_KEY)
- Free tier: ~1,000 RPD, 30 RPM. Use 2.5-second delay between calls.
- response_format={"type": "json_object"} for structured output
- On 429 or 5xx: fall through to Gemini

### Tier 2: Gemini 2.5 Flash-Lite (free)
- SDK: from google import genai; client = genai.Client(api_key=key)
- Model: "gemini-2.5-flash-lite" (NOT gemini-2.0-flash-lite — deprecated June 1 2026)
- Free tier: ~1,000 RPD, 15 RPM
- Use response_mime_type="application/json"
- Strip markdown fences before json.loads()
- On error: fall through to GPT-4o-mini

### Tier 3: GPT-4o-mini (paid, ~$10/yr)
- Endpoint: https://api.openai.com/v1/chat/completions
- Model: "gpt-4o-mini"
- Use openai package with default base_url
- Cost: ~$0.003/day ≈ $1/year at this volume
- On error: fall through to keyword-only

### Tier 4: Keyword-only fallback (free, built-in)
- If all three LLM providers fail, use the keyword score alone
- Score >= 50: publish. Score 25-49: publish with "unvalidated" flag. Score < 25: reject.
- This ensures the system NEVER stops processing due to LLM outages.

### All Providers: JSON Parsing
- Always try/except on json.loads()
- Strip markdown fences: text.strip().removeprefix("```json").removesuffix("```").strip()
- On parse failure: retry once, then fall to next tier
- Log every classification: provider, classification, confidence, reasoning

## Error Isolation
Every source function must:
1. Accept credentials as parameters
2. Return (list[dict], list[str]) — (results, errors)
3. Catch exceptions per-query, log, continue remaining
4. Retry: 3 attempts, backoff 2s/4s/8s on 5xx/timeout
5. Return partial results on partial failure
6. Log: source, queries attempted, results found, errors, quota remaining
