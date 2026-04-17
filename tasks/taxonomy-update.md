# Taxonomy Audit Implementation — Task File for Claude Code

Read CLAUDE.md first for project context. Then implement these changes in order.
All changes go into `config/keywords.yaml`, `config/companies.yaml`, `src/processors/keyword_filter.py`, `src/processors/llm_classifier.py`, and related test files.
Test after each section. Do NOT push to GitHub until I approve. Do NOT call real APIs.

---

## PHASE A: Preprocessing Pipeline (keyword_filter.py)

Add a `_preprocess(text: str) -> str` function that runs BEFORE any keyword matching. Apply it to both title and description in `score_job()`.

```python
import html
import re
import unicodedata

def _preprocess(text: str) -> str:
    """Normalize text before keyword matching."""
    if not text:
        return ""
    # 1. Decode HTML entities: &amp; → &, &nbsp; → space
    text = html.unescape(text)
    # 2. Strip HTML tags → single space (not empty)
    text = re.sub(r'<[^>]+>', ' ', text)
    # 3. Unicode NFKC normalization
    text = unicodedata.normalize('NFKC', text)
    # 4. Normalize dashes (U+2010-2015, U+2212, U+00AD) → ASCII hyphen
    text = re.sub(r'[\u2010-\u2015\u2212\u00AD]', '-', text)
    # 5. Normalize quotes (smart quotes → ASCII)
    text = re.sub(r'[\u2018\u2019]', "'", text)
    text = re.sub(r'[\u201C\u201D]', '"', text)
    # 6. Strip possessives before matching
    text = re.sub(r"'s\b", '', text)
    # 7. Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text
```

Apply `_preprocess()` to title and description at the START of `score_job()` before any matching happens.

Add tests for preprocessing: HTML entities, smart quotes, non-breaking hyphens, HTML tags, possessives.

---

## PHASE B: Scoring Recalibration (keyword_filter.py + keywords.yaml)

### B1: Cap T2 Description stacking

In `score_job()`, after accumulating T2 description matches, cap the total contribution from T2 desc at **16 points maximum** (i.e., max 2 matches count). This prevents description-only stacking to auto-publish threshold.

### B2: Require title match for auto-publish

Change the auto-publish logic: score >= 50 AND at least one positive keyword matched in the TITLE. If score >= 50 but all matches were description-only, route to `llm_review` instead of `auto_include`.

### B3: Gate company boost

Change company boost from +15 unconditional to **+10, but only if at least one positive keyword already matched** (any tier, any field). If zero positive keywords matched, company boost = 0. This prevents "Staff Accountant at Perceptyx" from reaching AI review.

### B4: Raise AI review floor

Change `llm_review_min` threshold from 10 to **15** in keywords.yaml. T3-alone (5-8 pts) no longer triggers AI review.

### B5: Downgrade T3 Title weights

In keywords.yaml, change `tier3_title` score from 10 to **7**. 

Also add a co-signal requirement: T3 title keywords only count if the description also contains at least one term from a co-signal list: `analytics, insights, data, dashboard, survey, Python, SQL, Tableau, R programming, Qualtrics, Workday, people data, HRIS, Visier`.

### B6: Graduated reducers

Replace the flat -15 for all score_reducers. Instead:

**Hard reducers (-40):**
- HRIS analyst
- HRIS administrator
- Workday HCM configurator
- SuccessFactors admin

**Medium reducers (-25):**
- market research
- brand strategy
- change management consultant (alone, without listening/analytics co-term)

**Standard reducers (-20):**
- customer engagement
- digital marketing
- revenue analytics
- product analytics
- web analytics
- campaign analytics
- user analytics
- growth analytics

### B7: Demote "continuous listening" from T1 Title

Move "continuous listening" from `tier1_title` (50 pts) to `tier2_title` (25 pts). Add a GATE: only score as T1 (50 pts) if "employee" or "workforce" appears within 60 characters of the match. Otherwise score as T2 (25 pts).

### B8: Downgrade "employee experience" and "workforce planning" in T3

- "employee experience" — keep in T3 but reduce to **5 points** and require co-signal (analytics/insights/data/survey/listening in title or first 400 chars of desc). Without co-signal, score 0.
- "workforce planning" — keep in T3 but reduce to **5 points** and require co-signal. Without co-signal, score 0. This kills hospital nurse scheduling and call-center WFM false positives.

Update all tests to reflect new scoring.

---

## PHASE C: Keyword Gating (keyword_filter.py + keywords.yaml)

### C1: Gate ONA

Remove bare "ONA" from tier2_description. Replace with the full phrase "organizational network analysis" (already in T1 title and T1 desc — verify it's there). Add "organisational network analysis" (British spelling) alongside it.

### C2: Gate Glint

Remove bare "Glint" from tier2_description. Replace with:
- "Viva Glint" (add to T1 desc if not already there)
- "Microsoft Viva Glint" (add to T1 desc)
Keep "Viva Glint" in T1 desc (already there — verify).

### C3: Gate Qualtrics and Medallia

In tier2_description, change:
- "Qualtrics" → only match "Qualtrics EX", "Qualtrics EmployeeXM", "Qualtrics Employee Experience" (these are already in T1 desc — remove bare "Qualtrics" from any tier if it exists there without EX qualifier)
- "Medallia" → only match "Medallia EX" or "Medallia employee". Remove bare "Medallia" from T2 desc. Keep qualified forms in T1 desc.

### C4: Normalize CultureAmp

In tier2_description, change "CultureAmp" to "Culture Amp" (with space). The preprocessing + word-boundary regex will handle it. Add both "CultureAmp" and "Culture Amp" as aliases that both match.

### C5: Gate "XM Scientist"

Move "XM scientist" from T2 title to require co-term: only match if "employee" or "EX" or "EE" appears in title or description within 60 chars. Otherwise, do not score (most Qualtrics XM Scientists are CX-side).

### C6: Cross-field dedup

If the same keyword matches in BOTH title and description, count it only once at the higher-tier score. Example: "people analytics" in title (T1, 50 pts) AND in description (T1, 30 pts) → count only the 50, not 80.

### C7: Maximal-munch dedup

If "people analytics manager" matches (T1 title, 50 pts) and "people analytics" also matches (T1 title, 50 pts), only count the longest match. Do not double-count overlapping phrases.

Add tests for each gating rule.

---

## PHASE D: Content Additions — Positive Keywords (keywords.yaml)

### D1: T1 Title additions (50 pts)

Add these to `tier1_title`:
- "head of people analytics"
- "VP people analytics"
- "chief people analytics officer"
- "global head of people analytics"
- "head of people insights"
- "principal people scientist"
- "senior people scientist"
- "staff people scientist"
- "lead people scientist"
- "people research scientist"
- "research scientist, people"
- "research scientist, HR"
- "employee listening program manager"
- "continuous listening program manager"
- "listening strategy lead"
- "listening architect"
- "pay equity analyst"
- "workplace equity analyst"
- "strategic workforce planning manager"
- "talent intelligence analyst"
- "talent intelligence lead"
- "decision scientist, people"

### D2: T2 Title additions (25 pts → or 20 pts per Phase B)

Add these to `tier2_title`:
- "behavioral scientist" (gate: require HR/people/employee/workforce co-term)
- "survey methodologist"
- "people data engineer"
- "labor economist" (gate: require corporate/HR context, not academic)
- "workforce economist"
- "skills intelligence analyst"
- "manager effectiveness analyst"
- "employee experience advisor"
- "associate experience manager"
- "associate experience analyst"
- "colleague experience manager"
- "partner experience analyst" (gate: require Starbucks or HR context)
- "workforce strategy consultant"

### D3: T1 Description additions (30 pts → or 25 pts per Phase B)

Add these vendor/product names to `tier1_description`:
- "Workday Peakon Employee Voice"
- "Microsoft Viva Glint"
- "Viva Pulse"
- "Viva Insights"
- "Perceptyx Ask"
- "Perceptyx Listen"
- "Perceptyx Activate"
- "Achievers Listen"
- "WorkTango"
- "Energage"
- "Workhuman"
- "Quantum Workplace"
- "DecisionWise"
- "Gallup Q12"
- "Workvivo"
- "Effectory"
- "Medallia EX"

### D4: T2 Description additions (10 pts, subject to cap)

Add to `tier2_description`:
- "driver analysis"
- "linkage research"
- "always-on listening"
- "passive listening"
- "feedback intelligence"
- "conversational surveys"
- "moments that matter"
- "skills intelligence"
- "pay equity"
- "workforce intelligence"
- "people intelligence"
- "manager effectiveness"
- "Syndio"
- "Trusaic"
- "Lightcast"
- "Eightfold"
- "Worklytics"
- "Humanyze"
- "Polinode"

---

## PHASE E: Content Additions — Negative Keywords (keywords.yaml)

### E1: New auto-reject terms

Add to `negative_auto_reject`:
- "voice of the patient"
- "voice of the citizen"
- "voice of the veteran"
- "patient experience analyst"
- "patient experience director"
- "patient satisfaction"
- "Press Ganey"
- "CX analyst"
- "CX manager"
- "CX strategy"
- "clinical psychologist"
- "counseling psychologist"
- "forensic psychologist"
- "school psychologist"
- "neuropsychologist"
- "psychometrist"
- "speech-language pathologist"
- "UX researcher"
- "event coordinator"
- "hospitality coordinator"
- "customer service manager"
- "revenue operations analyst"
- "Sirius XM"
- "XM Radio"

### E2: New score reducers

Add to `negative_score_reducers` at -20:
- "instructional designer"
- "curriculum developer"
- "training facilitator"
- "LMS administrator"
- "internal communications manager"
- "newsletter manager"
- "compensation consultant"
- "talent acquisition consultant"

Add at -40 (hard reducer):
- "Workday HCM configurator"
- "SuccessFactors administrator"
- "assessment scientist"
- "selection scientist"

---

## PHASE F: Vendor List Updates (companies.yaml + llm_classifier.py)

### F1: Add to companies.yaml (Tier 1 — core EL/PA vendors)

- WorkTango
- Energage
- Quantum Workplace
- DecisionWise
- Workhuman
- Achievers
- Workvivo
- Effectory
- WorkBuzz
- TINYpulse
- Syndio
- Trusaic
- Worklytics
- Polinode

### F2: Add to companies.yaml (Tier 2 — adjacent/TI vendors)

- Lightcast
- Eightfold AI
- Gloat
- Revelio Labs
- TechWolf
- Cornerstone (SkyHive)
- Fuel50
- Dayforce

### F3: Update LLM prompt vendor list

In `llm_classifier.py`, update the vendor context list to include all new Tier 1 vendors from F1. Also add rename notes:
- Humu → now part of Perceptyx
- Glint → now Microsoft Viva Glint
- Peakon → now Workday Peakon Employee Voice
- Kazoo → now WorkTango
- Orgnostic → now part of Culture Amp

### F4: Expand few-shot examples from 8 to 14

Add these 6 new examples to the LLM prompt:

**Positive:**
1. "People Research Scientist, Future of Work" | Meta → RELEVANT (92)
2. "Principal People Scientist" | Workvivo → RELEVANT (95)
3. "Staff Program Manager, Employee Listening & Performance Cycles" | Intuit → RELEVANT (85)

**Negative:**
4. "Employee Experience Coordinator" | Panorama Mountain Resort → NOT_RELEVANT (30)
5. "Patient Experience Program Manager" | Hennepin Healthcare → NOT_RELEVANT (15)
6. "Account Executive, Employee Experience" | Qualtrics → NOT_RELEVANT (40)

---

## PHASE G: Update Tests

Update all existing tests to reflect:
- New scoring weights (T3 = 7, T2 desc cap = 16, company boost = 10 gated, AI floor = 15)
- New preprocessing
- New gating rules (ONA, Glint, Qualtrics, Medallia, XM Scientist)
- Cross-field and maximal-munch dedup
- Title-required auto-publish
- Graduated reducers

Add new tests:
- All new positive keywords match correctly
- All new negative keywords reject correctly
- Gating rules work (ONA without context = no match, Glint bare = no match, etc.)
- T2 desc cap works (3 T2 desc matches = 16 not 30)
- Company boost gate works (no positive match = no boost)
- Title-required auto-publish works (desc-only 50+ → llm_review not auto_include)
- Preprocessing handles HTML entities, smart quotes, Unicode dashes
- Cross-field dedup works
- Maximal-munch dedup works
- Graduated reducers apply correct weights

Run full test suite. All tests must pass.

---

## Summary of scoring after all changes

| Signal | Points | Cap/Gate |
|---|---|---|
| T1 Title | 50 | — |
| T1 Desc (pure-play vendor) | 25 | — |
| T1 Desc (multi-category vendor) | 15 | Require employee/EX co-term |
| T2 Title | 20-25 | Some gated (XM Scientist, behavioral scientist) |
| T2 Desc | 8-10 per match | **Capped at 16 total** |
| T3 Title | 5-7 | Require co-signal |
| Company boost | +10 | Only if ≥1 positive keyword matched |
| Auto-publish threshold | 50+ | **AND ≥1 title match required** |
| AI review floor | 15 | (was 10) |
| Hard reducer | -40 | HRIS, HCM admin, assessment |
| Medium reducer | -25 | Market research, brand strategy |
| Standard reducer | -20 | Marketing, product, web analytics |
| Auto-reject | -100 | In title with no positive match |
