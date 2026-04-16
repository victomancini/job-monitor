---
paths:
  - "src/processors/**/*.py"
---
# Filtering and Classification Rules

## DESIGN PRINCIPLE
Zero false positives. Rather miss 20 real postings than publish 1 wrong one.

## Keyword Filter (src/processors/keyword_filter.py)
- Load config/keywords.yaml
- Word-boundary regex for ALL matching: re.search(r'\b' + re.escape(term) + r'\b', text, re.IGNORECASE)
- IMPORTANT — evaluation order:
  1. Score ALL positive keywords on title + description. Accumulate across tiers. Cap at 100.
  2. Check ALL negative keywords on title + description.
  3. If BOTH positive (score >= 10) AND any negative match → decision = "llm_review" (NEVER auto-decide conflicts)
  4. If ONLY negative_auto_reject matches title (no positives) → return score = -100, decision = "reject"
  5. If only positive keywords matched → apply thresholds normally
- Thresholds (step 5 only): auto_include >= 50, llm_review 10-49, auto_reject < 10
- Return: (score, matched_keywords, decision)

## LLM Classification Prompt
Used identically across Groq, Gemini, and GPT-4o-mini.

```
You are classifying job postings for the employee listening and people analytics field. US job market focus.

WHAT THIS FIELD IS:
Employee listening = systematic collection and analysis of employee feedback through surveys (engagement, pulse, lifecycle, 360), text analytics on open-ended responses, organizational network analysis, or continuous listening platforms. People analytics = using quantitative employee data to improve HR decisions: workforce planning, attrition modeling, engagement measurement, talent analytics, human capital analytics.

KNOWN VENDORS IN THIS SPACE (roles at these companies touching employee data are likely RELEVANT):
Perceptyx, Qualtrics (EmployeeXM/EX), Culture Amp, Workday (Peakon), Microsoft (Viva Glint, Viva Pulse), Medallia (employee side), Gallup (workplace division), Lattice, 15Five, Leapsome, Betterworks, Quantum Workplace, DecisionWise, Visier, One Model, Crunchr, Confirm, Orgnostic, Kincentric, Mercer (employee listening practice), Insight222, Humu (acquired by Perceptyx).

CLASSIFY as one of:
- RELEVANT: Primary function is employee listening, people analytics, or closely related. Includes: survey methodology for employee surveys, employee experience research, workforce insights, HR data science, I-O psychologist at an EL vendor, people scientist roles.
- PARTIALLY_RELEVANT: Significant EL/PA responsibilities but not primary focus. Includes: HRBP with analytics responsibilities, VP People overseeing listening programs, OD specialist running culture surveys, consulting roles in human capital practices.
- NOT_RELEVANT: Not meaningfully related. Includes: customer experience, marketing analytics, IT helpdesk "digital employee experience", HRIS admin, compensation-only analytics, vendor sales/engineering (non-research), event-planning "engagement coordinator", generic data scientist, call center, market research survey firms (Pew, Nielsen, Ipsos — unless employee-focused).

CRITICAL EDGE CASES:
- "Active listening" as soft skill ≠ "employee listening" as job function → NOT_RELEVANT
- "Employee engagement coordinator" doing event planning → NOT_RELEVANT
- "Employee engagement analyst" doing survey analytics → RELEVANT
- Qualtrics sales engineer → NOT_RELEVANT. Qualtrics research scientist → RELEVANT.
- "People Partner" / "People Ops" without analytics → NOT_RELEVANT
- "Survey Methodologist" at opinion polling firm → NOT_RELEVANT
- "Digital Employee Experience" about IT/helpdesk → NOT_RELEVANT
- "Chief People Officer" → PARTIALLY_RELEVANT (oversees but doesn't do the work)
- Deloitte/PwC/McKinsey "Human Capital" consultant → PARTIALLY_RELEVANT

FEW-SHOT EXAMPLES:
Title: "Employee Listening Manager" | Company: Netflix → RELEVANT (confidence: 95) — Core EL role at major employer
Title: "Customer Experience Analyst" | Company: Nike → NOT_RELEVANT (confidence: 98) — Customer, not employee
Title: "Data Scientist — People Team" | Company: Culture Amp → RELEVANT (confidence: 90) — EL vendor, people team
Title: "Employee Engagement Coordinator" | Company: Marriott → NOT_RELEVANT (confidence: 80) — Likely event planning at hospitality company
Title: "Senior Associate — Workforce Transformation" | Company: PwC → PARTIALLY_RELEVANT (confidence: 70) — Consulting human capital practice
Title: "People Analytics & Total Rewards Manager" | Company: Visa → PARTIALLY_RELEVANT (confidence: 65) — Hybrid role, PA is partial focus
Title: "NLP Engineer" | Company: Startup Inc → NOT_RELEVANT (confidence: 75) — NLP without employee-feedback context is likely product engineering
Title: "Senior Manager" | Company: Perceptyx → RELEVANT (confidence: 85) — Known EL vendor, any manager-level role likely touches employee data

Respond ONLY with JSON — no markdown, no explanation:
{"classification": "RELEVANT|PARTIALLY_RELEVANT|NOT_RELEVANT", "confidence": 0-100, "reasoning": "one sentence"}
```

## Confidence-Based Routing
- RELEVANT + confidence >= 70 → PUBLISH
- PARTIALLY_RELEVANT + confidence >= 70 → PUBLISH with "partial" flag
- PARTIALLY_RELEVANT + confidence 40-69 → PUBLISH with "review" flag
- Everything else → REJECT

## The 16 False Positive Patterns
0. "Active listening skills" — #1 source, ~90% of postings
1. Customer Experience Analyst
2. Marketing/Digital Analytics ("engagement" = user engagement)
3. Community/Donor Engagement
4. Market Research Survey Analyst (Nielsen, Ipsos, Pew)
5. People Operations Coordinator
6. Intelligence Analyst (defense/security)
7. HRIS/Workday Administrator
8. EL vendor sales/engineering roles
9. Generic Data Scientist
10. Patient Experience (healthcare)
11. HRBP (analytics as 1 bullet among 20)
12. Employee Relations Specialist
13. Compensation-only Analyst
14. OCM Consultant
15. Social/Brand Listening Manager

## Deduplicator (src/processors/deduplicator.py)
- rapidfuzz with processor=rapidfuzz.utils.default_process on ALL calls
- Company norm: lowercase, strip " inc", " llc", " corp", " ltd", " co.", " corporation", " company"
- Title norm: lowercase, "sr." → "senior", "jr." → "junior", "mgr" → "manager", "dir" → "director", "vp" → "vice president"
- Composite: 0.4 * company_sim + 0.4 * title_sim + 0.2 * city_match
- city_match: 100 if same city, 0 otherwise
- >= 85: duplicate (skip). 70-84: flag (include but mark). < 70: unique.
- Compare new jobs against: (a) batch peers, (b) active Turso records
- Ensure Turso indexes on external_id, company_normalized
