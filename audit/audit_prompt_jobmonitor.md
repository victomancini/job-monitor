# Project Audit — Job Monitor (customized)

You are auditing this codebase. The goal is an honest, evidence-backed assessment that leads to action on **Victor's next weekend of work** — not a checklist performance, not a fabricated gotcha list, not a greatest-hits tour of every lint rule.

This is a customization of a generic audit prompt. The stakes, tooling, audience, and prior-round history are already known — they're encoded below rather than asked. Read them and proceed. Do not re-elicit.

---

## Pre-set Engagement Context

**System.** Personal job-monitoring pipeline. Aggregates employee-listening / people-analytics postings from ~11 sources (JSearch, Jooble, Adzuna, USAJobs, Greenhouse, Lever, Ashby, JobSpy, Google Alerts RSS, SIOP, OneModel), filters via keyword + 4-tier LLM chain, deduplicates, enriches URLs, publishes to a self-hosted WordPress site, alerts Victor via Pushover/email.

**Purpose.** Post-refactor health check. The R11 initiative just landed (field provenance, consensus voting, schema.org extraction, enrichment guardrails). Audit whether that changed the risk surface and whether any Rs before it left debt the new code trips over.

**Audience.** Victor. Solo maintainer. One person, reads his own findings, remediates himself in weekend chunks (~4-8 hours at a time). No engineering team, no exec summary needed, no compliance officer.

**Stakes.** Accuracy of job leads for Victor's own search. "Failure" means: bad URLs that don't lead anywhere, wrong remote/hybrid flags that cause him to miss or waste time on roles, duplicated entries, silent drift where numbers look fine but data is wrong. No money, no PII, no downtime (GitHub Actions cron, not a service), no regulatory exposure, no multi-tenant.

**Capacity.** Solo. Weekend-scale. A "Large" finding is ≥1 weekend; "Medium" is a few hours; "Small" is under an hour. Remediation bandwidth is maybe 1-2 Larges per month at realistic pace.

**Access.** Full local. `pytest` runs (845 tests at time of writing, passing). No type checker or linter installed — their absence is itself a data point, not a blocker. No staging env; changes ship via git push → GitHub Actions cron. Turso DB is live but a dry-run mode exists (`--dry-run`).

**Known pain.** Documented in `tasks/todo.md` R-log and `tasks/lessons.md`. Read these first. The recurring themes:
1. Turso stream timeouts on long enrichment (R10 added reconnect wrapper; R11 added budget + circuit breaker)
2. Aggregator field noise on `is_remote` and `date_posted` (R11 addressed via consensus voting)
3. WP ⇄ Turso desync on `first_seen_date` (R11 Phase 0 fixed)
4. Rate limit bursts: JSearch 200/month, Groq daily TPD, Jooble 403s
5. Dedup edge cases around multi-location roles
6. Silent failures in per-job `try/except` loops (R10 discovered 42 swallowed stream errors)

**Pre-set deep-audit targets.** These are the modules where failure is most consequential. Confirm or revise, then proceed.
- `src/db.py` — state-of-the-record; a bug here corrupts everything downstream
- `src/processors/keyword_filter.py` — zero-false-positive gate, CLAUDE.md's hardest constraint
- `src/processors/llm_classifier.py` — 4-tier fallback chain; silent degradation risk
- `src/processors/deduplicator.py` — composite fuzzy matching + R11 consensus voting
- `src/processors/enrichment.py` — network I/O, guardrails, redirect following
- `src/collector.py` — pipeline orchestrator; ordering is a hard constraint per CLAUDE.md
- `wordpress/job-monitor.php` — the UI; desync with Python = user-visible wrong data

Everything else gets a surface pass. No threat model required — system has no internet-facing endpoints, no auth surface, no user input except config YAML.

---

## Step 1 — Signal Gathering (bounded; ≤30 minutes)

Run these, verbatim-capture outputs into the findings file as evidence:

```
python -m pytest -q                                     # expect 845 passed; note any flakes
python -m pytest --co -q | tail -3                      # test count; sanity
git log --since="180 days ago" --numstat --format= | awk '{f[$3]+=$1+$2} END{for(k in f)print f[k],k}' | sort -rn | head -20   # churn
grep -n "TODO\|FIXME\|XXX\|HACK" -r src/ wordpress/ | head -30   # known tech debt markers
grep -rn "except Exception" src/ | wc -l                # exception swallow count
find . -name "*.py" | xargs wc -l | tail -1            # code volume
```

No linter / type checker installed. **Note that absence as a finding** (it's relevant for a project this size) and proceed. Do not install one for the audit.

Docs: `CLAUDE.md` is the source of truth for conventions and rules. `README.md` (if it exists — check) is secondary. Any contradiction between code and `CLAUDE.md` is a finding.

Produce a **Signal Report** (≤ 1 page) before Step 2. Include:
- pytest outcome line
- top 10 churn files (concentrated risk)
- exception count
- total LOC
- any README↔code gap

---

## Step 2 — Criticality Map + Threat Envelope

The deep-audit targets are pre-set above. Your job is to **confirm or revise**, not re-derive. If you revise, justify in one sentence.

Then write the **Threat Envelope** in one paragraph — not a full threat model, just the boundary of what this system can and can't lose:

- Secrets at risk: `WP_APP_PASSWORD`, Turso auth token, JSearch/RapidAPI key, Groq/Gemini/OpenAI keys, Pushover token, Brevo SMTP credentials
- Attack surface: the WordPress site's custom REST endpoint (behind Basic Auth + optional `X-JM-Secret`), GitHub Actions secrets store, aggregator API responses (untrusted input we parse)
- Not at risk: user data (there are no users), real money, service uptime SLO, downstream consumers

Any finding outside this envelope is almost certainly Low or should be Accepted Risk.

---

## Step 3 — The Four Lenses

Apply to the deep-audit targets in depth. Surface pass on the rest. Tag findings with any lens that applies.

### Lens A — Correctness & Design
- Module boundaries vs. `CLAUDE.md`'s declared pipeline order (hard constraint)
- Hidden global state (R11 added `_circuit_breaker` and `_fetch_budget` at module level — is that safe under test isolation?)
- Silent error swallowing — every `except Exception` block should be interrogated: does it log usefully, does it leave partial state, does it hide real bugs?
- Tests that assert nothing real, or fixtures that don't reflect production shapes
- Zero-false-positives principle violations in keyword_filter.py
- Consensus voting math: can `compute_consensus` produce an answer with suspiciously high confidence from one source alone?

### Lens B — Security & Data Flow
- Secrets-in-logs audit (especially `raw_data` column which we cap at 50KB — does it ever contain an API key echoed back?)
- HTTPS enforcement (R7-C handles WP_URL + HEALTHCHECK_URL — are there others?)
- SQL injection in ad-hoc queries (libsql is sqlite3-compatible; any string concat into SQL?)
- User-Agent and referrer exposure in enrichment HTTP
- The WP plugin's `sanitize_text_field` + `esc_html/esc_url_raw/esc_attr` usage — all output paths escaped?
- Supply chain: `requirements.txt` pinning quality, abandoned-library check on key deps (libsql, google-genai, rapidfuzz, feedparser, jobspy)

### Lens C — Reliability & Operations
- Every outbound HTTP has a timeout? Every retry is idempotent?
- The Turso stream reconnect wrapper (R10) — tested against the actual failure mode or just a simulated one?
- Healthchecks.io observability: is the rich ping body actually useful at 3am, or padding?
- Rate-limit canary logic for JSearch 80% warning
- What breaks at 10x job volume (e.g., a niche expansion)? Enrichment budget? LLM tokens? Turso rows?
- Zero-results canary: will it fire correctly on an all-sources-down day?
- GH Actions workflow — is the keepalive cron actually doing the job? What if it stops?

### Lens D — Maintainability & Solo Human Cost
- Clone-to-green: can a fresh clone run the test suite without manual setup beyond `pip install`?
- CLAUDE.md vs. reality drift — is anything documented there no longer true?
- Tests that take more than a few seconds each (one found the suite takes ~4 minutes; is that the bottleneck?)
- Config duplication across YAML files
- Dead code from past R-rounds that survived a pivot

---

## Step 4 — Findings Format

**Write to `audit/findings.md` as you go.** Do not accumulate in memory and dump at the end.

Each finding:

| Field | Notes |
|---|---|
| **ID** | F-001, F-002, … |
| **Severity** | Critical / High / Medium / Low — calibrated to solo stakes (see below) |
| **Lenses** | A / B / C / D (any that apply) |
| **Where** | Exact `file:line` or `file:line_range`. No vague references. |
| **What** | One sentence. |
| **Evidence** | Snippet, command output, or test trace. Not paraphrase. |
| **Consequence** | Concrete: "Victor wastes 20 min debugging a silent dedup false-negative." Not "reduces quality." |
| **Fix** | Enough direction to start. Full patch optional. |
| **Hours** | Estimate in hours (not S/M/L) — solo remediation is hour-scale. |
| **Verified** | Yes / Needs Verification (+ how to verify in one line) |

### Severity rubric for this project

- **Critical** — Pipeline silently produces wrong data that Victor would act on, OR would require ≥ 1 hour of 3am debugging, OR credentials exposure. Every Critical becomes an R12 item.
- **High** — Real accuracy/data-quality regression Victor would act on within a week. Or an operational hazard (stream timeout, rate-limit blown budget).
- **Medium** — Operational friction that costs working time (dev loop too slow, unclear error, fragile test).
- **Low** — Hygiene, consistency, or would-be-nice. Bundle into a single hygiene sweep.

**An audit without a severity distribution that bunches at Low is probably fabricating.** A solo side-project after 11 rounds of R-audits should have 0-2 Criticals at most. If you find 5, go back and re-read what Critical means here.

---

## Step 5 — Anti-Fabrication Discipline

Load-bearing.

1. **Evidence or strike.** Every finding needs a snippet, test output, or command trace. "I think..." or "likely..." without evidence is struck.
2. **"This is fine" counts.** If you deep-read `src/db.py` and the R11 upsert logic holds up, say so — one or two sentences on why. A short honest list beats a padded one.
3. **Mark uncertainty.** If you're not sure, stamp "Needs Verification" + exactly how to verify. Don't guess.
4. **Don't recycle linter output as findings.** There's no linter anyway — but don't invent lint-grade complaints from code style.
5. **Coverage transparency.** At the end, list which source files you actually opened. The codebase is small (~40 Python files, 1 PHP file). "I read 60% of the non-target files" with a list is honest; "I read most of it" is not.
6. **Don't repeat lessons.md.** If something is already documented in `tasks/lessons.md` as a known pattern the project follows, don't re-flag it. Reference the existing lesson instead.
7. **Cross-check against CLAUDE.md.** When a finding concerns a rule listed in CLAUDE.md, cite the rule (`CLAUDE.md: "Word-boundary regex for ALL keyword matching..."`) so the fix can just conform.

---

## Step 6 — Final Deliverable

Write to `audit/report.md`:

### 1. Engagement Context (copy from above, acknowledge you read it)
One paragraph confirming you're auditing as the solo-maintainer post-R11 health check, not as a due-diligence review.

### 2. Executive Summary
Top 3-5 risks ranked by expected harm to Victor's job search. One paragraph each: what, consequence, recommended action, hours to fix.

### 3. Full Findings
Table from Step 4. Sort by severity DESC, then by lens.

### 4. R12 Remediation Plan
Victor tracks work in R-rounds. Produce a sequenced R12 plan structured as checkable items the same shape as existing `tasks/todo.md` R-sections. Sequence by:
1. All Critical findings (regardless of hours)
2. High + ≤ 2 hours each (quick risk reduction batch — a Saturday)
3. High + larger structural work (one per weekend)
4. Grouped Medium findings where one pass touches related code
5. Low bundle as a hygiene sweep (one evening)

Be blunt about what won't fit this month given weekend-scale capacity.

### 5. Accepted Risks
Required section. Examples of what belongs here for a solo personal tool:
- Findings whose fix cost exceeds expected harm for hobby-scale stakes
- "Proper" tooling (linter, type checker) whose setup cost isn't justified by the bug rate
- Security findings mitigated by the fact that nothing interesting runs here
- Scaling concerns that only matter at 10x+ volume

If this list is empty, you haven't thought hard enough.

### 6. What I Could Not Assess
Specific files not read, modules whose behavior requires a live system to verify, areas where the threat envelope from Step 2 made deeper review pointless.

### 7. Metrics
- Findings by severity / lens
- Deep-audit targets vs. surface-pass modules, by name
- **Files actually opened (count + list)** — the codebase has ~45 Python files + 1 PHP file + config; a full list is expected
- Total remediation hours (sum of finding estimates)
- Number of findings struck during Anti-Fabrication pass (if any)

---

## The Single Discipline to Hold

The purpose is to help Victor decide what to do with his next weekend — not to produce a long list, not to demonstrate thoroughness, not to rank every file.

**If a finding wouldn't change what Victor does this Saturday, it probably doesn't belong in the main findings table** — it goes in the Low-bundle hygiene sweep or gets struck entirely.

Read the code. Think hard. Write it down honestly. Stop when you're done.
