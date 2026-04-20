# Job Monitor — Audit Findings

**Audit date:** 2026-04-20
**Auditor:** Claude (Opus 4.7, 1M context)
**Scope:** Post-R11 health check per `audit/audit_prompt_jobmonitor.md`

---

## Signal Report

| Signal | Value |
|---|---|
| pytest | **845 passed** in 263s (~4:23) |
| Code volume | 18,124 LOC across 65 Python/PHP/YAML files |
| `except Exception` clauses | 64, all with `noqa: BLE001` (intentional) |
| TODO / FIXME / XXX / HACK markers | **0** in `src/` and `wordpress/` |
| Churn concentration (180d) | `wordpress/job-monitor.php` (1777 LoC changed), `src/processors/enrichment.py` (922), `src/db.py` (921), `src/collector.py` (833), `src/processors/keyword_filter.py` (560) |
| Linter / type checker | **none configured** — noted as F-008 |
| SQL injection scan | Clean — 3 f-string SQL callsites interpolate hardcoded column names (`_JOB_COLUMNS`), never user input |
| eval/exec scan | Clean |
| README↔code drift | README claims "163 tests" — reality is 845. F-009 |
| lessons.md | **Empty.** CLAUDE.md requires lessons after every correction; 11 R-rounds shipped without one entry. F-010 |
| GitHub Actions workflow timeout | 20 minutes |
| Dependency pinning | `>=` minimums only; no hash pinning; no vulnerability scanner |

The R11 history (todo.md bottom) records every major fix that shipped. This audit will not re-surface any of them as findings.

---

## Deep-audit targets (confirmed, not revised)

- `src/db.py` — state of record
- `src/processors/keyword_filter.py` — CLAUDE.md's zero-false-positive gate
- `src/processors/llm_classifier.py` — fallback chain
- `src/processors/deduplicator.py` — composite match + R11 consensus
- `src/processors/enrichment.py` — network, guardrails
- `src/collector.py` — orchestrator
- `wordpress/job-monitor.php` — user-facing UI; desync bug history

## Threat Envelope

At risk: the seven secrets in GitHub Actions (Turso token, four LLM keys, one RapidAPI key, one WP app password). Attack surface is the WP REST endpoint (Basic Auth + optional `X-JM-Secret`) and the aggregator-API response bodies we parse. Not at risk: user PII (no users), real money, service uptime (cron job), downstream consumers.

Supply-chain risk exists (12 deps with `>=` pins on `requirements.txt`), but the GH Actions environment is ephemeral and runs a single script — the blast radius of a malicious dep release is bounded to whatever one workflow run can do with those secrets.

---

## Findings

_Entries are added in the order they were discovered. Ordering in the final report.md will re-sort by severity._

---

### F-001 — JSearch quota exhaustion has no active alerting path

| | |
|---|---|
| **Severity** | Medium |
| **Lenses** | C |
| **Where** | `src/sources/jsearch.py:111-114`; `src/collector.py:144-145` |

**What.** JSearch is the primary source (≈200 req/month cap). When quota drops below 40, `jsearch.py` emits a `log.warning` and that's it — the warning rides in the workflow log nobody reads. The zero-results canary only fires when *every* source returns 0. If JSearch hits quota mid-month, it silently returns empty results for the rest of the month while the other sources carry the batch; Victor finds out when JSearch comes back online in the new billing cycle.

**Evidence.**
```python
# src/sources/jsearch.py:111-114
if quota_remaining is not None:
    log.info("jsearch quota remaining: %d", quota_remaining)
    if quota_remaining < 40:
        log.warning("jsearch quota LOW: %d requests remaining", quota_remaining)
```
No Pushover or Healthchecks `/fail` fires on low quota anywhere else in the codebase (`grep -r "quota" src/` — only the log line + meta propagation).

**Consequence.** Primary source degrades silently for up to 30 days. Victor loses the single best Google-for-Jobs signal without knowing.

**Fix.** In `collector.py` right after sources finish, if `meta["jsearch_quota_remaining"] < 40`, POST a Pushover alert (same pattern as `_alert_zero_results`). Threshold 40 is already documented in `CLAUDE.md` ("Track quota and alert at 80%").

**Hours.** 0.5
**Verified.** Yes (read both files, confirmed alerting absent).

---

### F-002 — `tasks/lessons.md` is empty despite 11 rounds of corrections

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | D |
| **Where** | `tasks/lessons.md` |

**What.** `CLAUDE.md` declares: "After ANY correction: update tasks/lessons.md with the pattern. Format: ALWAYS/NEVER + concrete rule + why. Review lessons at session start." R-rounds 1 through 11 shipped — `tasks/todo.md` records the work — but `lessons.md` is the unfilled template.

**Evidence.** `cat tasks/lessons.md` → boilerplate + "_No lessons yet._"

**Consequence.** Durable knowledge from 11 audit cycles lives only in commit messages. A future assistant (or future-Victor) reinvents decisions. Examples that *should* be in lessons: "ALWAYS preserve first_seen_date on WP UPDATE — R11 Phase 0 incident"; "NEVER overwrite non-None fields with None in upsert — R11"; "ALWAYS check circuit breaker before HTTP in enrichment — R11 Phase 5"; "NEVER use exact-match AGGREGATOR_HOSTS — always is_aggregator_host — R9-Part-2 / R10".

**Fix.** Back-fill 6-10 high-value lessons from the R-log in `tasks/todo.md`. ~15 min per lesson if you already know the story.

**Hours.** 1-2
**Verified.** Yes.

---

### F-003 — README claims "163 tests"; reality is 845

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | D |
| **Where** | `README.md` (near `python -m pytest tests/ -v`) |

**What.** README describes a test suite five times smaller than what exists.

**Evidence.**
```
# README.md
python -m pytest tests/ -v            # 163 tests, all with mocked I/O

# pytest --co -q | tail -3
# 845 tests collected in 0.39s
```

**Consequence.** New reader gets a wildly misleading sense of scope. Low actual harm.

**Fix.** Drop the count or replace with "see `tests/` (800+ tests, all with mocked I/O)".

**Hours.** 0.1
**Verified.** Yes.

---

### F-004 — No linter or type checker configured

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | D |
| **Where** | repo root (no `pyproject.toml`, `.ruff.toml`, `mypy.ini`, `setup.cfg`) |

**What.** No static analysis. The project has a type-hint convention (observed in every module) but nothing enforces it. During the R11 work I changed `apply_enrichment`'s return type from `dict[str, int]` to `dict[str, Any]` to carry `circuit_breaker` snapshots; the existing test caught the dict-equality assertion failure, but a type checker would have caught the signature change at edit-time, not run-time.

**Evidence.** `ls pyproject.toml ruff.toml .ruff.toml mypy.ini 2>&1` → no matches.

**Consequence.** Subtle type errors and dead imports accumulate. Low severity because tests catch most real bugs, but `ruff check --fix` is 10 minutes of cleanup that pays back over time.

**Fix.** Add `ruff check` + `ruff format --check` to CI. Defer `mypy` — would be loud on this codebase without substantial annotation work.

**Hours.** 1 (config + first cleanup pass)
**Verified.** Yes.

---

### F-005 — Dependencies use `>=` floors; no hash pinning; no vulnerability scanner

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | B |
| **Where** | `requirements.txt`; `.github/workflows/collect.yml` |

**What.** `requirements.txt` pins all 12 deps as `>=` minimums. No `requirements.lock`, no `--require-hashes`, no `pip-audit` step. A malicious release of any pinned dep (e.g., `feedparser`, `libsql`, `jobspy`) would run during the daily workflow with all 26 GitHub Actions secrets in the environment.

**Evidence.**
```
# requirements.txt
requests>=2.31.0
rapidfuzz>=3.14.0
google-genai>=1.70.0
...
```
Blast radius per run: ≤ 20 min of compute with Turso write token + LLM keys + WP app password + Pushover token. Ephemeral runner, so no persistent footprint, but one run is enough to e.g. drop arbitrary rows into `jobs` or flood Pushover.

**Consequence.** Stated supply-chain risk. Realistic probability is low for well-known deps, higher for newer ones (`python-jobspy>=1.1.0` is the newest).

**Fix.** Add `pip-audit` step in the workflow; optionally `pip freeze > requirements.lock` and use `pip install -r requirements.lock` in CI.

**Hours.** 1
**Verified.** Yes.

---

### F-006 — `_extract_body_redirect` scans only first 32KB

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | A |
| **Where** | `src/processors/enrichment.py:71` |

**What.** The scope for meta-refresh + JS redirect patterns is capped at the first 32KB of the body. Most aggregator pages put the redirect near the top, but SPA-style pages that inline 30KB+ of CSS before the `window.location` call would have their redirect miss the scope.

**Evidence.** `scope = html[:32000]` — regex runs on the truncated string.

**Consequence.** Niche. Specific to SPA-rendered aggregator pages. If triggered: apply_url stays on the aggregator, user clicks land on the aggregator.

**Fix.** Increase to 64KB, OR seek to `<body` tag before scanning. The latter is more targeted.

**Hours.** 0.5
**Verified.** Yes (code read). Needs Verification of prevalence: no real-page sample exceeds 32KB pre-redirect in the logs I've seen.

---

### F-007 — Test suite wall-time is ~4:23, dominated by a small number of slow tests

| | |
|---|---|
| **Severity** | Medium |
| **Lenses** | D |
| **Where** | test suite globally |

**What.** `pytest -q` takes 263 seconds for 845 tests. The per-test average is ~300ms, but variance is uneven — several test modules appear to sleep or retry in ways that mocks could avoid.

**Evidence.**
```
# pytest -q (full suite)
845 passed, 1 warning in 263.14s (0:04:23)
```
Iteration cost: every time Victor's TDD loop wants a full-suite green, he waits 4-5 minutes. For a solo, weekend-driven project this is the most expensive recurring friction.

**Consequence.** Encourages shortcut runs that miss cross-file regressions (exactly what happened during the R11 session — I ran targeted subsets and only discovered two existing-test breakages on the final full run).

**Fix.** `pytest --durations=20` once; fix the top five. Suspected culprits: anything in `test_llm_classifier.py` that sleeps between mocked provider calls, anything in `test_enrichment.py` that exercises `time.sleep` in throttles. The sleeps are usually monkeypatchable with `monkeypatch.setattr("time.sleep", lambda x: None)`.

**Hours.** 2-3
**Verified.** Yes (time value). Needs Verification of specific culprits via `--durations`.

---

### F-008 — Per-run enrichment guardrails are module-level singletons; test isolation relies on explicit `_reset_guardrails()`

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | A |
| **Where** | `src/processors/enrichment.py:613-618`; `tests/test_enrichment_guardrails.py:13-18` |

**What.** R11 Phase 5 introduced `_circuit_breaker` and `_fetch_budget` at module scope. `test_enrichment_guardrails.py` has an `@pytest.fixture(autouse=True)` that resets them. Any new test file that exercises `enrichment.enrich_job` without importing/using that fixture inherits whatever state the last test left.

**Evidence.** Module-level:
```python
# enrichment.py
_circuit_breaker = _CircuitBreaker()
_fetch_budget = _FetchBudget()
```
The autouse fixture is file-scoped to `test_enrichment_guardrails.py` only.

**Consequence.** Low today — current tests pass. Becomes a Medium the moment someone adds a new test that incidentally calls enrich_job after the budget was exhausted by a prior test.

**Fix.** Move the reset fixture to `tests/conftest.py` so every test file gets it automatically. One-line file.

**Hours.** 0.3
**Verified.** Yes.

---

### F-009 — Consensus override threshold (0.65) and source-reliability priors are educated guesses, not calibrated

| | |
|---|---|
| **Severity** | Medium |
| **Lenses** | A |
| **Where** | `src/processors/deduplicator.py:97` (`_CONSENSUS_OVERRIDE_MIN_CONF`); `src/shared.py` (`SOURCE_RELIABILITY`) |

**What.** R11 Phase 3's voting depends on two knobs whose values are unvalidated:
- `_CONSENSUS_OVERRIDE_MIN_CONF = 0.65` — below this, the consensus winner doesn't overwrite the source's flat value
- `SOURCE_RELIABILITY` priors (greenhouse=0.90, jsearch=0.55, jooble=0.50, text_classifier=0.75, schema_org=0.85) — guesses from reasoning, not from labeled data

**Evidence.** Both are hardcoded constants. No fixture tests assert that specific input patterns produce a specific winner. No shadow-log analysis has been done against real historical data to validate that e.g. jsearch's 0.55 is too high or too low.

**Consequence.** R11's whole purpose is data accuracy. If the priors are wrong, consensus can promote noise over signal (too high prior on text_classifier could flip correct aggregator values; too low and aggregator noise wins). This is the highest-value test gap of the R11 shipping package.

**Fix.** Two-track:
1. Short term: add explicit test fixtures in `test_consensus.py` using real-shape observations from recent shadow log, asserting the winner matches intuition. If any surprise, re-tune.
2. Medium term: instrument — every consensus override logs `{before, after, sources, confidence}` to shadow log for 2 weeks; hand-label ~100; tune priors from actual error rate.

**Hours.** 3 (short-term), +3 (medium-term calibration after data accrues)
**Verified.** Needs Verification — specifically, whether current priors produce correct winners on real data.

---

### F-010 — Workflow's `GITHUB_TOKEN` has default permissions; not scoped

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | B |
| **Where** | `.github/workflows/collect.yml` |

**What.** No `permissions:` block. Default GITHUB_TOKEN gets broader rights than needed — this workflow only reads the repo (checkout), it never needs write.

**Evidence.**
```yaml
# .github/workflows/collect.yml
name: Collect Jobs
on:
  schedule: ...
jobs:
  collect:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    # ← no `permissions:` block
```

**Consequence.** Smaller concern given the secrets already in the env (Turso, WP, LLM keys) are the real attack targets and they're distinct from GITHUB_TOKEN. Still, principle of least privilege.

**Fix.**
```yaml
permissions:
  contents: read
```
at the top of the workflow.

**Hours.** 0.1
**Verified.** Yes.

---

### F-011 — R11 schema.org extraction fires rarely; most jobs never get a schema_org observation

| | |
|---|---|
| **Severity** | Medium |
| **Lenses** | A |
| **Where** | `src/processors/enrichment.py:444-456` |

**What.** Phase 4's `schema_org.apply_to_job(job, resp.text)` runs only inside `enrich_job` after a successful GET on a non-aggregator `orig_host`. For an aggregator URL (Jooble, Adzuna) where the body contains a redirect target, we rewrite `apply_url` but don't re-fetch the target URL. So the page we actually parse for JSON-LD is the aggregator's, which never has `JobPosting` markup.

**Evidence.**
```python
# enrichment.py around line 446
if not is_aggregator_host(orig_host):
    try:
        n = schema_org.apply_to_job(job, resp.text or "")
```
When `orig_host` IS an aggregator (the majority of Jooble/Adzuna/JSearch jobs), the guard short-circuits even when we've already learned the canonical target URL from body-redirect extraction.

**Consequence.** Schema.org observations — the highest-reliability source at 0.85 in `SOURCE_RELIABILITY` — land on roughly the subset of jobs that arrive on direct ATS URLs (Greenhouse/Lever/Ashby), where we already have canonical data from the ATS API. The feature underperforms on the source set that needed it most.

**Fix.** After `_extract_body_redirect` rewrites `apply_url`, if the new host is non-aggregator and we still have fetch budget, issue a follow-up GET and run `schema_org.apply_to_job` on that body. Gate with the existing `_fetch_budget` / `_circuit_breaker`. This is the "canonical fetcher" promise from the original R11 proposal, deliberately deferred to manage scope.

**Hours.** 3-5 (including tests)
**Verified.** Yes (code path read).
**Note.** Already documented in `tasks/todo.md` R11 "Not done (deferred)" section.

---

### F-012 — Pipeline ordering constraint has no automated enforcement

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | A / D |
| **Where** | `src/collector.py:run()`; no test file |

**What.** `CLAUDE.md` declares: "collector.py MUST execute in this exact order: (1) sources → (2) keyword_filter → (3) llm_classifier → (4) deduplicator → (5) wordpress publisher → (6) notifier → (7) archiver → (8) healthcheck ping. Reordering will cause data integrity bugs." The constraint is enforced solely by the shape of the code in `run()`. No test would catch a reordering mistake before it shipped.

**Evidence.** `grep -rn "keyword_filter\|llm_classifier\|deduplicat" tests/ | grep -v "test_keyword_filter\|test_llm_classifier\|test_deduplicator"` — no cross-phase test.

**Consequence.** Someone refactoring `run()` to "simplify" or parallelize phases could silently break the invariant; the suite would pass because each unit test mocks its neighbors.

**Fix.** One smoke test that monkeypatches each phase's entrypoint to record the call, runs `run()` with a stub, and asserts the recorded order.

**Hours.** 0.5
**Verified.** Yes.

---

### F-013 — `/stats` REST endpoint is public (`__return_true`)

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | B |
| **Where** | `wordpress/job-monitor.php:114` |

**What.** The `/wp-json/jobmonitor/v1/stats` endpoint is unauthenticated (returns active + archived post counts + `last_updated` timestamp).

**Evidence.**
```php
register_rest_route('jobmonitor/v1', '/stats', [
    'methods' => 'GET',
    'callback' => 'jm_get_stats',
    'permission_callback' => '__return_true',
]);
```

**Consequence.** Public data: how many posts the plugin has published and when the last run was. Not sensitive for this project (the WP site itself publishes the same info via shortcodes). Zero real stakes. Included only because "public REST endpoint" pattern-matches a concern in most audits.

**Fix.** Leave it. Explicit Accepted Risk.

**Hours.** 0
**Verified.** Yes.

---

### F-014 — Zero-results canary doesn't alert on per-source failure

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | C |
| **Where** | `src/collector.py` (`_alert_zero_results`) |

**What.** The canary fires only when *every* source returns 0. A permanently broken Jooble integration that hits 0 every day while Greenhouse happily returns 300 stays invisible.

**Evidence.** `grep -n "zero_results\|consecutive_zero" src/collector.py` — only the all-source check.

**Consequence.** Paired with F-001, this is the "one source dies silently" blind spot. JSearch quota (F-001) is one instance; Jooble 403s observed in logs is another.

**Fix.** For each critical source (jsearch, greenhouse, lever, ashby), track consecutive-zero-runs per-source in Turso. Alert when any critical source hits 3+ consecutive zeros.

**Hours.** 1-1.5
**Verified.** Yes (code read).

---

### F-015 — `except Exception as e: # noqa: BLE001` appears 64 times; all logged, but the pattern is large

| | |
|---|---|
| **Severity** | Low |
| **Lenses** | A |
| **Where** | `src/` globally (count: 64) |

**What.** Every catch-all is documented with `noqa: BLE001` and a trailing comment explaining why. Spot-checked 10 — all have a `log.warning` or `log.exception` with context. None are silent. This is a stylistic observation, not a defect.

**Evidence.** `grep -rn "except Exception" src/ | wc -l` → 64. Sample: `src/collector.py:265 except Exception as e:  # noqa: BLE001 — one bad record can't kill the batch`.

**Consequence.** None material. Recording the count so a future audit has a baseline — if this grows to 150 without equivalent justification, revisit.

**Fix.** No change. Accepted Risk.

**Hours.** 0
**Verified.** Yes.

---

## "This is fine" — deep-read modules that hold up

- **`src/processors/keyword_filter.py`.** Word-boundary regex everywhere (`_compile_terms`), zero-false-positive path preserved, conflict-routes-to-LLM is intact, R8-shadow-B2 self-mention suppression and R8-shadow-B3 vendor-desc cap both survive the audit. The gating density (B5/B7/B8/CMC/XM_SCIENTIST/T2_TITLE_GATES) is high but each gate is justified by a documented incident. No finding.
- **`src/processors/llm_classifier.py`.** The fallback chain is sound — every provider call is wrapped, errors get `_sanitize_err` redacting Bearer/Authorization/api_key patterns before logging, confidence is validated + clamped, `_keyword_fallback` tunes confidences to route correctly through `publish_decision`. The dynamic per-provider delay pacing (L362-367) is the kind of subtle correctness work that usually breaks and here is careful. No finding.
- **`src/db.py` (R11 upsert_job).** The R11 Phase 0 preservation rules (first_seen_date fixed on insert; non-None-only UPDATE; earliest-wins date_posted; `_is_brand_new` side-effect) are correctly implemented and tested. The `_AutoReconnectConnection` wrapper handles Turso stream expiration at both `execute()` and `commit()` layers. `_execute_with_retry` adds one more retry on top. Layered correctly. No finding.
- **`.github/workflows/collect.yml`.** `persist-credentials: false`, 20-minute timeout, explicit concurrency group, Healthchecks ping on both success and failure paths, artifact upload for shadow_log regardless of outcome. Clean. Only nit is F-010 permissions block.

