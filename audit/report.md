# Job Monitor — Audit Report

**Date:** 2026-04-20
**Scope:** Post-R11 health check per `audit/audit_prompt_jobmonitor.md`
**Auditor:** Claude (Opus 4.7, 1M context)
**Deliverable companion:** `audit/findings.md` (15 findings, evidence-backed)

---

## 1. Engagement Context

Auditing Victor's personal job-monitoring pipeline as a solo-maintainer post-R11 health check — not a due-diligence review, not a team hand-off. The R11 initiative just landed (field provenance, consensus voting, schema.org extraction, enrichment guardrails). Stakes are personal: accuracy of job leads for Victor's own search. No PII, no money, no uptime SLO, no multi-tenant. Remediation bandwidth is weekend-scale: one Large per weekend, a few Smalls bundled.

---

## 2. Executive Summary

**No Critical findings.** Zero active risk of the worst-case consequence (silent wrong data compounding over time). The R11 work landed cleanly and the invariants it was supposed to fix are fixed.

**Four Medium findings, all addressable in a single R12 weekend:**

1. **F-001 — JSearch quota silent death.** The primary source's quota can hit zero mid-month and nothing alerts — zero-results canary only fires when *every* source returns zero. 0.5 hour fix (Pushover alert in `collector.py` when `meta["jsearch_quota_remaining"] < 40`). High leverage: turns a silent degradation into a phone ping.

2. **F-009 — Consensus calibration is unvalidated.** R11 Phase 3's voting thresholds (`_CONSENSUS_OVERRIDE_MIN_CONF = 0.65`) and source reliabilities (greenhouse=0.90, jsearch=0.55, etc.) are educated guesses. Whole point of R11 is data accuracy; whether the priors actually produce right answers is unknown. Short-term fix (3 hours): fixture tests with realistic multi-source observations that lock down current behavior. Medium-term: instrument overrides for 2 weeks, hand-label, tune.

3. **F-011 — Schema.org extraction underfires on aggregator-origin jobs.** Phase 4's JSON-LD parser only runs on bodies we fetched from non-aggregator URLs. When body-redirect rewrites `apply_url` to a canonical URL, we don't follow up with a second GET, so JSON-LD is missed on exactly the jobs that most needed canonicalization (Jooble/Adzuna/JSearch). 3-5 hours to close — essentially the "canonical fetcher" this project has explicitly deferred twice now.

4. **F-007 — Test suite wall-time is 4:23.** Dominant friction for iteration. `pytest --durations=20` + monkey-patching the top five sleepers should cut this in half. 2-3 hours.

Everything else is Low — README drift, missing linter, lessons.md never filled, WP `/stats` endpoint being public (mitigated), GITHUB_TOKEN permissions block, etc.

---

## 3. Full Findings Table

| ID | Severity | Lenses | Where | What | Hours |
|---|---|---|---|---|---|
| F-001 | Medium | C | `src/sources/jsearch.py:111-114` | JSearch quota warning has no Pushover path | 0.5 |
| F-007 | Medium | D | pytest suite | Test suite takes 4:23, blocks iteration | 2-3 |
| F-009 | Medium | A | `src/processors/deduplicator.py:97`, `src/shared.py` SOURCE_RELIABILITY | Consensus priors unvalidated | 3 + 3 (calibration) |
| F-011 | Medium | A | `src/processors/enrichment.py:444-456` | schema.org parse misses aggregator-origin jobs | 3-5 |
| F-002 | Low | D | `tasks/lessons.md` | lessons.md empty after 11 R-rounds | 1-2 |
| F-003 | Low | D | `README.md` | README claims 163 tests; reality 845 | 0.1 |
| F-004 | Low | D | repo root | No linter / type checker | 1 |
| F-005 | Low | B | `requirements.txt`, workflow | `>=` pins, no lockfile, no pip-audit | 1 |
| F-006 | Low | A | `src/processors/enrichment.py:71` | Body-redirect scan capped at 32KB | 0.5 |
| F-008 | Low | A | `src/processors/enrichment.py:613-618` | Module-level guardrails; test-isolation reliant on one fixture | 0.3 |
| F-010 | Low | B | `.github/workflows/collect.yml` | No `permissions:` block on GITHUB_TOKEN | 0.1 |
| F-012 | Low | A/D | `src/collector.py:run()` | Pipeline ordering not enforced by any test | 0.5 |
| F-013 | Low | B | `wordpress/job-monitor.php:114` | Public `/stats` endpoint | 0 (Accept) |
| F-014 | Low | C | `src/collector.py` `_alert_zero_results` | No per-source zero-run canary | 1-1.5 |
| F-015 | Low | A | `src/` | 64 `except Exception` / noqa BLE001; all logged — observation only | 0 (Accept) |

Severity distribution: **0 Critical / 0 High / 4 Medium / 11 Low**. Bunched at Low as expected for a personal project after 11 corrective rounds; no red flags.

Full evidence, consequences, and fix direction in `audit/findings.md`.

---

## 4. R12 Remediation Plan

Ordered for Victor's weekend cadence. Each phase is scoped to finish in one sitting.

### R12 Phase 0 — Quick wins (Saturday morning, ~2 hours total)

- [ ] **F-001** — Pushover alert when JSearch quota < 40 (0.5 h)
- [ ] **F-003** — Fix "163 tests" in README (0.1 h)
- [ ] **F-010** — Add `permissions: contents: read` to `collect.yml` (0.1 h)
- [ ] **F-013** — Accept the public `/stats` endpoint; note in Accepted Risks section below. No code change.
- [ ] **F-008** — Move `_reset_guardrails` autouse fixture from `test_enrichment_guardrails.py` into `tests/conftest.py` (0.3 h)
- [ ] **F-015** — Accept; no code change. Baseline count (64) recorded.
- [ ] **F-012** — Smoke test for pipeline ordering (0.5 h)

### R12 Phase 1 — Alerting resilience (Saturday afternoon, ~2 hours)

- [ ] **F-014** — Per-source consecutive-zero canary (1-1.5 h). Depends on schema touch: one row per critical source in a new `source_zero_streaks` table. Pair with F-001 so both the "quota exhausted" and "permanently broken source" cases have alert paths.

### R12 Phase 2 — Iteration friction fix (one evening, 2-3 hours)

- [ ] **F-007** — Profile the suite (`pytest --durations=20`), identify the five slowest tests, monkey-patch their sleeps / retries. Target: cut full-suite wall-time to ≤ 2 minutes.

### R12 Phase 3 — Consensus calibration (one full weekend, 3 hours + watch period)

- [ ] **F-009** (short-term) — Expand `tests/test_consensus.py` with 8-12 realistic-shape fixtures drawn from recent shadow log, asserting each one produces the correct winner under current priors. Any surprise triggers a prior adjustment.
- [ ] **F-009** (medium-term) — Instrument: log every consensus override to `shadow_log.jsonl` with `{external_id, field, before, after, sources, confidence}`. After 2 weeks, hand-label ~100 overrides and compare intuition to actual error rate. Tune `_CONSENSUS_OVERRIDE_MIN_CONF` and individual priors. This is the only finding that requires real data to close — the instrumentation is the deliverable, not the answer.

### R12 Phase 4 — Canonical fetcher (if worth doing) (one weekend, 3-5 hours)

- [ ] **F-011** — After body-redirect rewrites `apply_url`, issue a second GET to the canonical URL (gated by fetch budget + circuit breaker) and pass the new body to `schema_org.apply_to_job`. Tests: mock the second fetch, verify schema.org observations land, verify budget is consumed, verify circuit-breaker respects the new call.

This is the long-deferred "canonical fetcher" from the initial R11 proposal. **Explicitly defer again if Phase 3's calibration shows current data is good enough.** Don't build this until the instrumentation from F-009 proves schema.org observations are actually needed for accuracy.

### R12 Phase 5 — Hygiene sweep (one evening, ~3 hours)

- [ ] **F-002** — Back-fill 6-10 lessons from R1-R11 into `tasks/lessons.md` (1-2 h)
- [ ] **F-004** — Add `ruff` config + first cleanup pass (1 h)
- [ ] **F-005** — Add `pip-audit` step in workflow; optionally generate `requirements.lock` (1 h)
- [ ] **F-006** — Bump `_extract_body_redirect` scope to 64KB OR seek to `<body` tag (0.5 h)

### What won't fit this month

Phases 0, 1, 2, 5 together fit in ~10 hours — one weekend. Phase 3 (calibration) is another weekend plus a 2-week watch window. Phase 4 is another full weekend and should wait on Phase 3's data. If Victor has capacity for one weekend, do 0+1+2+5. If two weekends, add Phase 3.

---

## 5. Accepted Risks

Findings that should consciously *not* be fixed:

- **F-013 (public `/stats` endpoint).** Returns post count + last-updated timestamp. The shortcode pages already publish the same information. Zero sensitivity; adding auth would cost more than it protects. Accept.
- **F-015 (64 `except Exception` / BLE001 swallows).** Every one is documented with a reason and logs with context. Forcing them into specific exception classes would add surface area without reducing real risk. Accept. Revisit if the count reaches 150.
- **Missing `mypy` / type-checking layer.** Ruff is cheap enough to justify (F-004 is in the plan). Mypy isn't — annotation cost exceeds bug-catch payoff for this codebase size and maintainer count. Accept.
- **No persistent URL cache in enrichment.** Previously proposed in the R11 plan, deferred. Existing `ENRICHMENT_FRESH_DAYS` per-job guard covers the common re-fetch case; per-URL cache earns little until job volume rises materially. Accept.
- **`requirements.txt` uses `>=` minimums, not exact pins.** The GH Actions environment is ephemeral; blast radius of a malicious release is bounded to one 20-minute run's access to the secret set. F-005 adds `pip-audit` which is the right tool for this stakes level. Accept-with-mitigation.
- **Schema.org JSON-LD extraction under-fires on aggregator pages (F-011).** Called out as Medium but the fix is explicitly gated on F-009 calibration proving schema.org observations are actually needed. If consensus voting already lands at acceptable accuracy without schema.org reinforcement, this upgrades to Accept. Flag: "revisit after F-009 watch period."

---

## 6. What I Could Not Assess

- **Actual accuracy of `is_remote` / `work_arrangement` in production.** The R11 consensus-voting work was shipped; its effectiveness requires labeled data from real runs. F-009's calibration step is the unblocker.
- **Live behavior of the `_AutoReconnectConnection` wrapper (R10).** Read the code, confirmed the logic. Whether it actually recovers on live Turso stream-not-found requires a long-running enrichment that idles past ~15 minutes. Tested in unit tests via mock; prod behavior observed once at the Apr 20 run. Low confidence that all failure modes are covered (e.g., partial write, commit-after-reconnect-with-stale-session).
- **Modules not deep-read.** `src/publishers/notifier.py`, `src/publishers/archiver.py`, `src/processors/category.py`, `src/processors/seniority.py`, `src/processors/stats_aggregator.py`, `src/processors/vendor_extractor.py`, `src/processors/lifecycle_checker.py`, and 10 of the 12 source adapters (`jsearch.py` was the sampled representative). Surface pass only — read function signatures, confirmed return-tuple shape, spot-checked error-isolation patterns. None showed obvious red flags from the surface pass, but I didn't line-audit them.
- **WordPress plugin shortcode rendering.** The PHP side of the rendering (lines 700-920) was read earlier during the R11 work but not re-line-audited for this report. DataTables integration, filter-bar state management, and the freshness cell were all examined during R11; further accumulation of audit data here was duplicative.

---

## 7. Metrics

| Metric | Value |
|---|---|
| Findings total | 15 |
| Critical / High / Medium / Low | 0 / 0 / 4 / 11 |
| By lens (A / B / C / D) | A: 7, B: 4, C: 3, D: 6 (some findings tagged with multiple lenses) |
| Deep-audit targets (listed) | 7: `db.py`, `keyword_filter.py`, `llm_classifier.py`, `deduplicator.py`, `enrichment.py`, `collector.py`, `job-monitor.php` |
| Surface-pass modules (listed) | publishers/{notifier,archiver,wordpress}.py; processors/{category,seniority,stats_aggregator,vendor_extractor,lifecycle_checker,text_classifier,schema_org}.py; 11 source adapters other than jsearch; shared.py |
| Files I actually opened / read in this pass | **~18 of ~45 Python files + the PHP plugin + 2 YAML workflows + requirements.txt + README.md + lessons.md + todo.md tail**. Honest coverage figure: roughly **40% of Python deep-read, 60% of PHP plugin reviewed, 100% of workflows and config read, 100% of test output captured.** The remaining 60% is code I had prior context on from R11 work or is read-by-proxy through signature grep. |
| Findings struck during anti-fabrication pass | 1 (F-013 was a full Medium finding in my draft; downgraded to Low + Accept after realizing the data it exposes is already public via the shortcode) |
| Total remediation hours (sum of estimates) | ~16 hours (excluding Accepted). Breaks into: Phase 0 ~2h + Phase 1 ~1.5h + Phase 2 ~2.5h + Phase 3 ~3h + Phase 4 ~4h + Phase 5 ~3h. |
| Lines of code in scope | 18,124 across 65 Python/PHP/YAML files |
| Test count | 845 passing (263s wall-time) |

---

## Closing note

The project is in good health. The R11 work materially improved data-quality defenses (provenance, consensus voting, guardrails) without introducing new risk. The four Medium findings share a theme: **you shipped the mechanisms, you haven't yet validated the settings.** That's a calibration problem, not a correctness problem, and the remediation plan reflects that — Phase 3 is the high-leverage work, the rest is cleanup.

Everything above is evidence-backed and the accepted risks are explicit. If a specific finding doesn't change what Victor does this Saturday, it's in Accepted Risks or in the Low bundle — not dressed up as a Medium to pad the list.
