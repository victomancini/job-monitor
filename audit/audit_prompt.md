# Project Audit — Consensus Prompt

You are auditing this codebase. The goal is an honest, evidence-backed assessment that leads to action. Not a checklist performance. Not a fabricated gotcha list. Not a greatest-hits tour of every lint rule ever written. A real read, with real judgment, producing a real plan.

---

## Step 0 — Scope the Engagement (do not skip)

Before reading any code, ask me (the user) for the following. If I have not provided it, stop and request it.

1. **Audit purpose.** Pre-launch review, post-incident review, due diligence, team hand-off, general health check, or other?
2. **Audience for the report.** Engineers on the team, incoming team, exec leadership, external reviewer?
3. **Stakes.** What does this system do and what breaks when it fails? (data loss, money, trust, downtime, regulatory exposure)
4. **Capacity.** Rough team size and how much time is realistically available for remediation.
5. **Known pain.** Any modules, bugs, or areas you already suspect. The oral tradition of the team.
6. **Access.** What can you actually run — tests, linters, a local instance, a staging env? What's off-limits?

Use these answers to calibrate depth, severity thresholds, and what to include vs. defer. A hobby project and a trading system get different audits. Say so explicitly at the top of your final report.

---

## Step 1 — Signal Gathering (before deep reads)

Spend a bounded amount of effort gathering cheap signal that will direct the deep work.

- **Repo topology.** Languages, frameworks, build system, test runner, deploy target. Top-level modules and entry points.
- **Tooling output.** Run the linter, type checker, test suite, and any dependency-vulnerability scanner available. Capture outputs verbatim. These count as findings; do not re-derive them by hand later.
- **Churn.** Last 90–180 days of git history. Which files/modules change constantly? Churn concentrates risk.
- **Operational signal, if it exists.** Postmortems, incident logs, on-call runbooks, alert volume, SLO dashboards. If none exist and this is a production system, that is itself a Critical finding — flag it and move on.
- **Docs reality check.** Does the README match what the code does? Gaps and lies in docs are a tell.

Produce a short **Signal Report** (½ to 1 page) before proceeding. This is what directs Steps 2 and 3.

---

## Step 2 — Criticality Map

From the signal report, identify the **3 to 6 modules where failure has the worst consequences** for this system — correctness, security, availability, or money. Justify each selection in one or two sentences.

These are your **deep-audit targets**. Everything else gets a surface pass.

For systems with meaningful security exposure (user data, payments, auth, multi-tenant, internet-facing), produce a brief **Threat Model** alongside the criticality map: assets, entry points, trust boundaries, plausible adversaries. If you judge a threat model unnecessary for this system, say why in one sentence.

---

## Step 3 — The Four Lenses

Apply each of the four lenses below to the deep-audit targets. Apply them at surface level to the rest. The lenses are cross-cutting, not siloed — a concurrency bug in auth is a Security finding *and* a Correctness finding; record it once, tag it both.

### Lens A — Correctness & Design
Does this code do what it claims, under the conditions it will actually meet? Look for: tangled module boundaries, cyclic deps, hidden global state, unsafe concurrency, resource leaks, boundary and null-path bugs, type-safety gaps, error-swallowing, duplicated logic, dead code, tests that assert nothing real, missing failure-case coverage, fixtures that don't reflect production shapes.

### Lens B — Security & Data Flow
Trace inputs to sinks. Every trust boundary: is input validated? Every sensitive operation: is it authorized? Injection vectors (SQL, command, template, path, SSRF, XSS). Secrets in code, config, logs, fixtures. Crypto misuse. AuthN/AuthZ gaps. PII handling and log hygiene. Supply-chain risk (unpinned deps, install scripts, abandoned libraries). Evaluate findings against the threat model from Step 2.

### Lens C — Reliability & Operations
How does this behave when things go wrong? Timeouts on every external call. Retry safety and idempotency. Circuit breakers and backpressure. Graceful shutdown. Logging that helps at 3am. Metrics, tracing, health checks. Migration reversibility and zero-downtime safety. Rollback story. Deploy and config-drift risk. What breaks first at 10x load. Cost anomalies.

### Lens D — Maintainability & Human Cost
What does this codebase do to the humans working on it? Clone-to-running time. Feedback loop speed (build, test, lint). CI coverage and enforcement. Onboarding footguns. Docs that mislead. Inconsistent conventions. Dependencies doing too much or too little. Technical debt the team has learned to step around.

---

## Step 4 — Findings Format

Persist findings to a file as you generate them (`findings.md` or equivalent). Do not accumulate everything in context and dump at the end — that's how findings get lost or fabricated.

Each finding carries these fields:

| Field | Notes |
|---|---|
| **ID** | F-001, F-002, … |
| **Severity** | Critical / High / Medium / Low |
| **Lenses** | A, B, C, D (may be multiple) |
| **Where** | Exact `file:line`. No vague module references. |
| **What** | One-sentence problem statement |
| **Evidence** | The snippet, test run output, or trace that proves it |
| **Consequence** | What happens (or could happen) if unfixed. Concrete, not abstract. |
| **Fix direction** | Not necessarily the full patch, but enough to start |
| **Effort** | S / M / L, calibrated to the team size provided in Step 0 |
| **Confidence** | Confirmed / Needs Verification (+ how to verify) |

**Severity rubric (calibrated to the stakes provided in Step 0):**
- **Critical** — Active risk of the worst-case consequence for this system's stakes.
- **High** — Serious risk that will likely manifest without intervention.
- **Medium** — Quality, resilience, or maintainability issue with meaningful cost.
- **Low** — Hygiene, small inconsistency, or nit.

---

## Step 5 — Anti-Fabrication Discipline

These rules are load-bearing. Do not skip them.

1. **Every finding needs evidence** — a snippet, a trace, a test output. Assertions without evidence get struck.
2. **"This is fine" is a valid finding.** If you deep-read a module and it holds up, say so explicitly with one or two sentences of why. A short, well-justified findings list beats a padded one.
3. **Uncertainty is a feature.** If you're not sure, mark Needs Verification and describe the test that would confirm. Do not guess.
4. **No recycling linter output as "findings."** Lint and type-checker output is captured in Step 1 and referenced. Don't pad the findings list with the first 40 warnings from ESLint.
5. **No coverage theater.** If you only read 30% of the non-target files, say so. Percentage-of-codebase-actually-read is a required metric in the final report.

---

## Step 6 — Final Deliverable

Produce these sections, in this order:

### 1. Engagement Context
Repeat back the purpose, audience, stakes, capacity, and access assumptions from Step 0. One short paragraph. This is how the reader knows whether the audit applies to their situation.

### 2. Executive Summary
Top 3–5 risks ranked by expected harm. One paragraph each: what, consequence, recommended action, rough effort.

### 3. Full Findings
The table from Step 4, sortable by severity, lens, or area.

### 4. Remediation Plan
Sequenced workstreams, each with: goal, findings addressed (by ID), order of operations, dependencies, risk of doing nothing. Sequence by the following priority:
1. All Critical findings, regardless of effort.
2. High + Small-effort (quick risk reduction).
3. High + larger structural work.
4. Grouped Medium findings where one pass touches related code.
5. Low/nit bundle as a hygiene sweep.

Calibrate workstream sizing to the team capacity given in Step 0.

### 5. Accepted Risks
Findings that the team should consciously *not fix*, with justification. This is a required section. An audit without an accepted-risks list overstates obligation and misses the point of prioritization. Examples: low-severity issues in deprecated modules, High-severity issues where mitigation exists at another layer, issues whose fix cost exceeds their expected harm.

### 6. What I Could Not Assess
Areas where you lacked access, tooling, domain context, or time. Be specific about what would unblock a follow-up pass.

### 7. Metrics
- Findings by severity and lens
- Deep-audit targets vs. surface-pass modules (with names)
- Percentage of codebase actually read — honest number
- Estimated total remediation effort, in S/M/L counts

---

## One Discipline to Hold Throughout

The purpose of this audit is to help a team make better decisions about their codebase. It is not to produce a long list. It is not to demonstrate thoroughness. It is not to rank every file on a 10-point scale. If a finding wouldn't change what someone does on Monday, it probably doesn't belong in the report — or belongs in the nit bundle, not the main findings table.

Read the code. Think hard. Write it down honestly. Stop when you're done.
