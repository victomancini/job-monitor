"""R-audit Issue 2: lifecycle checker tests — ATS snapshot resolution,
direct-URL HEAD classification, aggregator fallback, freshness skip, rate limit."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src import db
from src.processors import lifecycle_checker as lc


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    db.migrate(c)
    yield c
    c.close()


def _mk_job(ext_id, source_name, apply_url, company="Co", title="People Analytics Manager"):
    return {
        "external_id": ext_id,
        "title": title,
        "company": company,
        "source_name": source_name,
        "apply_url": apply_url,
        "source_url": apply_url,
        "raw_data": None,
    }


# ──────────────────────── check_job_status — ATS category A ────────────────

def test_greenhouse_job_in_snapshot_returns_active():
    job = _mk_job("gh_cultureamp_12345", "greenhouse",
                  "https://boards.greenhouse.io/cultureamp/jobs/12345")
    snap = {("greenhouse", "cultureamp"): {"12345", "67890"}}
    assert lc.check_job_status(job, ats_snapshots=snap) == "active"


def test_greenhouse_job_missing_from_snapshot_returns_likely_closed():
    """R-audit: ATS API showed the job yesterday but not today → definitive
    signal it was filled/withdrawn. No HEAD check needed."""
    job = _mk_job("gh_cultureamp_12345", "greenhouse",
                  "https://boards.greenhouse.io/cultureamp/jobs/12345")
    snap = {("greenhouse", "cultureamp"): {"67890"}}  # 12345 is GONE
    assert lc.check_job_status(job, ats_snapshots=snap) == "likely_closed"


def test_lever_job_membership_resolution():
    job = _mk_job("lever_ramp_abc-123", "lever",
                  "https://jobs.lever.co/ramp/abc-123")
    snap_active = {("lever", "ramp"): {"abc-123"}}
    snap_closed = {("lever", "ramp"): {"other-id"}}
    assert lc.check_job_status(job, ats_snapshots=snap_active) == "active"
    assert lc.check_job_status(job, ats_snapshots=snap_closed) == "likely_closed"


def test_ashby_job_membership_resolution():
    job = _mk_job("ashby_notion_xyz-9", "ashby",
                  "https://jobs.ashbyhq.com/notion/xyz-9")
    snap = {("ashby", "notion"): {"xyz-9"}}
    assert lc.check_job_status(job, ats_snapshots=snap) == "active"


def test_ats_no_snapshot_falls_through_to_http():
    """When we have no snapshot for this slug (e.g., collector skipped the
    board this run due to the 404-cache), we fall through to HEAD check."""
    job = _mk_job("gh_unfetched_99", "greenhouse",
                  "https://boards.greenhouse.io/unfetched/jobs/99")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=200):
        assert lc.check_job_status(job, ats_snapshots={}) == "active"


# ──────────────────────── check_job_status — direct URL (cat B) ────────────

def test_direct_company_url_200_returns_active():
    job = _mk_job("jsearch_x", "jsearch", "https://careers.netflix.com/job/1")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=200):
        assert lc.check_job_status(job) == "active"


def test_direct_company_url_404_returns_likely_closed():
    job = _mk_job("jsearch_x", "jsearch", "https://careers.netflix.com/job/1")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=404):
        assert lc.check_job_status(job) == "likely_closed"


def test_direct_company_url_410_returns_likely_closed():
    """410 Gone is as authoritative as 404 for a filled role."""
    job = _mk_job("jsearch_x", "jsearch", "https://careers.netflix.com/job/1")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=410):
        assert lc.check_job_status(job) == "likely_closed"


def test_direct_company_url_403_is_unknown():
    """403 often means auth wall — can't tell if job is closed."""
    job = _mk_job("jsearch_x", "jsearch", "https://careers.netflix.com/job/1")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=403):
        assert lc.check_job_status(job) == "unknown"


def test_direct_company_url_503_is_unknown():
    """Transient 5xx shouldn't flip to likely_closed — retry next run."""
    job = _mk_job("jsearch_x", "jsearch", "https://careers.netflix.com/job/1")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=503):
        assert lc.check_job_status(job) == "unknown"


def test_direct_company_url_request_failure_is_unknown():
    job = _mk_job("jsearch_x", "jsearch", "https://careers.netflix.com/job/1")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=None):
        assert lc.check_job_status(job) == "unknown"


# ──────────────────────── check_job_status — aggregator (cat C) ────────────

def test_jooble_url_without_direct_fallback_is_unknown():
    """R-audit Issue 2: no authoritative check possible on a Jooble URL — return
    unknown and let the time-based archiver handle it."""
    job = _mk_job("jooble_1", "jooble", "https://jooble.org/desc/abc")
    assert lc.check_job_status(job) == "unknown"


def test_adzuna_url_without_direct_fallback_is_unknown():
    job = _mk_job("adzuna_1", "adzuna", "https://adzuna.com/land/ad/123")
    assert lc.check_job_status(job) == "unknown"


def test_empty_url_is_unknown():
    job = _mk_job("weird_1", "jsearch", "")
    assert lc.check_job_status(job) == "unknown"


# ──────────────────────── check_lifecycle_batch ────────────────────────────

def _seed_active_job(conn, ext_id, source_name, apply_url, last_lifecycle_check=None):
    job = {
        "external_id": ext_id,
        "title": "T",
        "company": "C",
        "source_name": source_name,
        "apply_url": apply_url,
        "raw_data": None,
    }
    db.upsert_job(conn, job)
    if last_lifecycle_check is not None:
        conn.execute(
            "UPDATE jobs SET last_lifecycle_check=? WHERE external_id=?",
            (last_lifecycle_check, ext_id),
        )
        conn.commit()


def test_batch_skips_recently_checked_jobs(conn):
    """R-audit: jobs with last_lifecycle_check within stale_days must be skipped."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    _seed_active_job(conn, "fresh", "jsearch", "https://a.com/1", last_lifecycle_check=today)
    _seed_active_job(conn, "also_fresh", "jsearch", "https://a.com/2", last_lifecycle_check=yesterday)
    _seed_active_job(conn, "stale", "jsearch", "https://a.com/3",
                     last_lifecycle_check=(datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d"))
    _seed_active_job(conn, "never_checked", "jsearch", "https://a.com/4")

    with patch("src.processors.lifecycle_checker._head_status_code", return_value=200), \
         patch("src.processors.lifecycle_checker.time.sleep"):
        stats = lc.check_lifecycle_batch(conn, stale_days=2, delay=0)
    # Only 'stale' + 'never_checked' should have been processed
    assert stats["checked"] == 2


def test_batch_uses_ats_snapshot_without_http(conn):
    """When the snapshot covers all jobs, no HEAD requests should fire."""
    _seed_active_job(conn, "gh_acme_1", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/1")
    _seed_active_job(conn, "gh_acme_2", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/2")
    snap = {("greenhouse", "acme"): {"1"}}  # job 2 is gone

    head_mock = MagicMock()
    with patch("src.processors.lifecycle_checker._head_status_code", head_mock), \
         patch("src.processors.lifecycle_checker.time.sleep"):
        stats = lc.check_lifecycle_batch(conn, ats_snapshots=snap, delay=0)
    head_mock.assert_not_called()
    assert stats["ats_snapshot_hits"] == 2
    assert stats["http_checks"] == 0
    assert stats["still_active"] == 1
    assert stats["likely_closed"] == 1

    # DB state reflects verdicts
    row1 = conn.execute("SELECT lifecycle_status, last_lifecycle_check "
                        "FROM jobs WHERE external_id=?", ("gh_acme_1",)).fetchone()
    row2 = conn.execute("SELECT lifecycle_status, last_lifecycle_check "
                        "FROM jobs WHERE external_id=?", ("gh_acme_2",)).fetchone()
    assert row1[0] == "active" and row1[1] is not None
    assert row2[0] == "likely_closed" and row2[1] is not None


def test_batch_updates_aggregator_only_jobs_with_unknown_verdict(conn):
    """Aggregator-only rows: last_lifecycle_check stamped, lifecycle_status
    left alone (no authoritative info). Archiver time-path handles them."""
    _seed_active_job(conn, "jooble_1", "jooble", "https://jooble.org/desc/1")
    with patch("src.processors.lifecycle_checker._head_status_code") as head, \
         patch("src.processors.lifecycle_checker.time.sleep"):
        stats = lc.check_lifecycle_batch(conn, delay=0)
    head.assert_not_called()
    assert stats["unknown"] == 1
    row = conn.execute("SELECT lifecycle_status, last_lifecycle_check "
                       "FROM jobs WHERE external_id=?", ("jooble_1",)).fetchone()
    assert row[0] == "active"  # unchanged
    assert row[1] is not None  # but stamped so we skip it next run


def test_batch_rate_limits_between_http_checks(conn):
    """R5-11: same-host HEAD requests serialize at `delay` seconds apart via
    _HostThrottle. All three jobs target 'a.com' so they must serialize even
    with the thread pool."""
    _seed_active_job(conn, "ext1", "jsearch", "https://a.com/1")
    _seed_active_job(conn, "ext2", "jsearch", "https://a.com/2")
    _seed_active_job(conn, "ext3", "jsearch", "https://a.com/3")

    with patch("src.processors.lifecycle_checker._head_status_code", return_value=200), \
         patch("src.processors.lifecycle_checker.time.sleep") as ms:
        lc.check_lifecycle_batch(conn, delay=0.5, max_workers=1)  # sequential for deterministic sleep assert
    # 3 HTTP checks → 2 inter-check sleeps at ~0.5s (throttle uses monotonic
    # deltas so allow small drift).
    assert ms.call_count == 2
    for call in ms.call_args_list:
        assert abs(call.args[0] - 0.5) < 0.01


def test_batch_no_rate_limit_for_ats_snapshot_only(conn):
    _seed_active_job(conn, "gh_acme_1", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/1")
    _seed_active_job(conn, "gh_acme_2", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/2")
    _seed_active_job(conn, "gh_acme_3", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/3")
    snap = {("greenhouse", "acme"): {"1", "2", "3"}}
    with patch("src.processors.lifecycle_checker.time.sleep") as ms:
        lc.check_lifecycle_batch(conn, ats_snapshots=snap, delay=1.0)
    ms.assert_not_called()


# ──────────────────────── archiver fast-path ──────────────────────────────

def test_archiver_fast_path_archives_confirmed_closed(conn):
    """R-audit Issue 2e / R7-A: a row flipped to likely_closed by
    lifecycle_checker (authoritative 'likely_closed' verdict) archives
    immediately. Track B time-marked rows must NOT be picked up here — that's
    what last_lifecycle_verdict discriminates."""
    from src.publishers import archiver
    _seed_active_job(conn, "confirmed_closed", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/1")
    # Simulate lifecycle_checker having flipped it today with an authoritative
    # likely_closed verdict (e.g., ATS API returned 404).
    conn.execute(
        "UPDATE jobs SET lifecycle_status='likely_closed', last_lifecycle_check=?, "
        "last_lifecycle_verdict='likely_closed' WHERE external_id=?",
        (datetime.now(timezone.utc).strftime("%Y-%m-%d"), "confirmed_closed"),
    )
    conn.commit()

    result = archiver.archive_stale(conn)
    assert result["archived_fast_path"] == 1
    row = conn.execute(
        "SELECT is_active, archived_date FROM jobs WHERE external_id=?",
        ("confirmed_closed",),
    ).fetchone()
    assert row[0] == 0
    assert row[1] is not None


# ───── R7-4: lifecycle transitions returned for WP push ──────────

def test_record_lifecycle_check_returns_new_status_on_transition(conn):
    """R7-4: `record_lifecycle_check` returns the new lifecycle_status when it
    changed, None otherwise. The caller uses this to build the WP push list."""
    _seed_active_job(conn, "a", "greenhouse", "https://boards.greenhouse.io/x/jobs/1")
    # Default lifecycle_status='active' → verdict=likely_closed changes it
    new = db.record_lifecycle_check(conn, "a", "likely_closed")
    assert new == "likely_closed"
    # Re-applying the same verdict does NOT transition
    again = db.record_lifecycle_check(conn, "a", "likely_closed")
    assert again is None
    # Transition back to active
    reactivated = db.record_lifecycle_check(conn, "a", "active")
    assert reactivated == "active"


def test_record_lifecycle_check_unknown_never_returns_transition(conn):
    """Unknown verdict leaves lifecycle_status unchanged → None return."""
    _seed_active_job(conn, "b", "jsearch", "https://careers.co/1")
    assert db.record_lifecycle_check(conn, "b", "unknown") is None


def test_batch_collects_transitions_for_wp_push(conn):
    """R7-4: batch collects external_ids that transitioned this run so the
    collector can push a targeted WP meta update."""
    _seed_active_job(conn, "gh_x_1", "greenhouse", "https://boards.greenhouse.io/x/jobs/1")
    _seed_active_job(conn, "gh_x_2", "greenhouse", "https://boards.greenhouse.io/x/jobs/2")
    # Snapshot says job 1 is gone (closed), job 2 still active
    snap = {("greenhouse", "x"): {"2"}}

    with patch("src.processors.lifecycle_checker._head_status_code", return_value=200):
        stats = lc.check_lifecycle_batch(conn, ats_snapshots=snap)

    assert "gh_x_1" in stats["transitions_to_closed"]
    # gh_x_2 was already active, no transition
    assert "gh_x_2" not in stats["transitions_to_active"]


def test_batch_collects_reactivations(conn):
    """A previously-likely_closed job that re-appears in the snapshot
    transitions back to active — also pushed to WP."""
    _seed_active_job(conn, "gh_x_1", "greenhouse", "https://boards.greenhouse.io/x/jobs/1")
    # Mark as likely_closed via prior run
    db.mark_job_likely_closed(conn, "gh_x_1")
    # Now it's back in the snapshot
    snap = {("greenhouse", "x"): {"1"}}
    stats = lc.check_lifecycle_batch(conn, ats_snapshots=snap)
    assert "gh_x_1" in stats["transitions_to_active"]


def test_archiver_fast_path_skips_unknown_verdict_on_time_based_likely_closed(conn):
    """R7-A regression: the specific false positive that motivated
    last_lifecycle_verdict.

    Scenario: Track B time-marks a 7-day-stale aggregator job likely_closed.
    Later, lifecycle_checker HEAD-checks the URL but the response is 403/
    timeout/etc — verdict 'unknown'. record_lifecycle_check stamps
    last_lifecycle_check and last_lifecycle_verdict='unknown' but leaves
    lifecycle_status at 'likely_closed'.

    Before the fix, fast-path would see (lifecycle_status='likely_closed' AND
    last_lifecycle_check IS NOT NULL) → archive immediately, robbing the job
    of its 21-day grace window. After the fix, fast-path requires
    last_lifecycle_verdict='likely_closed' so this row stays in Track B.
    """
    from src.publishers import archiver
    _seed_active_job(conn, "stale_then_unknown", "jooble",
                     "https://jooble.org/desc/1")
    # Track B flagged it likely_closed; later HEAD returned unknown
    conn.execute(
        "UPDATE jobs SET lifecycle_status='likely_closed', last_lifecycle_check=?, "
        "last_lifecycle_verdict='unknown' WHERE external_id=?",
        (datetime.now(timezone.utc).strftime("%Y-%m-%d"), "stale_then_unknown"),
    )
    conn.commit()

    result = archiver.archive_stale(conn)
    assert result["archived_fast_path"] == 0  # must NOT fast-path
    row = conn.execute(
        "SELECT is_active FROM jobs WHERE external_id=?",
        ("stale_then_unknown",),
    ).fetchone()
    assert row[0] == 1  # still active, waiting for Track B 21-day window


# ──── R4-1: HTTP budget control ─────────────────────────────────

def test_batch_respects_http_budget(conn):
    """R4-1: once http_budget is hit, remaining HTTP-required jobs are
    deferred without stamping last_lifecycle_check — they come back next run."""
    # Seed 5 aggregator-origin direct URLs so each requires HEAD
    for i in range(5):
        _seed_active_job(conn, f"ext{i}", "jsearch", f"https://careers.co/job{i}")

    with patch("src.processors.lifecycle_checker._head_status_code", return_value=200), \
         patch("src.processors.lifecycle_checker.time.sleep"):
        stats = lc.check_lifecycle_batch(conn, delay=0, http_budget=2)

    # Only 2 HTTP checks performed; remaining 3 deferred
    assert stats["http_checks"] == 2
    assert stats["http_budget_deferred"] == 3
    assert stats["checked"] == 2
    # Deferred rows must NOT have last_lifecycle_check stamped — they need to
    # come back in the next run's candidate pool.
    stamped = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE last_lifecycle_check IS NOT NULL"
    ).fetchone()[0]
    assert stamped == 2


def test_budget_doesnt_limit_ats_snapshot_jobs(conn):
    """R4-1: ATS-snapshot resolutions are free and shouldn't consume budget."""
    # 3 ATS jobs, all resolvable from snapshot
    for i in range(3):
        _seed_active_job(conn, f"gh_acme_{i}", "greenhouse",
                         f"https://boards.greenhouse.io/acme/jobs/{i}")
    snap = {("greenhouse", "acme"): {"0", "1", "2"}}
    with patch("src.processors.lifecycle_checker._head_status_code") as head:
        stats = lc.check_lifecycle_batch(conn, ats_snapshots=snap, http_budget=0)
    head.assert_not_called()
    assert stats["checked"] == 3
    assert stats["ats_snapshot_hits"] == 3
    assert stats["http_budget_deferred"] == 0


# ──── R4-4: board-failure-safe snapshots ────────────────────────

def test_snapshot_empty_set_is_authoritative(conn):
    """R4-4: an empty set for a successfully-fetched slug means 'board really
    has no jobs right now' → all stored jobs for that slug are likely_closed."""
    _seed_active_job(conn, "gh_acme_1", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/1")
    # Acme board fetched successfully today but returned 0 jobs
    snap = {("greenhouse", "acme"): set()}
    with patch("src.processors.lifecycle_checker._head_status_code") as head, \
         patch("src.processors.lifecycle_checker.time.sleep"):
        stats = lc.check_lifecycle_batch(conn, ats_snapshots=snap, delay=0)
    head.assert_not_called()  # ATS resolution — no HTTP
    assert stats["likely_closed"] == 1


def test_snapshot_missing_key_falls_through_to_http(conn):
    """R4-4: a slug that was NOT successfully fetched this run (no key in the
    snapshot map) must NOT mass-close its jobs — fall through to HEAD."""
    _seed_active_job(conn, "gh_acme_1", "greenhouse",
                     "https://boards.greenhouse.io/acme/jobs/1")
    # snapshot has OTHER slugs but no 'acme' entry (acme fetch failed)
    snap = {("greenhouse", "other"): {"99"}}
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=200) as head, \
         patch("src.processors.lifecycle_checker.time.sleep"):
        stats = lc.check_lifecycle_batch(conn, ats_snapshots=snap, delay=0)
    head.assert_called_once()
    assert stats["still_active"] == 1
    assert stats["likely_closed"] == 0


# ──── R5-7: HEAD retry on network reset ─────────────────────────

def test_head_retries_once_on_request_exception():
    """R5-7: a single connection blip shouldn't flip the job to unknown."""
    import requests as rq
    ok = MagicMock(status_code=200)
    with patch("src.processors.lifecycle_checker.requests.head",
               side_effect=[rq.ConnectionError("reset"), ok]) as mhead, \
         patch("src.processors.lifecycle_checker.requests.get") as mget:
        code = lc._head_status_code("https://careers.example.com/job/1")
    assert code == 200
    assert mhead.call_count == 2  # one retry after first ConnectionError
    mget.assert_not_called()


def test_head_does_not_retry_on_timeout():
    """R6-C3: a host that didn't answer in `timeout` seconds is probably dead.
    Retrying just burns wall-time budget — skip to GET fallback (which also
    won't retry on timeout) so we move on quickly."""
    import requests as rq
    with patch("src.processors.lifecycle_checker.requests.head",
               side_effect=rq.Timeout("host dead")) as mhead, \
         patch("src.processors.lifecycle_checker.requests.get",
               side_effect=rq.Timeout("host dead")) as mget:
        code = lc._head_status_code("https://dead.example.com/job/1")
    assert code is None
    # HEAD called exactly once (no retry on timeout), then GET once (also no retry)
    assert mhead.call_count == 1
    assert mget.call_count == 1


def test_head_retry_uses_shorter_timeout():
    """R6-C3: the retry uses CHECK_RETRY_TIMEOUT_SEC (shorter). Verify by
    capturing the timeout keyword on each HEAD call."""
    import requests as rq
    ok = MagicMock(status_code=200)
    calls: list[float] = []

    def fake_head(url, **kw):
        calls.append(kw.get("timeout", -1))
        if len(calls) == 1:
            raise rq.ConnectionError("reset")
        return ok

    with patch("src.processors.lifecycle_checker.requests.head",
               side_effect=fake_head):
        lc._head_status_code("https://flaky.example.com/job/1")
    assert len(calls) == 2
    assert calls[0] == lc.CHECK_TIMEOUT_SEC
    assert calls[1] == lc.CHECK_RETRY_TIMEOUT_SEC


def test_head_gives_up_after_two_exceptions():
    """Two consecutive RequestExceptions on HEAD → fall through to GET."""
    import requests as rq
    ok = MagicMock(status_code=200)
    ok.close = MagicMock()
    with patch("src.processors.lifecycle_checker.requests.head",
               side_effect=[rq.ConnectionError("reset"),
                            rq.ConnectionError("still down")]), \
         patch("src.processors.lifecycle_checker.requests.get",
               return_value=ok) as mget:
        code = lc._head_status_code("https://careers.example.com/job/1")
    assert code == 200
    mget.assert_called_once()


def test_get_retries_once_on_request_exception():
    """After HEAD gets 403, GET retries once if the first attempt connection-fails."""
    import requests as rq
    head_403 = MagicMock(status_code=403)
    ok = MagicMock(status_code=200)
    ok.close = MagicMock()
    with patch("src.processors.lifecycle_checker.requests.head",
               return_value=head_403), \
         patch("src.processors.lifecycle_checker.requests.get",
               side_effect=[rq.ConnectionError("blip"), ok]) as mget:
        code = lc._head_status_code("https://careers.example.com/job/1")
    assert code == 200
    assert mget.call_count == 2


# ──── R5-11: parallel HEAD checks + per-host throttle ───────────

def test_parallel_batch_distinct_hosts_overlap(conn):
    """R5-11 / R6-I2: three jobs on distinct hosts must actually execute in
    parallel. Wall-clock assertions flake under CI load, so we verify with a
    threading.Barrier: if all three workers reach the barrier simultaneously,
    they're genuinely concurrent. If the code is serial, the first worker
    blocks forever waiting for the barrier and the test times out."""
    import threading
    _seed_active_job(conn, "e1", "jsearch", "https://a.example.com/1")
    _seed_active_job(conn, "e2", "jsearch", "https://b.example.com/2")
    _seed_active_job(conn, "e3", "jsearch", "https://c.example.com/3")

    barrier = threading.Barrier(3, timeout=2.0)

    def head_with_barrier(url, **kw):
        # Each worker waits until all three have arrived. Passes iff they're
        # running concurrently; BrokenBarrierError on timeout.
        barrier.wait()
        return 200

    with patch("src.processors.lifecycle_checker._head_status_code",
               side_effect=head_with_barrier):
        stats = lc.check_lifecycle_batch(conn, delay=0.0, max_workers=3)
    assert stats["still_active"] == 3


def test_parallel_batch_same_host_serializes(conn):
    """R5-11 / R6-I2: three jobs on the SAME host must NOT execute concurrently.
    The throttle spaces acquisitions `delay` seconds apart; with per-call work
    of 0.01s and delay of 0.05s, the second/third request can't start until
    the prior one has returned. Track peak concurrent workers — must be 1."""
    import threading
    _seed_active_job(conn, "e1", "jsearch", "https://one.example.com/1")
    _seed_active_job(conn, "e2", "jsearch", "https://one.example.com/2")
    _seed_active_job(conn, "e3", "jsearch", "https://one.example.com/3")

    in_flight = {"count": 0, "peak": 0}
    lock = threading.Lock()

    def head_tracking(url, **kw):
        with lock:
            in_flight["count"] += 1
            in_flight["peak"] = max(in_flight["peak"], in_flight["count"])
        import time as _t
        _t.sleep(0.01)  # work window smaller than throttle gap
        with lock:
            in_flight["count"] -= 1
        return 200

    with patch("src.processors.lifecycle_checker._head_status_code",
               side_effect=head_tracking):
        stats = lc.check_lifecycle_batch(conn, delay=0.05, max_workers=5)
    assert stats["still_active"] == 3
    assert in_flight["peak"] == 1, f"same-host throttle bypassed (peak={in_flight['peak']})"


def test_db_writes_serialized_on_main_thread(conn):
    """DB writes must happen on the caller thread — SQLite/libsql connections
    aren't thread-safe. Verify every verdict was recorded in DB after a
    parallel batch."""
    _seed_active_job(conn, "e1", "jsearch", "https://a.example.com/1")
    _seed_active_job(conn, "e2", "jsearch", "https://b.example.com/2")
    _seed_active_job(conn, "e3", "jsearch", "https://c.example.com/3")
    with patch("src.processors.lifecycle_checker._head_status_code",
               return_value=200):
        lc.check_lifecycle_batch(conn, delay=0.0, max_workers=3)
    stamped = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE last_lifecycle_check IS NOT NULL"
    ).fetchone()[0]
    assert stamped == 3


# ──── R4-16: HEAD→GET fallback on 403 ───────────────────────────

def test_head_403_retries_as_get():
    """Cloudflare-style: HEAD returns 403 but GET succeeds → use GET verdict."""
    import requests as real_requests

    head_resp = MagicMock(status_code=403)
    get_resp = MagicMock(status_code=200)
    get_resp.close = MagicMock()

    with patch("src.processors.lifecycle_checker.requests.head",
               return_value=head_resp) as mhead, \
         patch("src.processors.lifecycle_checker.requests.get",
               return_value=get_resp) as mget:
        code = lc._head_status_code("https://careers.example.com/job/1")
    assert code == 200
    mhead.assert_called_once()
    mget.assert_called_once()


def test_head_404_is_authoritative_no_get_retry():
    head_resp = MagicMock(status_code=404)
    with patch("src.processors.lifecycle_checker.requests.head",
               return_value=head_resp), \
         patch("src.processors.lifecycle_checker.requests.get") as mget:
        code = lc._head_status_code("https://careers.example.com/job/1")
    assert code == 404
    mget.assert_not_called()


def test_head_503_stays_transient_no_get_retry():
    head_resp = MagicMock(status_code=503)
    with patch("src.processors.lifecycle_checker.requests.head",
               return_value=head_resp), \
         patch("src.processors.lifecycle_checker.requests.get") as mget:
        code = lc._head_status_code("https://careers.example.com/job/1")
    assert code == 503
    mget.assert_not_called()


def test_head_405_retries_as_get():
    """Canonical 'method not allowed' still triggers GET fallback."""
    head_resp = MagicMock(status_code=405)
    get_resp = MagicMock(status_code=200)
    get_resp.close = MagicMock()
    with patch("src.processors.lifecycle_checker.requests.head",
               return_value=head_resp), \
         patch("src.processors.lifecycle_checker.requests.get",
               return_value=get_resp):
        code = lc._head_status_code("https://careers.example.com/job/1")
    assert code == 200


def test_archiver_fast_path_skips_time_based_likely_closed(conn):
    """A job marked likely_closed by the time-based Track B (last_lifecycle_check
    is NULL) should NOT be archived by the fast-path — only Track B's 21-day
    window applies."""
    from src.publishers import archiver
    _seed_active_job(conn, "time_based", "jooble", "https://jooble.org/desc/1")
    # Simulate Track B having marked it (no lifecycle_check stamp)
    conn.execute(
        "UPDATE jobs SET lifecycle_status='likely_closed' WHERE external_id=?",
        ("time_based",),
    )
    conn.commit()

    result = archiver.archive_stale(conn)
    assert result["archived_fast_path"] == 0
    row = conn.execute(
        "SELECT is_active FROM jobs WHERE external_id=?", ("time_based",),
    ).fetchone()
    assert row[0] == 1  # still active pending 21-day window
