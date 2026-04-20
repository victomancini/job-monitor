"""Tests for wordpress.py, notifier.py, archiver.py. Uses in-memory sqlite + mocked HTTP."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from src import db
from src.publishers import archiver, notifier, wordpress


# ────────────────────── fixtures ──────────────────────

@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    db.migrate(c)
    yield c
    c.close()


def _job(ext_id="jsearch_1", title="People Analytics Manager", company="Netflix",
         fit_score=75, llm_classification="RELEVANT"):
    return {
        "external_id": ext_id,
        "title": title,
        "company": company,
        "location": "Los Gatos, CA",
        "source_url": "https://example.com/1",
        "source_name": "jsearch",
        "description": "desc",
        "description_snippet": "desc",
        "fit_score": fit_score,
        "llm_classification": llm_classification,
        "llm_confidence": 90,
        "llm_provider": "groq",
        "salary_range": "$150K-$200K",
    }


def _mock_resp(body, status=200):
    m = MagicMock()
    m.status_code = status
    m.json.return_value = body
    m.text = str(body)
    return m


# ────────────────────── WordPress ─────────────────────

def test_wordpress_publish_one_job(conn):
    db.upsert_job(conn, _job())
    body = {"created": 1, "updated": 0, "errors": 0, "post_ids": {"jsearch_1": 101}}
    with patch("src.publishers.wordpress.retry_request", return_value=_mock_resp(body)):
        totals = wordpress.publish(
            [_job()], wp_url="https://site.com", username="u", app_password="p", conn=conn,
        )
    assert totals["created"] == 1
    # Verify wp_post_id stored in Turso
    row = conn.execute("SELECT wp_post_id FROM jobs WHERE external_id=?", ("jsearch_1",)).fetchone()
    assert row[0] == 101


def test_wordpress_payload_includes_phase_f_fields():
    """Phase F6: apply_url, seniority, *_confidence, enrichment_source must be sent to WP."""
    j = _job()
    j.update({
        "apply_url": "https://careers.netflix.com/job/1",
        "seniority": "Senior Manager",
        "location_confidence": "confirmed",
        "salary_confidence": "aggregator_only",
        "remote_confidence": "confirmed",
        "enrichment_source": "source_page",
    })
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured.update(json)
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p")

    payload = captured["jobs"][0]
    assert payload["apply_url"] == "https://careers.netflix.com/job/1"
    assert payload["seniority"] == "Senior Manager"
    assert payload["location_confidence"] == "confirmed"
    assert payload["salary_confidence"] == "aggregator_only"
    assert payload["remote_confidence"] == "confirmed"
    assert payload["enrichment_source"] == "source_page"


def test_wordpress_payload_includes_date_posted_and_seniority_confidence():
    """Phase B/F (R2): date_posted + seniority_confidence in WP payload."""
    j = _job()
    j.update({
        "date_posted": "2026-04-14",
        "seniority_confidence": "inferred",
    })
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured.update(json)
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p")

    payload = captured["jobs"][0]
    assert payload["date_posted"] == "2026-04-14"
    assert payload["seniority_confidence"] == "inferred"


def test_wordpress_payload_includes_vendors_mentioned():
    """Phase 5 (R3): vendors_mentioned is sent as a comma-separated string."""
    j = _job()
    j["vendors_mentioned"] = "Qualtrics,Medallia,Python,SQL"
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured.update(json)
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p")

    assert captured["jobs"][0]["vendors_mentioned"] == "Qualtrics,Medallia,Python,SQL"


def test_wordpress_payload_includes_r11_freshness_fields():
    """R11 Phase 0: first_seen_date + days_since_posted + is_brand_new must
    reach WordPress so the plugin stops re-stamping today on recreated posts
    and can sort freshness numerically without timezone drift."""
    from datetime import datetime, timedelta, timezone
    seven_days_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    j = _job()
    j.update({
        "date_posted": seven_days_ago,
        "first_seen_date": seven_days_ago,
        "_is_brand_new": False,
    })
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured.update(json)
        return _mock_resp({"created": 0, "updated": 1, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p")

    payload = captured["jobs"][0]
    assert payload["first_seen_date"] == seven_days_ago
    assert payload["days_since_posted"] == 7
    assert payload["is_brand_new"] == 0


def test_wordpress_payload_brand_new_flag_set_on_creation():
    """When Turso just inserted the row, is_brand_new=1 reaches WP so the NEW
    badge can render on exactly the day the job first appeared."""
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    j = _job()
    j.update({
        "first_seen_date": today,
        "_is_brand_new": True,
    })
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured.update(json)
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p")

    payload = captured["jobs"][0]
    assert payload["is_brand_new"] == 1
    assert payload["days_since_posted"] == 0


def test_wordpress_payload_ships_consensus_vote_fields():
    """R11 Phase 6: consensus transparency. WP rendering needs the per-field
    agreement count + source list to render the 'verified by N sources'
    tooltip on the Remote cell."""
    j = _job()
    j["_consensus"] = {
        "is_remote": {
            "value": "hybrid",
            "confidence": 0.82,
            "sources": ["greenhouse", "text_classifier"],
        },
        "work_arrangement": {
            "value": "hybrid",
            "confidence": 0.82,
            "sources": ["greenhouse", "text_classifier"],
        },
    }
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured.update(json)
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p")

    payload = captured["jobs"][0]
    assert payload["remote_vote_confidence"] == 0.82
    assert payload["remote_vote_sources"] == "greenhouse,text_classifier"
    assert payload["remote_vote_agreement"] == 2
    assert payload["work_arrangement_vote_sources"] == "greenhouse,text_classifier"


def test_wordpress_payload_without_consensus_omits_vote_fields():
    """Jobs never touched by consensus voting (no observations) shouldn't
    ship empty vote fields that would clutter WP meta."""
    j = _job()
    # No _consensus key
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured.update(json)
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p")

    payload = captured["jobs"][0]
    assert "remote_vote_confidence" not in payload
    assert "remote_vote_sources" not in payload


def test_wordpress_update_existing_not_duplicate(conn):
    """Same external_id → endpoint returns 'updated', not 'created'."""
    body = {"created": 0, "updated": 1, "errors": 0, "post_ids": {"jsearch_1": 101}}
    with patch("src.publishers.wordpress.retry_request", return_value=_mock_resp(body)):
        totals = wordpress.publish([_job()], wp_url="https://s", username="u", app_password="p")
    assert totals["updated"] == 1
    assert totals["created"] == 0


def test_wordpress_down_queues_to_retry(conn):
    """Connection error → job goes to retry_queue."""
    with patch("src.publishers.wordpress.retry_request", side_effect=Exception("connection refused")), \
         patch("src.publishers.wordpress.time.sleep"):
        totals = wordpress.publish(
            [_job("a"), _job("b")], wp_url="https://s", username="u", app_password="p", conn=conn,
        )
    assert totals["queued"] == 2
    queued = db.fetch_retry_queue(conn)
    assert len(queued) == 2


def test_wordpress_5xx_queues_to_retry(conn):
    with patch("src.publishers.wordpress.retry_request", return_value=_mock_resp({}, status=500)), \
         patch("src.publishers.wordpress.time.sleep"):
        totals = wordpress.publish([_job()], wp_url="https://s", username="u", app_password="p", conn=conn)
    assert totals["queued"] == 1


def test_wordpress_process_retry_queue_succeeds(conn):
    db.enqueue_retry(conn, _job("queued_1"))
    db.enqueue_retry(conn, _job("queued_2"))
    body = {"created": 2, "updated": 0, "errors": 0, "post_ids": {"queued_1": 201, "queued_2": 202}}
    with patch("src.publishers.wordpress.retry_request", return_value=_mock_resp(body)), \
         patch("src.publishers.wordpress.time.sleep"):
        result = wordpress.process_retry_queue(conn, wp_url="https://s", username="u", app_password="p")
    assert result["succeeded"] == 2
    assert db.fetch_retry_queue(conn) == []


def test_wordpress_retry_queue_three_failures_drops(conn):
    db.enqueue_retry(conn, _job("queued_drop"))
    with patch("src.publishers.wordpress.retry_request", side_effect=Exception("down")), \
         patch("src.publishers.wordpress.time.sleep"):
        for _ in range(3):
            wordpress.process_retry_queue(conn, wp_url="https://s", username="u", app_password="p")
    # After 3 failures: attempts >= 3 → drop_exhausted_retries should have removed it
    assert db.fetch_retry_queue(conn, max_attempts=99) == []


def test_wordpress_retry_queue_surfaces_dropped_count(conn, caplog):
    """Regression for M2: dropped jobs are logged + returned so the healthcheck
    ping can surface them instead of silent data loss."""
    import logging
    # Pre-seed a retry row already at the failure threshold
    db.enqueue_retry(conn, _job("exhausted"))
    conn.execute("UPDATE retry_queue SET attempts = 3 WHERE 1=1")
    conn.commit()
    with caplog.at_level(logging.WARNING, logger="src.publishers.wordpress"):
        result = wordpress.process_retry_queue(
            conn, wp_url="https://s", username="u", app_password="p",
        )
    assert result["dropped"] == 1
    assert any("dropped 1" in rec.getMessage() for rec in caplog.records)


def test_wordpress_empty_jobs_returns_zeros():
    totals = wordpress.publish([], wp_url="https://s", username="u", app_password="p")
    assert totals["created"] == 0 and totals["queued"] == 0


def test_wordpress_missing_creds_queues_all(conn):
    totals = wordpress.publish([_job()], wp_url="", username="u", app_password="p", conn=conn)
    assert totals["queued"] == 1


def test_wordpress_batches_of_20(conn):
    jobs = [_job(f"j{i}") for i in range(45)]
    body = {"created": 20, "updated": 0, "errors": 0, "post_ids": {}}
    with patch("src.publishers.wordpress.retry_request", return_value=_mock_resp(body)) as mock, \
         patch("src.publishers.wordpress.time.sleep"):
        totals = wordpress.publish(jobs, wp_url="https://s", username="u", app_password="p", conn=conn)
    assert totals["batches"] == 3  # 20+20+5
    assert mock.call_count == 3


# Regression: N1 — retry_queue must store the full job dict (not the
# cast-to-string _payload) so retries can rebuild the payload fresh at send time.
def test_retry_queue_stores_full_dict_not_payload(conn):
    j = _job("rq_full")
    j["salary_min"] = 150000.0  # float — _payload would str() this
    j["llm_confidence"] = 90     # int — _payload passes through
    j["llm_reasoning"] = "core EL role"
    with patch("src.publishers.wordpress.retry_request",
               side_effect=Exception("wp down")), \
         patch("src.publishers.wordpress.time.sleep"):
        wordpress.publish([j], wp_url="https://s", username="u", app_password="p", conn=conn)
    queued = db.fetch_retry_queue(conn)
    assert len(queued) == 1
    _, stored = queued[0]
    # Floats survive as floats, not "150000.0" strings
    assert stored["salary_min"] == 150000.0
    assert isinstance(stored["salary_min"], float)
    assert stored["llm_confidence"] == 90
    # Fields stripped by _payload (e.g. llm_reasoning isn't in _WP_FIELDS) must
    # still be present on the enqueued dict for debugging / future re-classify.
    assert stored["llm_reasoning"] == "core EL role"


# Regression: N4 — X-JM-Secret header is sent only when WP_SHARED_SECRET is set.
def test_wordpress_sends_shared_secret_header_when_env_set(conn, monkeypatch):
    monkeypatch.setenv("WP_SHARED_SECRET", "s3cret")
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured["headers"] = headers
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([_job()], wp_url="https://s", username="u", app_password="p", conn=conn)
    assert captured["headers"].get("X-JM-Secret") == "s3cret"


def test_wordpress_omits_shared_secret_header_when_env_unset(conn, monkeypatch):
    monkeypatch.delenv("WP_SHARED_SECRET", raising=False)
    captured = {}

    def capture(method, url, *, headers, json, **kw):
        captured["headers"] = headers
        return _mock_resp({"created": 1, "updated": 0, "errors": 0, "post_ids": {}})

    with patch("src.publishers.wordpress.retry_request", side_effect=capture):
        wordpress.publish([_job()], wp_url="https://s", username="u", app_password="p", conn=conn)
    assert "X-JM-Secret" not in captured["headers"]


# ────────────────────── Notifier ──────────────────────

def test_is_qualifying_score():
    assert notifier.is_qualifying({"fit_score": 55, "llm_classification": "NOT_RELEVANT"})


def test_is_qualifying_llm_relevant():
    assert notifier.is_qualifying({"fit_score": 10, "llm_classification": "RELEVANT"})


def test_is_not_qualifying_low_score_and_not_relevant():
    assert not notifier.is_qualifying({"fit_score": 20, "llm_classification": "NOT_RELEVANT"})


def test_pushover_sends():
    with patch("src.publishers.notifier.retry_request", return_value=_mock_resp({"status": 1})):
        ok = notifier.send_pushover(_job(), user_key="u", app_token="t")
    assert ok


def test_pushover_missing_creds_silent_false():
    ok = notifier.send_pushover(_job(), user_key="", app_token="")
    assert ok is False


def test_pushover_http_failure_returns_false():
    with patch("src.publishers.notifier.retry_request", return_value=_mock_resp({}, status=500)):
        ok = notifier.send_pushover(_job(), user_key="u", app_token="t")
    assert ok is False


def test_format_digest_html_escapes_hostile_fields():
    """Regression for C4: hostile strings in job fields must not break out of
    HTML or the href attribute."""
    malicious = _job(
        ext_id="x",
        title='<script>alert("xss")</script>',
        company='Acme" onload="pwn()',
    )
    malicious["location"] = "NY & NJ"
    malicious["source_url"] = 'javascript:"alert(1)"'
    html_out = notifier._format_digest_html([malicious])
    # Script tags never render raw
    assert "<script>alert" not in html_out
    # Ampersand in data is HTML-entity-encoded
    assert "NY &amp; NJ" in html_out
    # URL quote must be escaped so it can't close the href="" attribute
    assert 'javascript:"' not in html_out  # raw form absent
    assert 'javascript:&quot;' in html_out


def test_brevo_email_digest_sends():
    with patch("src.publishers.notifier.smtplib.SMTP") as mock_smtp:
        instance = mock_smtp.return_value.__enter__.return_value
        ok = notifier.send_email_digest(
            [_job("a"), _job("b")],
            smtp_user="u", smtp_pass="p", to_email="me@x.com",
        )
    assert ok
    instance.starttls.assert_called_once()
    instance.login.assert_called_once_with("u", "p")
    instance.send_message.assert_called_once()


def test_brevo_email_empty_jobs_no_send():
    with patch("src.publishers.notifier.smtplib.SMTP") as mock_smtp:
        ok = notifier.send_email_digest([], smtp_user="u", smtp_pass="p", to_email="me@x.com")
    assert ok
    mock_smtp.assert_not_called()


def test_brevo_email_missing_creds_returns_false():
    ok = notifier.send_email_digest([_job()], smtp_user="", smtp_pass="", to_email="me@x.com")
    assert ok is False


def test_notify_orchestrator_both_channels():
    jobs = [_job("a", fit_score=80), _job("b", fit_score=20, llm_classification="NOT_RELEVANT")]
    with patch("src.publishers.notifier.retry_request", return_value=_mock_resp({"status": 1})), \
         patch("src.publishers.notifier.smtplib.SMTP"):
        r = notifier.notify(jobs,
                            pushover_user="u", pushover_token="t",
                            brevo_user="b", brevo_pass="bp", email_to="me@x")
    assert r["qualifying"] == 1
    assert r["pushes_sent"] == 1
    assert r["email_sent"] == 1


# ────────────────────── Archiver ──────────────────────

def test_archiver_marks_stale(conn):
    """Phase 6 (R3): archival threshold moved to 21 days (7-day `likely_closed` + 14)."""
    db.upsert_job(conn, _job("stale_a"))
    old = (datetime.now(timezone.utc) - timedelta(days=22)).strftime("%Y-%m-%d")
    conn.execute("UPDATE jobs SET last_seen_date=? WHERE external_id=?", (old, "stale_a"))
    conn.commit()

    result = archiver.archive_stale(conn)
    assert result["archived"] == 1
    row = conn.execute(
        "SELECT is_active, archived_date, days_active FROM jobs WHERE external_id=?",
        ("stale_a",),
    ).fetchone()
    assert row[0] == 0
    assert row[1] is not None
    assert row[2] >= 1


def test_archiver_ignores_fresh_jobs(conn):
    db.upsert_job(conn, _job("fresh"))
    result = archiver.archive_stale(conn)
    assert result["archived"] == 0
    row = conn.execute("SELECT is_active FROM jobs WHERE external_id=?", ("fresh",)).fetchone()
    assert row[0] == 1


def test_archiver_days_active_computed(conn):
    """days_active = last_seen_date - first_seen_date."""
    db.upsert_job(conn, _job("x"))
    first = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    last = (datetime.now(timezone.utc) - timedelta(days=22)).strftime("%Y-%m-%d")
    conn.execute(
        "UPDATE jobs SET first_seen_date=?, last_seen_date=? WHERE external_id=?",
        (first, last, "x"),
    )
    conn.commit()
    archiver.archive_stale(conn)
    row = conn.execute("SELECT days_active FROM jobs WHERE external_id=?", ("x",)).fetchone()
    assert row[0] == 8
