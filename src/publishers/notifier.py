"""Dual-channel notifier: Pushover push (per-job) + Brevo SMTP digest (all qualifiers in one email).
Qualifying jobs: fit_score >= 50 OR llm_classification == 'RELEVANT'.
"""
from __future__ import annotations

import logging
import smtplib
from email.message import EmailMessage
from typing import Any

from src.sources._http import retry_request

log = logging.getLogger(__name__)

PUSHOVER_URL = "https://api.pushover.net/1/messages.json"
BREVO_HOST = "smtp-relay.brevo.com"
BREVO_PORT = 587


def is_qualifying(job: dict[str, Any]) -> bool:
    if int(job.get("fit_score") or 0) >= 50:
        return True
    return job.get("llm_classification") == "RELEVANT"


# ──────────────────────────── Pushover ───────────────────────────

def send_pushover(
    job: dict[str, Any],
    *,
    user_key: str,
    app_token: str,
) -> bool:
    if not user_key or not app_token:
        return False
    title = f"{job.get('title', '')} — {job.get('company', '')}"[:250]
    location = job.get("location") or ""
    salary = job.get("salary_range") or ""
    score = job.get("fit_score") or 0
    message = f"{location}\n{salary}\nScore: {score}".strip()
    payload = {
        "token": app_token,
        "user": user_key,
        "title": title,
        "message": message,
        "url": job.get("source_url") or "",
        "priority": 0,
    }
    try:
        resp = retry_request("POST", PUSHOVER_URL, data=payload)
        if resp.status_code != 200:
            log.warning("pushover: HTTP %s for %s", resp.status_code, job.get("external_id"))
            return False
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("pushover: error sending %s: %s", job.get("external_id"), e)
        return False


# ──────────────────────────── Brevo digest email ─────────────────

def _format_digest_html(jobs: list[dict[str, Any]]) -> str:
    rows = []
    for j in jobs:
        title = j.get("title") or ""
        company = j.get("company") or ""
        location = j.get("location") or ""
        salary = j.get("salary_range") or ""
        score = j.get("fit_score") or 0
        url = j.get("source_url") or "#"
        cls = j.get("llm_classification") or "-"
        rows.append(
            f'<tr><td><a href="{url}">{title}</a></td><td>{company}</td>'
            f'<td>{location}</td><td>{salary}</td><td>{score}</td><td>{cls}</td></tr>'
        )
    return (
        "<html><body><h2>Job Monitor Digest</h2>"
        f"<p>{len(jobs)} qualifying new role(s).</p>"
        "<table border='1' cellpadding='4' cellspacing='0'>"
        "<tr><th>Title</th><th>Company</th><th>Location</th><th>Salary</th><th>Score</th><th>LLM</th></tr>"
        + "".join(rows)
        + "</table></body></html>"
    )


def _format_digest_text(jobs: list[dict[str, Any]]) -> str:
    out = [f"Job Monitor Digest — {len(jobs)} qualifying role(s)\n"]
    for j in jobs:
        out.append(
            f"- {j.get('title','')} @ {j.get('company','')}\n"
            f"  {j.get('location','')}  |  {j.get('salary_range','') or 'salary n/a'}  "
            f"|  score={j.get('fit_score',0)}  |  llm={j.get('llm_classification','-')}\n"
            f"  {j.get('source_url','')}\n"
        )
    return "\n".join(out)


def send_email_digest(
    jobs: list[dict[str, Any]],
    *,
    smtp_user: str,
    smtp_pass: str,
    to_email: str,
    from_email: str | None = None,
) -> bool:
    if not jobs:
        return True
    if not smtp_user or not smtp_pass or not to_email:
        log.warning("brevo: credentials incomplete, skipping digest")
        return False
    msg = EmailMessage()
    msg["Subject"] = f"[Job Monitor] {len(jobs)} new role(s)"
    msg["From"] = from_email or to_email
    msg["To"] = to_email
    msg.set_content(_format_digest_text(jobs))
    msg.add_alternative(_format_digest_html(jobs), subtype="html")
    try:
        with smtplib.SMTP(BREVO_HOST, BREVO_PORT, timeout=30) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("brevo: send error: %s", e)
        return False


# ──────────────────────────── Orchestrator entry point ───────────

def notify(
    jobs: list[dict[str, Any]],
    *,
    pushover_user: str = "",
    pushover_token: str = "",
    brevo_user: str = "",
    brevo_pass: str = "",
    email_to: str = "",
) -> dict[str, int]:
    """Send one push per qualifying job, one digest email with all qualifiers."""
    qualifying = [j for j in jobs if is_qualifying(j)]
    pushes = 0
    for j in qualifying:
        if send_pushover(j, user_key=pushover_user, app_token=pushover_token):
            pushes += 1
    email_sent = send_email_digest(
        qualifying,
        smtp_user=brevo_user,
        smtp_pass=brevo_pass,
        to_email=email_to,
    ) if qualifying else True
    return {
        "qualifying": len(qualifying),
        "pushes_sent": pushes,
        "email_sent": 1 if email_sent else 0,
    }
