"""Shared HTTP retry helper. 3 attempts, exponential backoff 2s/4s/8s on 5xx/timeout."""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

import requests

log = logging.getLogger(__name__)


def retry_request(
    method: str,
    url: str,
    *,
    max_attempts: int = 3,
    timeout: float = 30.0,
    on_response: Callable[[requests.Response], None] | None = None,
    **kwargs: Any,
) -> requests.Response:
    """Do an HTTP request with exponential backoff on 5xx / timeout / connection error.

    Raises the final exception or returns the response (possibly 4xx — caller must check).
    `on_response` is called on every response before return (for quota header logging).
    """
    backoffs = [2, 4, 8]
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if on_response is not None:
                try:
                    on_response(resp)
                except Exception:  # noqa: BLE001 — never let logging break the request
                    log.exception("on_response hook raised; continuing")
            if 500 <= resp.status_code < 600 and attempt < max_attempts - 1:
                log.warning("HTTP %s on %s (attempt %d) — retrying", resp.status_code, url, attempt + 1)
                time.sleep(backoffs[attempt])
                continue
            return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < max_attempts - 1:
                log.warning("%s on %s (attempt %d) — retrying", type(e).__name__, url, attempt + 1)
                time.sleep(backoffs[attempt])
            else:
                raise
    assert last_exc is not None  # unreachable
    raise last_exc
