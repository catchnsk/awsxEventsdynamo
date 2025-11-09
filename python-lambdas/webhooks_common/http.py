from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Dict

import requests


def post_with_retry(url: str, payload: Dict[str, Any], headers: Dict[str, str], secret: str | None = None,
                    max_attempts: int = 3, backoff_seconds: int = 1) -> requests.Response:
    signing_headers = headers.copy()
    body = json.dumps(payload, separators=(",", ":"))
    if secret:
        signature = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
        signing_headers["X-Signature"] = signature
    attempt = 1
    while True:
        try:
            response = requests.post(url, data=body, headers={"Content-Type": "application/json", **signing_headers}, timeout=10)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt >= max_attempts:
                raise
            time.sleep(backoff_seconds * attempt)
            attempt += 1
