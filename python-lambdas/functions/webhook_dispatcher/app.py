from __future__ import annotations

import base64
import json
import time
from typing import Any, Dict, List

from webhooks_common import dynamodb, http
from webhooks_common.config import load_config
from webhooks_common.logging import LOGGER


def _decode_record(record: Dict[str, Any]) -> Dict[str, Any]:
    body = base64.b64decode(record["value"]).decode()
    return json.loads(body)


def handler(event, context):
    cfg = load_config()
    failures: List[Dict[str, str]] = []

    for _, records in event.get("records", {}).items():
        for record in records:
            event_payload = _decode_record(record)
            event_id = event_payload.get("event_id")
            subscription_id = event_payload.get("subscription_id")
            if not subscription_id:
                LOGGER.error("subscription_id missing; skipping")
                failures.append({"itemIdentifier": record.get("recordId", event_id or "unknown")})
                continue

            subscription = dynamodb.fetch_subscription(subscription_id)
            if not subscription:
                LOGGER.error("Subscription %s not found", subscription_id)
                failures.append({"itemIdentifier": record.get("recordId", event_id or "unknown")})
                continue

            idempotency_key = event_payload.get("idempotency_key", event_id)
            if idempotency_key and dynamodb.check_idempotency(idempotency_key):
                LOGGER.info("Duplicate event %s skipped", event_id)
                continue

            delivery_url = subscription.get("delivery_url")
            secret = subscription.get("auth", {}).get("secret") if isinstance(subscription.get("auth"), dict) else None

            try:
                http.post_with_retry(delivery_url, event_payload["data"], headers={
                    "X-Webhook-Event": event_payload.get("event_name", "unknown"),
                    "X-Webhook-EventId": event_id or "unknown",
                }, secret=secret)

                dynamodb.update_delivery_status(event_id, "SUCCESS",
                                                delivered_at=int(time.time() * 1000),
                                                attempts=event_payload.get("attempt", 1))
                if idempotency_key:
                    ttl = int(time.time()) + 3600
                    dynamodb.put_idempotency_marker(idempotency_key, ttl)
            except Exception as exc:
                LOGGER.exception("Delivery failure for event %s", event_id)
                dynamodb.update_delivery_status(event_id, "RETRY",
                                                last_error=str(exc),
                                                attempts=event_payload.get("attempt", 1) + 1)
                failures.append({"itemIdentifier": record.get("recordId", event_id or "unknown")})

    return {"batchItemFailures": failures}
