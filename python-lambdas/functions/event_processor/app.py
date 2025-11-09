from __future__ import annotations

import base64
import json
from typing import Any, Dict, List

from webhooks_common import dynamodb, kafka
from webhooks_common.config import load_config
from webhooks_common.logging import LOGGER


def _decode_record(record: Dict[str, Any]) -> Dict[str, Any]:
    value = record.get("value")
    if value is None:
        raise ValueError("Missing record value")
    payload = base64.b64decode(value).decode()
    return json.loads(payload)


def handler(event, context):
    cfg = load_config()
    failures: List[Dict[str, str]] = []
    processed = 0
    failed = 0

    for _, records in event.get("records", {}).items():
        for record in records:
            record_id = record.get("recordId") or record.get("sequenceNumber", "unknown")
            try:
                payload = _decode_record(record)
                subscription_id = payload.get("subscription_id")
                if not subscription_id:
                    raise ValueError("subscription_id missing")

                subscription = dynamodb.fetch_subscription(subscription_id)
                if not subscription:
                    raise ValueError(f"Subscription {subscription_id} not found")

                partner_id = subscription["PK"].split("#")[0]
                target_topic = kafka.build_egress_topic(partner_id, payload.get("event_name", "unknown"))

                # In production, publish to MSK via Kafka Producer
                LOGGER.info(
                    "Publishing event %s to topic %s for subscription %s",
                    payload.get("event_id"),
                    target_topic,
                    subscription_id,
                )
                processed += 1
            except Exception as exc:
                LOGGER.error("Failed to process record", exc_info=exc)
                failures.append({"itemIdentifier": record_id})
                failed += 1

    return {
        "batchItemFailures": failures,
        "metrics": {
            "processed": processed,
            "failed": failed
        }
    }
