from __future__ import annotations

import base64
import json
import time
from typing import Any, Dict

from webhooks_common import config, dynamodb, kafka
from webhooks_common.logging import LOGGER


def _parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if "body" not in event:
        raise ValueError("Missing body")
    body = event["body"]
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()
    payload = json.loads(body)
    required = ["partner_id", "domain", "event_name", "version", "delivery_url"]
    missing = [key for key in required if key not in payload]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    return payload


def handler(event, context):
    try:
        payload = _parse_event(event)
        subscription_id = f"{payload['partner_id']}#{payload['domain']}#{payload['event_name']}#{payload['version']}"
        item = {
            "PK": subscription_id,
            "delivery_url": payload["delivery_url"],
            "status": payload.get("status", "Pending"),
            "auth": payload.get("auth", {}),
            "created_at": int(time.time() * 1000),
        }
        dynamodb.put_subscription_item(item)

        topic = kafka.build_egress_topic(payload["partner_id"], payload["event_name"])
        kafka.ensure_topic(topic)

        return {
            "statusCode": 201,
            "body": json.dumps({"subscriptionId": subscription_id, "topic": topic})
        }
    except ValueError as exc:
        LOGGER.exception("Subscription validation error")
        return {"statusCode": 400, "body": json.dumps({"error": str(exc)})}
    except Exception as exc:
        LOGGER.exception("Subscription admin failure")
        return {"statusCode": 500, "body": json.dumps({"error": str(exc)})}
