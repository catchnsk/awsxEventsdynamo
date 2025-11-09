from __future__ import annotations

import base64
import json
import time
from typing import Any, Dict

from jsonschema import Draft7Validator, SchemaError

from webhooks_common import config
from webhooks_common import dynamodb
from webhooks_common import kafka
from webhooks_common.logging import LOGGER


def _parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if "body" not in event:
        raise ValueError("Missing body")
    body = event["body"]
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()
    payload = json.loads(body)
    required_fields = ["domain", "event_name", "version", "schema"]
    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    return payload


def handler(event, context):
    try:
        payload = _parse_event(event)
        Draft7Validator.check_schema(json.loads(payload["schema"]))

        cfg = config.load_config()
        item = {
            "PK": f"{payload['domain']}#{payload['event_name']}#{payload['version']}",
            "schema": payload["schema"],
            "status": payload.get("status", "ACTIVE"),
            "updated_at": int(time.time() * 1000),
            "contains_sensitive": str(payload.get("contains_sensitive", False)),
        }
        dynamodb.put_schema_item(item)

        topic = kafka.build_ingress_topic(payload["domain"], payload["event_name"], payload["version"])
        kafka.ensure_topic(topic)

        return {
            "statusCode": 201,
            "body": json.dumps({"schemaId": item["PK"], "topic": topic})
        }
    except (ValueError, SchemaError) as exc:
        LOGGER.exception("Schema registration failed")
        return {"statusCode": 400, "body": json.dumps({"error": str(exc)})}
    except Exception as exc:
        LOGGER.exception("Unhandled error in schema admin")
        return {"statusCode": 500, "body": json.dumps({"error": str(exc)})}
