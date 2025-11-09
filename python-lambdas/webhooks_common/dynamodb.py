from __future__ import annotations

from typing import Any, Dict, Mapping

import boto3
from boto3.dynamodb.conditions import Key

from .config import load_config


def _client():
    return boto3.resource("dynamodb", region_name=load_config().region)


def table(name: str):
    return _client().Table(name)


def put_schema_item(item: Mapping[str, Any]):
    cfg = load_config()
    table(cfg.event_schema_table).put_item(Item=dict(item))


def put_subscription_item(item: Mapping[str, Any]):
    cfg = load_config()
    table(cfg.subscription_table).put_item(Item=dict(item))


def update_delivery_status(event_id: str, status: str, **attrs):
    cfg = load_config()
    tbl = table(cfg.delivery_status_table)
    update_expression = "SET event_status = :s"
    expression_attrs = {":s": status}
    for idx, (key, value) in enumerate(attrs.items()):
        placeholder = f":attr{idx}"
        update_expression += f", {key} = {placeholder}"
        expression_attrs[placeholder] = value
    tbl.update_item(
        Key={"WEBHOOK_EVENT_UUID": event_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attrs,
    )


def fetch_subscription(subscription_id: str) -> Dict[str, Any] | None:
    cfg = load_config()
    response = table(cfg.subscription_table).get_item(Key={"PK": subscription_id})
    return response.get("Item")


def put_idempotency_marker(key: str, ttl: int):
    cfg = load_config()
    table(cfg.idempotency_table).put_item(Item={"IDEMPOTENCY_KEY": key, "expires_at": ttl})


def check_idempotency(key: str) -> bool:
    cfg = load_config()
    response = table(cfg.idempotency_table).get_item(Key={"IDEMPOTENCY_KEY": key})
    return "Item" in response
