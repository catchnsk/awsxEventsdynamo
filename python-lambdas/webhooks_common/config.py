import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class AppConfig:
    event_schema_table: str
    subscription_table: str
    event_kafka_table: str
    delivery_status_table: str
    idempotency_table: str
    ingress_topic_prefix: str
    egress_topic_prefix: str
    region: str
    secret_arn: str | None = None


@lru_cache(maxsize=1)
def load_config() -> AppConfig:
    return AppConfig(
        event_schema_table=os.environ.get("EVENT_SCHEMA_TABLE", "event_schema"),
        subscription_table=os.environ.get("SUBSCRIPTION_TABLE", "partner_event_subscription"),
        event_kafka_table=os.environ.get("EVENT_KAFKA_TABLE", "event_subscription_kafka"),
        delivery_status_table=os.environ.get("DELIVERY_STATUS_TABLE", "event_delivery_status"),
        idempotency_table=os.environ.get("IDEMPOTENCY_TABLE", "idempotency_ledger"),
        ingress_topic_prefix=os.environ.get("INGRESS_TOPIC_PREFIX", "wh.ingress"),
        egress_topic_prefix=os.environ.get("EGRESS_TOPIC_PREFIX", "wh.egress"),
        region=os.environ.get("AWS_REGION", "us-east-1"),
        secret_arn=os.environ.get("WEBHOOK_SECRET_ARN"),
    )
