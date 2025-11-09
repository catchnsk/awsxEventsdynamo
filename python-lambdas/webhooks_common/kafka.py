from __future__ import annotations

from .logging import LOGGER


def ensure_topic(name: str, partitions: int = 6):
    """
    Placeholder for MSK topic management.
    In production we would call Kafka Admin API or AWS MSK APIs via kafka-cluster permissions.
    Here we simply log intent so IaC/Lambda can hook into kafka admin tooling.
    """
    LOGGER.info("Ensuring topic %s with %s partitions", name, partitions)


def build_ingress_topic(domain: str, event: str, version: str) -> str:
    from .config import load_config

    cfg = load_config()
    return f"{cfg.ingress_topic_prefix}.{domain}.{event}.{version}"


def build_egress_topic(partner: str, event: str) -> str:
    from .config import load_config

    cfg = load_config()
    return f"{cfg.egress_topic_prefix}.{partner}.{event}"
