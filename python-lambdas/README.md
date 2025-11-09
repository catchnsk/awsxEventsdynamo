# Python Lambda Components

Implements the Lambda flows from `05-python-lambdas-plan.md`. Each function uses Python 3.11 and shares utilities from `webhooks_common` (logging, schema helpers, DynamoDB/MSK wrappers, HTTP client, retry helpers).

## Layout
```
python-lambdas/
  requirements.txt
  webhooks_common/
    __init__.py
    config.py
    logging.py
    dynamodb.py
    kafka.py
    schema.py
    http.py
    retry.py
  functions/
    schema_admin/app.py
    subscription_admin/app.py
    event_processor/app.py
    webhook_dispatcher/app.py
  tests/
    test_schema_admin.py
```

## Functions
| Function | Handler | Purpose |
| --- | --- | --- |
| Schema Registration Admin | `functions/schema_admin/app.handler` | Validates JSON schema docs, writes to `event_schema`, ensures ingress topic |
| Subscription Admin | `functions/subscription_admin/app.handler` | Registers partner subscriptions, stores delivery metadata, ensures egress topic |
| Event Processor & Filter | `functions/event_processor/app.handler` | Consumes ingress MSK batches, enriches payload, routes to partner/egress topics |
| Webhook Dispatcher | `functions/webhook_dispatcher/app.handler` | Sends HTTP requests to partner endpoints with retries/idempotency tracking |

All functions import shared helpers from `webhooks_common`.

## Local Setup
```bash
cd python-lambdas
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r dev-requirements.txt
pytest
```

A `dev-requirements.txt` is optional; use `pip install -r requirements.txt` for Lambda packaging.

### Key Environment Variables
| Variable | Default | Description |
| --- | --- | --- |
| `EVENT_SCHEMA_TABLE` | `event_schema` | DynamoDB table storing schema versions |
| `SUBSCRIPTION_TABLE` | `partner_event_subscription` | Partner subscription metadata |
| `EVENT_KAFKA_TABLE` | `event_subscription_kafka` | Kafka routing metadata |
| `DELIVERY_STATUS_TABLE` | `event_delivery_status` | Delivery audit records |
| `IDEMPOTENCY_TABLE` | `idempotency_ledger` | Tracks dispatched events to prevent duplicates |
| `INGRESS_TOPIC_PREFIX` | `wh.ingress` | Prefix for ingress MSK topics |
| `EGRESS_TOPIC_PREFIX` | `wh.egress` | Prefix for partner egress topics |
| `WEBHOOK_SECRET_ARN` | _None_ | Secrets Manager ARN for dispatcher signing key (optional) |

## Deployment
Each function can be packaged via AWS SAM/Serverless/Terraform. Zip the relevant handler plus `webhooks_common` folder (as layer or bundled) and upload to S3 for Lambda. Environment variables configure table names, MSK topic prefixes, AppConfig, Secrets Manager ARNs, etc.
