# Quick Test Guide

## 1. Insert DynamoDB Schema Record

```bash
# For both endpoints (they use the same schema)
aws dynamodb put-item \
  --cli-input-json file://postman/dynamodb_schema_publisherCE.json \
  --endpoint-url http://localhost:8000
```

## 2. Test `/webhook/event/publisherCE` (CloudEvents Format)

**Request Body:**
```json
{
  "id": "F701560E-A31B-4748-927F-2655CAF785F9",
  "type": "com.bee.us.card.activation",
  "source": "payments",
  "subject": "transactionCreated",
  "specVersion": "1.0",
  "time": "2025-12-05T23:00:00Z",
  "data": {
    "transactionId": "txn-999",
    "customerId": "cust-001",
    "amount": 150.75,
    "currency": "USD",
    "status": "SUCCESS"
  }
}
```

**cURL Command:**
```bash
curl -X POST http://localhost:8080/webhook/event/publisherCE \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
  "id": "F701560E-A31B-4748-927F-2655CAF785F9",
  "type": "com.bee.us.card.activation",
  "source": "payments",
  "subject": "transactionCreated",
  "specVersion": "1.0",
  "time": "2025-12-05T23:00:00Z",
  "data": {
    "transactionId": "txn-999",
    "customerId": "cust-001",
    "amount": 150.75,
    "currency": "USD",
    "status": "SUCCESS"
  }
}
EOF
```

**Field Mapping:**
- `source` → `PRODUCER_DOMAIN` = "payments"
- `subject` → `EVENT_NAME` = "transactionCreated"
- `specVersion` → `VERSION` = "1.0"

## 3. Test `/webhook/event/publisher` (Generic Format)

**Request Body:**
```json
{
  "domain": "payments",
  "eventName": "transactionCreated",
  "version": "1.0",
  "data": {
    "transactionId": "txn-999",
    "customerId": "cust-001",
    "amount": 150.75,
    "currency": "USD",
    "status": "SUCCESS",
    "metadata": {
      "source": "mobile-app"
    }
  }
}
```

**cURL Command:**
```bash
curl -X POST http://localhost:8080/webhook/event/publisher \
  -H "Content-Type: application/json" \
  -d @- << 'EOF'
{
  "domain": "payments",
  "eventName": "transactionCreated",
  "version": "1.0",
  "data": {
    "transactionId": "txn-999",
    "customerId": "cust-001",
    "amount": 150.75,
    "currency": "USD",
    "status": "SUCCESS",
    "metadata": {
      "source": "mobile-app"
    }
  }
}
EOF
```

**Field Mapping:**
- `domain` → `PRODUCER_DOMAIN` = "payments"
- `eventName` → `EVENT_NAME` = "transactionCreated"
- `version` → `VERSION` = "1.0"

## 4. Verify Idempotency Ledger

```bash
aws dynamodb scan \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --endpoint-url http://localhost:8000
```

Expected record:
- `EVENT_ID`: Event ID from response
- `EVENT_STATUS`: `EVENT_READY_FOR_DELIVERY`
- `EVENT_SCHEMAID`: `SCHEMA_PAYMENTS_TRANSACTIONCREATED_1_0`

## Expected Success Response

```json
{
  "eventId": "F701560E-A31B-4748-927F-2655CAF785F9"
}
```

## Notes

- Both endpoints use the same DynamoDB schema record
- The schema record must have `EVENT_SCHEMA_DEFINITION` with a valid JSON Schema (not Avro)
- Set `webhooks.validation.enabled: false` in `application.yaml` to skip validation for testing

