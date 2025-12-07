# Test Instructions for Event Publishing Endpoints

This document provides instructions for testing the `/webhook/event/publisherCE` and `/webhook/event/publisher` endpoints.

## Prerequisites

1. DynamoDB Local running on port 8000
2. Kafka running (if you want to test actual publishing)
3. Spring Boot application running on port 8080 (default)

## Step 1: Create DynamoDB Table (if not exists)

```bash
aws dynamodb create-table \
  --table-name event_schema \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://localhost:8000
```

## Step 2: Insert Schema Records

### For `/webhook/event/publisherCE` endpoint:

```bash
aws dynamodb put-item \
  --cli-input-json file://postman/dynamodb_schema_publisherCE.json \
  --endpoint-url http://localhost:8000
```

**Important Mapping:**
- `source` in CloudEvent → `PRODUCER_DOMAIN` in DynamoDB
- `subject` in CloudEvent → `EVENT_NAME` in DynamoDB
- `specVersion` in CloudEvent → `VERSION` in DynamoDB

### For `/webhook/event/publisher` endpoint:

```bash
aws dynamodb put-item \
  --cli-input-json file://postman/dynamodb_schema_publisher.json \
  --endpoint-url http://localhost:8000
```

**Note:** Both endpoints can use the same schema record since they both use:
- `PRODUCER_DOMAIN`: "payments"
- `EVENT_NAME`: "transactionCreated"
- `VERSION`: "1.0"

## Step 3: Test with Postman

### Option 1: Import Postman Collection

1. Import `test_publisherCE.json` and `test_publisher.json` into Postman
2. Set the `baseUrl` variable to `http://localhost:8080`
3. Run the requests

### Option 2: Use cURL

#### Test `/webhook/event/publisherCE`:

```bash
curl -X POST http://localhost:8080/webhook/event/publisherCE \
  -H "Content-Type: application/json" \
  -H "X-Event-Id: F701560E-A31B-4748-927F-2655CAF785F9" \
  -H "Idempotency-Key: idemp-key-12345" \
  -d '{
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
  }'
```

#### Test `/webhook/event/publisher`:

```bash
curl -X POST http://localhost:8080/webhook/event/publisher \
  -H "Content-Type: application/json" \
  -H "X-Event-Id: 550e8400-e29b-41d4-a716-446655440000" \
  -H "Idempotency-Key: idemp-key-67890" \
  -d '{
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
  }'
```

## Expected Responses

### Success Response (202 Accepted):

```json
{
  "eventId": "F701560E-A31B-4748-927F-2655CAF785F9"
}
```

### Error Response (400 Bad Request):

```json
{
  "timestamp": "2025-12-05T23:00:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed: ...",
  "path": "/webhook/event/publisherCE"
}
```

## Verify Idempotency Ledger

After successful publishing, check the `EVENT_IDEMPOTENCY_LEDGER` table:

```bash
aws dynamodb scan \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --endpoint-url http://localhost:8000
```

You should see records with:
- `EVENT_ID`: The event ID from the response
- `EVENT_STATUS`: `EVENT_READY_FOR_DELIVERY`
- `EVENT_SCHEMAID`: `SCHEMA_PAYMENTS_TRANSACTIONCREATED_1_0`

## Testing with Validation Disabled

If you want to skip validation (for testing Kafka publishing only), set in `application.yaml`:

```yaml
webhooks:
  validation:
    enabled: false
```

## Troubleshooting

### Schema Not Found (404)
- Verify the schema record exists in DynamoDB
- Check that `PRODUCER_DOMAIN`, `EVENT_NAME`, and `VERSION` match exactly
- For `/webhook/event/publisherCE`: Check that `source`, `subject`, and `specVersion` map correctly

### Validation Failed (400)
- Verify the `data` field matches the JSON Schema in `EVENT_SCHEMA_DEFINITION`
- Check that required fields are present
- Ensure data types match the schema

### Kafka Unavailable (503)
- Verify Kafka is running
- Check `webhooks.kafka.bootstrap-servers` configuration
- Review application logs for connection errors

