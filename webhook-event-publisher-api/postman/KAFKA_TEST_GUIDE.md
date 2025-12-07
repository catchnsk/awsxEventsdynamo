# Kafka Publishing Test Guide

This guide helps you verify that messages are being published to Kafka successfully.

## Prerequisites

1. **Kafka Running**: Kafka should be running on `localhost:9092`
2. **DynamoDB Schema**: Schema record must be inserted (see `dynamodb_schema_publisherCE.json`)
3. **Spring Boot App**: Application should be running on `localhost:8080`

## Quick Test Methods

### Method 1: Using Docker (Recommended)

If you have Kafka running in Docker:

```bash
# 1. Start a consumer in one terminal
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic wh.ingress.payments.transactionCreated \
  --from-beginning

# 2. In another terminal, make a test request
curl -X POST http://localhost:8080/webhook/event/publisherCE \
  -H "Content-Type: application/json" \
  -d '{
    "id": "TEST-12345",
    "type": "com.bee.us.card.activation",
    "source": "payments",
    "subject": "transactionCreated",
    "specVersion": "1.0",
    "time": "2025-12-06T10:00:00Z",
    "data": {
      "transactionId": "txn-999",
      "customerId": "cust-001",
      "amount": 150.75,
      "currency": "USD",
      "status": "SUCCESS"
    }
  }'

# 3. You should see the message appear in the consumer terminal
```

### Method 2: Using kafkacat/kcat

```bash
# 1. Install kafkacat/kcat (if not installed)
# macOS: brew install kafkacat
# Linux: apt-get install kafkacat

# 2. Start consumer
kcat -b localhost:9092 -t wh.ingress.payments.transactionCreated -C -o beginning

# 3. Make test request (same as Method 1)
# 4. Message should appear in consumer
```

### Method 3: Using Test Script

```bash
# Make script executable
chmod +x postman/test_kafka_simple.sh

# Run test
./postman/test_kafka_simple.sh
```

## Topic Name Format

The topic name is constructed as:
```
{ingressTopicPrefix}.{domain}.{eventName}
```

For our test:
- `ingressTopicPrefix`: `wh.ingress` (from `application.yaml`)
- `domain`: `payments` (from request `source` or `domain`)
- `eventName`: `transactionCreated` (from request `subject` or `eventName`)

**Result**: `wh.ingress.payments.transactionCreated`

## Expected Message Format

The message published to Kafka will be a JSON string containing the event data:

```json
{
  "transactionId": "txn-999",
  "customerId": "cust-001",
  "amount": 150.75,
  "currency": "USD",
  "status": "SUCCESS"
}
```

## Verification Steps

### 1. Check if Topic Exists

```bash
# Using Docker
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Using kafka tools (if installed)
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 2. Check Topic Details

```bash
# Using Docker
docker exec -it kafka kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic wh.ingress.payments.transactionCreated
```

### 3. Consume Messages

```bash
# Consume from beginning
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic wh.ingress.payments.transactionCreated \
  --from-beginning

# Consume latest messages only
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic wh.ingress.payments.transactionCreated
```

### 4. Check Message Count

```bash
# Get offset information
docker exec -it kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic wh.ingress.payments.transactionCreated
```

## Troubleshooting

### No Messages Appearing

1. **Check Application Logs**: Look for Kafka publishing errors
   ```bash
   # Check application logs for errors
   tail -f logs/application.log | grep -i kafka
   ```

2. **Verify Kafka Connection**: Check if application can connect to Kafka
   ```bash
   # Test Kafka connection
   nc -zv localhost 9092
   ```

3. **Check Topic Auto-Creation**: Kafka may need auto-creation enabled
   ```bash
   # Check Kafka server.properties
   # auto.create.topics.enable should be true
   ```

4. **Verify Schema**: Ensure DynamoDB schema exists
   ```bash
   aws dynamodb scan --table-name event_schema --endpoint-url http://localhost:8000
   ```

### Connection Errors

If you see connection errors:

1. **Kafka Not Running**: Start Kafka
   ```bash
   docker run -d --name kafka -p 9092:9092 apache/kafka:latest
   ```

2. **Wrong Bootstrap Server**: Check `application.yaml`
   ```yaml
   webhooks:
     kafka:
       bootstrap-servers: localhost:9092
   ```

3. **Network Issues**: Ensure Kafka is accessible
   ```bash
   telnet localhost 9092
   ```

### Message Format Issues

If messages appear but format is wrong:

1. **Check Validation**: Ensure `webhooks.validation.enabled` is set correctly
2. **Check Schema**: Verify `EVENT_SCHEMA_DEFINITION` contains valid JSON Schema
3. **Check Logs**: Review application logs for validation errors

## Testing Both Endpoints

### Test `/webhook/event/publisherCE`

```bash
curl -X POST http://localhost:8080/webhook/event/publisherCE \
  -H "Content-Type: application/json" \
  -d '{
    "id": "TEST-CE-001",
    "type": "com.bee.us.card.activation",
    "source": "payments",
    "subject": "transactionCreated",
    "specVersion": "1.0",
    "time": "2025-12-06T10:00:00Z",
    "data": {
      "transactionId": "txn-999",
      "customerId": "cust-001",
      "amount": 150.75,
      "currency": "USD",
      "status": "SUCCESS"
    }
  }'
```

### Test `/webhook/event/publisher`

```bash
curl -X POST http://localhost:8080/webhook/event/publisher \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "payments",
    "eventName": "transactionCreated",
    "version": "1.0",
    "data": {
      "transactionId": "txn-999",
      "customerId": "cust-001",
      "amount": 150.75,
      "currency": "USD",
      "status": "SUCCESS"
    }
  }'
```

Both should publish to the same topic: `wh.ingress.payments.transactionCreated`

## Success Indicators

✅ **HTTP 202 Response**: Request accepted
✅ **Message in Kafka**: Message appears in consumer
✅ **Idempotency Ledger**: Record created in `EVENT_IDEMPOTENCY_LEDGER` table
✅ **No Errors in Logs**: Application logs show successful publish

## Next Steps

After verifying Kafka publishing works:

1. **Check Idempotency Ledger**: Verify records are created
   ```bash
   aws dynamodb scan --table-name EVENT_IDEMPOTENCY_LEDGER --endpoint-url http://localhost:8000
   ```

2. **Monitor Consumer Lag**: Check if consumers are processing messages
3. **Test Error Scenarios**: Test with invalid data to verify error handling
4. **Performance Testing**: Test with multiple concurrent requests

