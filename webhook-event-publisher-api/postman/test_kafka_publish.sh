#!/bin/bash

# Test script to verify Kafka message publishing
# This script will:
# 1. Check if Kafka is running
# 2. Create the topic if needed
# 3. Start a consumer in the background
# 4. Make a test request
# 5. Verify the message was received

set -e

KAFKA_BOOTSTRAP="localhost:9092"
TOPIC="wh.ingress.payments.transactionCreated"
BASE_URL="${BASE_URL:-http://localhost:8080}"
CONSUMER_GROUP="test-consumer-group-$(date +%s)"

echo "========================================="
echo "Kafka Publishing Test"
echo "========================================="
echo "Kafka Bootstrap: $KAFKA_BOOTSTRAP"
echo "Topic: $TOPIC"
echo "Base URL: $BASE_URL"
echo "Consumer Group: $CONSUMER_GROUP"
echo ""

# Check if Kafka is running
echo "Step 1: Checking if Kafka is running..."
if ! nc -z localhost 9092 2>/dev/null; then
    echo "❌ ERROR: Kafka is not running on localhost:9092"
    echo "Please start Kafka first:"
    echo "  docker run -d --name kafka -p 9092:9092 apache/kafka:latest"
    exit 1
fi
echo "✅ Kafka is running"
echo ""

# Check if kafka-console-consumer is available
KAFKA_CONSUMER=""
if command -v kafka-console-consumer.sh &> /dev/null; then
    KAFKA_CONSUMER="kafka-console-consumer.sh"
elif command -v kafka-console-consumer &> /dev/null; then
    KAFKA_CONSUMER="kafka-console-consumer"
else
    echo "⚠️  WARNING: kafka-console-consumer not found in PATH"
    echo "You may need to install Kafka or add it to PATH"
    echo "Alternatively, you can use Docker:"
    echo "  docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Create topic if it doesn't exist (optional, Kafka auto-creates topics by default)
echo "Step 2: Ensuring topic exists..."
if [ -n "$KAFKA_CONSUMER" ]; then
    # Try to create topic (may fail if it exists, that's OK)
    kafka-topics.sh --create --bootstrap-server $KAFKA_BOOTSTRAP \
        --topic $TOPIC --partitions 1 --replication-factor 1 2>/dev/null || true
    echo "✅ Topic ready: $TOPIC"
else
    echo "⚠️  Skipping topic creation (kafka tools not available)"
fi
echo ""

# Start consumer in background
echo "Step 3: Starting Kafka consumer..."
CONSUMER_OUTPUT="/tmp/kafka_test_output_$$.txt"
rm -f "$CONSUMER_OUTPUT"

if [ -n "$KAFKA_CONSUMER" ]; then
    $KAFKA_CONSUMER --bootstrap-server $KAFKA_BOOTSTRAP \
        --topic $TOPIC \
        --group $CONSUMER_GROUP \
        --from-beginning \
        --timeout-ms 10000 \
        --max-messages 1 > "$CONSUMER_OUTPUT" 2>&1 &
    CONSUMER_PID=$!
    echo "✅ Consumer started (PID: $CONSUMER_PID)"
    sleep 2
else
    echo "⚠️  Consumer not started (kafka tools not available)"
    echo "   You can manually run:"
    echo "   docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning"
fi
echo ""

# Make test request
echo "Step 4: Making test request to /webhook/event/publisherCE..."
EVENT_ID="TEST-$(date +%s)-$(uuidgen | cut -d'-' -f1)"
REQUEST_BODY=$(cat <<EOF
{
  "id": "$EVENT_ID",
  "type": "com.bee.us.card.activation",
  "source": "payments",
  "subject": "transactionCreated",
  "specVersion": "1.0",
  "time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "data": {
    "transactionId": "txn-test-$(date +%s)",
    "customerId": "cust-001",
    "amount": 150.75,
    "currency": "USD",
    "status": "SUCCESS"
  }
}
EOF
)

echo "Request Body:"
echo "$REQUEST_BODY" | jq .
echo ""

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/webhook/event/publisherCE" \
  -H "Content-Type: application/json" \
  -d "$REQUEST_BODY")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

echo "HTTP Status: $HTTP_CODE"
echo "Response Body:"
echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
echo ""

if [ "$HTTP_CODE" != "202" ]; then
    echo "❌ ERROR: Request failed with status $HTTP_CODE"
    if [ -n "$CONSUMER_PID" ]; then
        kill $CONSUMER_PID 2>/dev/null || true
    fi
    exit 1
fi

echo "✅ Request successful"
echo ""

# Wait a bit for message to be published
echo "Step 5: Waiting for message to be published..."
sleep 3

# Check consumer output
if [ -n "$KAFKA_CONSUMER" ] && [ -f "$CONSUMER_OUTPUT" ]; then
    if [ -s "$CONSUMER_OUTPUT" ]; then
        echo "✅ Message received in Kafka!"
        echo ""
        echo "Message content:"
        cat "$CONSUMER_OUTPUT"
        echo ""
    else
        echo "⚠️  No message received yet (consumer may still be waiting)"
        echo "   Check manually with:"
        echo "   docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning"
    fi
    kill $CONSUMER_PID 2>/dev/null || true
    rm -f "$CONSUMER_OUTPUT"
else
    echo "⚠️  Could not verify message (consumer not available)"
    echo "   Please check manually:"
    echo "   docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning"
fi

echo ""
echo "========================================="
echo "Test Complete"
echo "========================================="
echo "Event ID: $EVENT_ID"
echo "Topic: $TOPIC"
echo ""

