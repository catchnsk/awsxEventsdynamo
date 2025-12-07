#!/bin/bash

# Simple Kafka test using Docker
# This script uses Docker to consume messages from Kafka

KAFKA_BOOTSTRAP="localhost:9092"
TOPIC="wh.ingress.payments.transactionCreated"
BASE_URL="${BASE_URL:-http://localhost:8080}"

echo "========================================="
echo "Simple Kafka Publishing Test"
echo "========================================="
echo ""

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ ERROR: Docker is not installed or not in PATH"
    exit 1
fi

# Check if Kafka container is running
if ! docker ps | grep -q kafka; then
    echo "⚠️  WARNING: Kafka container not found"
    echo "Starting Kafka with Docker..."
    docker run -d --name kafka-test -p 9092:9092 \
        -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
        apache/kafka:latest || {
        echo "❌ Failed to start Kafka container"
        echo "Please start Kafka manually"
        exit 1
    }
    sleep 10
fi

echo "Step 1: Starting Kafka consumer in background..."
echo "Topic: $TOPIC"
echo ""

# Start consumer in background
docker exec -d kafka-test kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC \
    --from-beginning \
    --timeout-ms 10000 \
    --max-messages 1 &

CONSUMER_PID=$!
echo "✅ Consumer started"
echo ""

# Make test request
echo "Step 2: Making test request..."
EVENT_ID="TEST-$(date +%s)"
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

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/webhook/event/publisherCE" \
  -H "Content-Type: application/json" \
  -d "$REQUEST_BODY")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

echo "HTTP Status: $HTTP_CODE"
if [ "$HTTP_CODE" = "202" ]; then
    echo "✅ Request successful"
    echo "Response: $BODY"
else
    echo "❌ Request failed"
    echo "Response: $BODY"
    exit 1
fi

echo ""
echo "Step 3: Waiting for message to be published..."
sleep 3

echo ""
echo "Step 4: Checking Kafka for messages..."
echo "Run this command to see messages:"
echo "  docker exec -it kafka-test kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $TOPIC --from-beginning"
echo ""
echo "Or use kafkacat/kcat:"
echo "  kcat -b localhost:9092 -t $TOPIC -C -o beginning"
echo ""

