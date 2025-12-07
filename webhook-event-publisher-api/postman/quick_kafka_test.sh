#!/bin/bash

# Quick Kafka test - Run this to test if messages are published to Kafka
# Usage: ./quick_kafka_test.sh

BASE_URL="${BASE_URL:-http://localhost:8080}"
TOPIC="wh.ingress.payments.transactionCreated"

# Generate a valid UUID for the event ID
if command -v uuidgen &> /dev/null; then
    EVENT_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
elif command -v python3 &> /dev/null; then
    EVENT_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
else
    # Fallback: use a fixed test UUID
    EVENT_ID="f701560e-a31b-4748-927f-2655caf785f9"
fi

# Generate ISO 8601 timestamp with timezone offset
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "🚀 Testing Kafka Publishing..."
echo "Event ID: $EVENT_ID"
echo ""

# Make request
echo "📤 Sending request to /webhook/event/publisherCE..."
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/webhook/event/publisherCE" \
  -H "Content-Type: application/json" \
  -d "{
    \"id\": \"$EVENT_ID\",
    \"type\": \"com.bee.us.card.activation\",
    \"source\": \"payments\",
    \"subject\": \"transactionCreated\",
    \"specVersion\": \"1.0\",
    \"time\": \"$TIMESTAMP\",
    \"data\": {
      \"transactionId\": \"txn-test-$(date +%s)\",
      \"customerId\": \"cust-001\",
      \"amount\": 150.75,
      \"currency\": \"USD\",
      \"status\": \"SUCCESS\"
    }
  }")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" = "202" ]; then
    echo "✅ Request successful (HTTP $HTTP_CODE)"
    echo "   Event ID: $EVENT_ID"
    echo "   Response: $BODY"
    echo ""
    echo "📥 To verify message in Kafka, run:"
    echo ""
    echo "   # Using Docker:"
    echo "   docker exec -it kafka kafka-console-consumer.sh \\"
    echo "     --bootstrap-server localhost:9092 \\"
    echo "     --topic $TOPIC \\"
    echo "     --from-beginning"
    echo ""
    echo "   # Or using kcat:"
    echo "   kcat -b localhost:9092 -t $TOPIC -C -o beginning"
    echo ""
    echo "   # Or check recent messages:"
    echo "   docker exec -it kafka kafka-console-consumer.sh \\"
    echo "     --bootstrap-server localhost:9092 \\"
    echo "     --topic $TOPIC"
    echo ""
else
    echo "❌ Request failed (HTTP $HTTP_CODE)"
    echo "Response: $BODY"
    exit 1
fi

