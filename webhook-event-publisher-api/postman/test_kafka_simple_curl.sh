#!/bin/bash

# Simple Kafka test using curl with JSON file
# Usage: ./test_kafka_simple_curl.sh

BASE_URL="${BASE_URL:-http://localhost:8080}"
TOPIC="wh.ingress.payments.transactionCreated"
JSON_FILE="postman/test_request_publisherCE.json"

echo "🚀 Testing Kafka Publishing..."
echo ""

# Check if JSON file exists
if [ ! -f "$JSON_FILE" ]; then
    echo "❌ ERROR: JSON file not found: $JSON_FILE"
    exit 1
fi

# Make request
echo "📤 Sending request to /webhook/event/publisherCE..."
echo "Using JSON file: $JSON_FILE"
echo ""

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$BASE_URL/webhook/event/publisherCE" \
  -H "Content-Type: application/json" \
  -d @"$JSON_FILE")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

echo "HTTP Status: $HTTP_CODE"
echo "Response Body:"
echo "$BODY" | jq . 2>/dev/null || echo "$BODY"
echo ""

if [ "$HTTP_CODE" = "202" ]; then
    echo "✅ Request successful!"
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
else
    echo "❌ Request failed (HTTP $HTTP_CODE)"
    echo ""
    echo "Troubleshooting:"
    echo "1. Check if DynamoDB schema is inserted:"
    echo "   aws dynamodb scan --table-name event_schema --endpoint-url http://localhost:8000"
    echo ""
    echo "2. Check application logs for detailed error"
    echo ""
    exit 1
fi

