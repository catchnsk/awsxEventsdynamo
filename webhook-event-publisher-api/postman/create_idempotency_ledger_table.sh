#!/bin/bash

# Create EVENT_IDEMPOTENCY_LEDGER table with GSI
# Table: event-idempotency-ledger
# Partition Key: eventId (String)
# GSI: event-status-index (eventId HASH, eventStatus RANGE)

TABLE_NAME="event-idempotency-ledger0"
ENDPOINT_URL="http://localhost:8000"

echo "Creating table: $TABLE_NAME"
echo "Partition Key: eventId (String, HASH)"
echo "GSI: event-status-index (eventId HASH, eventStatus RANGE)"
echo ""

aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
    AttributeName=eventId,AttributeType=S \
    AttributeName=eventStatus,AttributeType=S \
  --key-schema \
    AttributeName=eventId,KeyType=HASH \
  --global-secondary-indexes \
    "[
      {
        \"IndexName\": \"event-status-index\",
        \"KeySchema\": [
          {\"AttributeName\": \"eventId\", \"KeyType\": \"HASH\"},
          {\"AttributeName\": \"eventStatus\", \"KeyType\": \"RANGE\"}
        ],
        \"Projection\": {
          \"ProjectionType\": \"ALL\"
        }
      }
    ]" \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url "$ENDPOINT_URL"

echo ""
echo "Waiting for table to be active..."
aws dynamodb wait table-exists \
  --table-name "$TABLE_NAME" \
  --endpoint-url "$ENDPOINT_URL"

echo ""
echo "Table created successfully!"
echo ""
echo "Verifying table structure..."
aws dynamodb describe-table \
  --table-name "$TABLE_NAME" \
  --endpoint-url "$ENDPOINT_URL" \
  --query 'Table.[TableName,KeySchema,GlobalSecondaryIndexes[0].[IndexName,KeySchema]]' \
  --output json

