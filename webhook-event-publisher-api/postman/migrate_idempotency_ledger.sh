#!/bin/bash

# Migration script to rename EVENT_ID to eventID and EVENT_STATUS to eventStatus
# in the EVENT_IDEMPOTENCY_LEDGER DynamoDB table

TABLE_NAME="EVENT_IDEMPOTENCY_LEDGER"
ENDPOINT_URL="http://localhost:8000"

echo "Starting migration of $TABLE_NAME table..."
echo "Renaming: EVENT_ID -> eventID, EVENT_STATUS -> eventStatus"
echo ""

# Check if table exists
echo "Checking if table exists..."
aws dynamodb describe-table \
  --table-name "$TABLE_NAME" \
  --endpoint-url "$ENDPOINT_URL" > /dev/null 2>&1

if [ $? -ne 0 ]; then
  echo "Error: Table $TABLE_NAME does not exist!"
  echo "Please create the table first."
  exit 1
fi

echo "Table found. Starting migration..."
echo ""

# Get table key schema to check if EVENT_ID is the partition key
KEY_SCHEMA=$(aws dynamodb describe-table \
  --table-name "$TABLE_NAME" \
  --endpoint-url "$ENDPOINT_URL" \
  --query 'Table.KeySchema' \
  --output json)

echo "Current table key schema:"
echo "$KEY_SCHEMA" | jq '.'
echo ""

# Check if EVENT_ID is the partition key
IS_PK=$(echo "$KEY_SCHEMA" | jq -r '.[] | select(.AttributeName == "EVENT_ID" and .KeyType == "HASH") | .AttributeName')

if [ "$IS_PK" = "EVENT_ID" ]; then
  echo "WARNING: EVENT_ID is the partition key!"
  echo "To rename the partition key, you need to:"
  echo "1. Create a new table with 'eventID' as the partition key"
  echo "2. Migrate all data"
  echo "3. Delete the old table"
  echo ""
  read -p "Do you want to proceed with recreating the table? (yes/no): " confirm
  
  if [ "$confirm" != "yes" ]; then
    echo "Migration cancelled."
    exit 0
  fi
  
  echo ""
  echo "Step 1: Creating new table with renamed key..."
  aws dynamodb create-table \
    --table-name "${TABLE_NAME}_new" \
    --attribute-definitions \
      AttributeName=eventID,AttributeType=S \
    --key-schema \
      AttributeName=eventID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --endpoint-url "$ENDPOINT_URL"
  
  echo "Waiting for table to be active..."
  aws dynamodb wait table-exists \
    --table-name "${TABLE_NAME}_new" \
    --endpoint-url "$ENDPOINT_URL"
  
  echo "Step 2: Migrating data..."
  
  # Scan all items and migrate
  aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --endpoint-url "$ENDPOINT_URL" \
    --output json | jq -c '.Items[]' | while read -r item; do
      
      # Extract values
      EVENT_ID=$(echo "$item" | jq -r '.EVENT_ID.S // empty')
      EVENT_STATUS=$(echo "$item" | jq -r '.EVENT_STATUS.S // empty')
      EVENT_SCHEMAID=$(echo "$item" | jq -r '.EVENT_SCHEMAID.S // empty')
      CREATION_TIMESTAMP=$(echo "$item" | jq -r '.CREATION_TIMESTAMP.S // empty')
      UPDATED_TIMESTAMP=$(echo "$item" | jq -r '.UPDATED_TIMESTAMP.S // empty')
      
      if [ -z "$EVENT_ID" ]; then
        echo "Skipping item without EVENT_ID"
        continue
      fi
      
      # Build new item with renamed attributes
      NEW_ITEM=$(jq -n \
        --arg eventID "$EVENT_ID" \
        --arg eventStatus "$EVENT_STATUS" \
        --arg schemaId "$EVENT_SCHEMAID" \
        --arg creationTs "$CREATION_TIMESTAMP" \
        --arg updatedTs "$UPDATED_TIMESTAMP" \
        '{
          "eventID": {"S": $eventID},
          "eventStatus": {"S": $eventStatus},
          "CREATION_TIMESTAMP": {"S": ($creationTs // "")},
          "UPDATED_TIMESTAMP": {"S": ($updatedTs // "")}
        } + (if $schemaId != "" and $schemaId != "null" then {"EVENT_SCHEMAID": {"S": $schemaId}} else {} end)')
      
      # Put item in new table
      aws dynamodb put-item \
        --table-name "${TABLE_NAME}_new" \
        --item "$NEW_ITEM" \
        --endpoint-url "$ENDPOINT_URL" > /dev/null
      
      echo "Migrated: eventID=$EVENT_ID, eventStatus=$EVENT_STATUS"
    done
  
  echo ""
  echo "Step 3: Verifying migration..."
  OLD_COUNT=$(aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --endpoint-url "$ENDPOINT_URL" \
    --select COUNT \
    --query 'Count' \
    --output text)
  
  NEW_COUNT=$(aws dynamodb scan \
    --table-name "${TABLE_NAME}_new" \
    --endpoint-url "$ENDPOINT_URL" \
    --select COUNT \
    --query 'Count' \
    --output text)
  
  echo "Old table count: $OLD_COUNT"
  echo "New table count: $NEW_COUNT"
  
  if [ "$OLD_COUNT" = "$NEW_COUNT" ]; then
    echo ""
    echo "Migration successful! Counts match."
    echo ""
    read -p "Do you want to delete the old table and rename the new one? (yes/no): " confirm2
    
    if [ "$confirm2" = "yes" ]; then
      echo "Deleting old table..."
      aws dynamodb delete-table \
        --table-name "$TABLE_NAME" \
        --endpoint-url "$ENDPOINT_URL"
      
      echo "Waiting for table to be deleted..."
      aws dynamodb wait table-not-exists \
        --table-name "$TABLE_NAME" \
        --endpoint-url "$ENDPOINT_URL"
      
      echo "Renaming new table..."
      # Note: DynamoDB doesn't support rename, so we'll create with original name
      aws dynamodb create-table \
        --table-name "$TABLE_NAME" \
        --attribute-definitions \
          AttributeName=eventID,AttributeType=S \
        --key-schema \
          AttributeName=eventID,KeyType=HASH \
        --billing-mode PAY_PER_REQUEST \
        --endpoint-url "$ENDPOINT_URL"
      
      aws dynamodb wait table-exists \
        --table-name "$TABLE_NAME" \
        --endpoint-url "$ENDPOINT_URL"
      
      # Copy data from _new to original
      aws dynamodb scan \
        --table-name "${TABLE_NAME}_new" \
        --endpoint-url "$ENDPOINT_URL" \
        --output json | jq -c '.Items[]' | while read -r item; do
        aws dynamodb put-item \
          --table-name "$TABLE_NAME" \
          --item "$item" \
          --endpoint-url "$ENDPOINT_URL" > /dev/null
      done
      
      # Delete _new table
      aws dynamodb delete-table \
        --table-name "${TABLE_NAME}_new" \
        --endpoint-url "$ENDPOINT_URL"
      
      echo "Migration complete! Table $TABLE_NAME now uses eventID as partition key."
    else
      echo "Old table kept. New table is: ${TABLE_NAME}_new"
    fi
  else
    echo "WARNING: Count mismatch! Please verify manually."
  fi
  
else
  # EVENT_ID is not the partition key, so we can just update items
  echo "EVENT_ID is not the partition key. Updating items in place..."
  echo ""
  
  # Scan all items and update
  ITEM_COUNT=0
  aws dynamodb scan \
    --table-name "$TABLE_NAME" \
    --endpoint-url "$ENDPOINT_URL" \
    --output json | jq -c '.Items[]' | while read -r item; do
    
    # Extract key attributes (assuming EVENT_ID might be used as key or we need to identify the actual key)
    # Get the actual key from the table schema
    KEY_ATTR=$(echo "$KEY_SCHEMA" | jq -r '.[0].AttributeName')
    
    # Extract key value
    KEY_VALUE=$(echo "$item" | jq -r --arg key "$KEY_ATTR" '.[$key].S // .[$key].N // empty')
    
    if [ -z "$KEY_VALUE" ]; then
      echo "Skipping item without key: $KEY_ATTR"
      continue
    fi
    
    # Extract values
    EVENT_ID=$(echo "$item" | jq -r '.EVENT_ID.S // empty')
    EVENT_STATUS=$(echo "$item" | jq -r '.EVENT_STATUS.S // empty')
    EVENT_SCHEMAID=$(echo "$item" | jq -r '.EVENT_SCHEMAID.S // empty')
    CREATION_TIMESTAMP=$(echo "$item" | jq -r '.CREATION_TIMESTAMP.S // empty')
    UPDATED_TIMESTAMP=$(echo "$item" | jq -r '.UPDATED_TIMESTAMP.S // empty')
    
    # Build update expression
    UPDATE_EXPR="SET eventID = :eventId, eventStatus = :eventStatus"
    EXPR_VALUES=":eventId = {\"S\": \"$EVENT_ID\"}, :eventStatus = {\"S\": \"$EVENT_STATUS\"}"
    
    if [ -n "$CREATION_TIMESTAMP" ] && [ "$CREATION_TIMESTAMP" != "null" ]; then
      UPDATE_EXPR="$UPDATE_EXPR, CREATION_TIMESTAMP = :creationTs"
      EXPR_VALUES="$EXPR_VALUES, :creationTs = {\"S\": \"$CREATION_TIMESTAMP\"}"
    fi
    
    if [ -n "$UPDATED_TIMESTAMP" ] && [ "$UPDATED_TIMESTAMP" != "null" ]; then
      UPDATE_EXPR="$UPDATE_EXPR, UPDATED_TIMESTAMP = :updatedTs"
      EXPR_VALUES="$EXPR_VALUES, :updatedTs = {\"S\": \"$UPDATED_TIMESTAMP\"}"
    fi
    
    if [ -n "$EVENT_SCHEMAID" ] && [ "$EVENT_SCHEMAID" != "null" ]; then
      UPDATE_EXPR="$UPDATE_EXPR, EVENT_SCHEMAID = :schemaId"
      EXPR_VALUES="$EXPR_VALUES, :schemaId = {\"S\": \"$EVENT_SCHEMAID\"}"
    fi
    
    # Remove old attributes
    REMOVE_EXPR="REMOVE EVENT_ID, EVENT_STATUS"
    
    # Build key for update
    KEY_JSON=$(jq -n --arg keyName "$KEY_ATTR" --arg keyValue "$KEY_VALUE" '{($keyName): {"S": $keyValue}}')
    
    # Update item
    aws dynamodb update-item \
      --table-name "$TABLE_NAME" \
      --key "$KEY_JSON" \
      --update-expression "$UPDATE_EXPR" \
      --expression-attribute-values "{$EXPR_VALUES}" \
      --endpoint-url "$ENDPOINT_URL" > /dev/null
    
    # Remove old attributes
    aws dynamodb update-item \
      --table-name "$TABLE_NAME" \
      --key "$KEY_JSON" \
      --update-expression "$REMOVE_EXPR" \
      --endpoint-url "$ENDPOINT_URL" > /dev/null
    
    ITEM_COUNT=$((ITEM_COUNT + 1))
    echo "Updated item $ITEM_COUNT: eventID=$EVENT_ID, eventStatus=$EVENT_STATUS"
  done
  
  echo ""
  echo "Migration complete! Updated $ITEM_COUNT items."
fi

echo ""
echo "Verifying final state..."
aws dynamodb scan \
  --table-name "$TABLE_NAME" \
  --endpoint-url "$ENDPOINT_URL" \
  --limit 5 \
  --output json | jq '.Items[0] // "No items found"'

echo ""
echo "Migration script completed!"

