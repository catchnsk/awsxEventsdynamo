aws dynamodb create-table \
  --table-name event-schema \
  --attribute-definitions \
    AttributeName=eventSchemaId,AttributeType=S \
    AttributeName=eventName,AttributeType=S \
    AttributeName=eventVersion,AttributeType=S \
  --key-schema \
    AttributeName=eventSchemaId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --global-secondary-indexes '[
    {
      "IndexName": "eventName-eventVersion-index",
      "KeySchema": [
        {"AttributeName": "eventName", "KeyType": "HASH"},
        {"AttributeName": "eventVersion", "KeyType": "RANGE"}
      ],
      "Projection": {"ProjectionType": "ALL"}
    }
  ]'
