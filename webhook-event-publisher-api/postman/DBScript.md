docker run -d --name dynamodb-local -p 8000:8000 amazon/dynamodb-local



TableName": "event_schema",
"TableStatus": "ACTIVE",
"KeySchema": [
{"AttributeName": "PK", "KeyType": "HASH"},
{"AttributeName": "SK", "KeyType": "RANGE"}
]

-- Avero format 
dynamodb put-item \
--table-name event-schema \
--item '{
"EVENT_SCHEMA_ID": { "S": "SCHEMA_TRANSACTION_CREATED_V1" },

    "PRODUCER_DOMAIN": { "S": "payments" },
    "EVENT_NAME": { "S": "transactionCreated" },
    "VERSION": { "S": "1.0" },

    "EVENT_SCHEMA_HEADER": { "S": "Standard event header with ID, domain, 
version, and timestamp" },

    "EVENT_SCHEMA_DEFINITION": {
      "S": "{
        \"type\": \"record\",
        \"name\": \"TransactionCreatedEvent\",
        \"namespace\": \"com.webhooks.events\",
        \"doc\": \"Schema definition for transaction-created webhook 
event\",
\"fields\": [
{
\"name\": \"eventHeader\",
\"type\": {
\"type\": \"record\",
\"name\": \"EventHeader\",
\"fields\": [
{ \"name\": \"eventId\", \"type\": \"string\" },
{ \"name\": \"eventName\", \"type\": \"string\" },
{ \"name\": \"producerDomain\", \"type\": \"string\" },
{ \"name\": \"version\", \"type\": \"string\" },
{ \"name\": \"timestamp\", \"type\": \"string\" }
]
}
},
{
\"name\": \"eventPayload\",
\"type\": {
\"type\": \"record\",
\"name\": \"EventPayload\",
\"fields\": [
{ \"name\": \"transactionId\", \"type\": \"string\" },
{ \"name\": \"customerId\", \"type\": \"string\" },
{ \"name\": \"amount\", \"type\": \"double\" },
{ \"name\": \"currency\", \"type\": \"string\" },
{ \"name\": \"status\", \"type\": \"string\" },
{
\"name\": \"metadata\",
\"type\": { \"type\": \"map\", \"values\": \"string\" },
\"default\": {}
}
]
}
}
]
}"
},

    "EVENT_SAMPLE": {
      "S": "{\"eventHeader\":{\"eventId\":\"evt-123\",\"eventName\":\"trans
actionCreated\",\"producerDomain\":\"payments\",\"version\":\"1.0\",\"times
tamp\":\"2025-01-01T10:00:00Z\"},\"eventPayload\":{\"transactionId\":\"txn-
999\",\"customerId\":\"cust-001\",\"amount\":150.75,\"currency\":\"USD\",\"
status\":\"SUCCESS\",\"metadata\":{\"source\":\"mobile-app\"}}}"
},

    "EVENT_SCHEMA_STATUS": { "S": "ACTIVE" },
    "HAS_SENSITIVE_DATA": { "S": "NO" },

    "PRODUCER_SYSTEM_USERS_ID": { "S": "user123" },

    "TOPIC_NAME": { "S": "payments.transaction.created" },
    "TOPIC_STATUS": { "S": "ACTIVE" },

    "INSERT_TS": { "S": "2025-11-13T20:00:00Z" },
    "INSERT_USER": { "S": "system" },
    "UPDATE_TS": { "S": "2025-11-13T20:00:00Z" },
    "UPDATE_USER": { "S": "system" }
}' 


--json format 