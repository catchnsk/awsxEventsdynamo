docker run -d --name dynamodb-local -p 8000:8000 amazon/dynamodb-local

--- Planned to create table 

TableName": "event_schema",
"TableStatus": "ACTIVE",
"KeySchema": [
{"AttributeName": "PK", "KeyType": "HASH"},
{"AttributeName": "SK", "KeyType": "RANGE"}
]
-- Created table in local 
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

-- Created record in local 

aws dynamodb put-item \
--cli-input-json file://create_sample_schema.json \
--endpoint-url http://localhost:8000


--check the record
aws dynamodb scan \
--table-name event_schema \
--endpoint-url http://localhost:8000


-- Get the list of items 

aws dynamodb query \
  --table-name event_schema \
  --key-condition-expression "PK = :pk AND begins_with(SK, :sk)" \
  --expression-attribute-values '{
    ":pk": {"S": "SCHEMA_0001"},
    ":sk": {"S": "v"}
  }' \
  --endpoint-url http://localhost:8000




aws dynamodb put-item \
--table-name event_schema \
--item '{
"PK": { "S": "SCHEMA_TRANSACTION_CREATED_V1" },
"SK": { "S": "v1.0" },

    "EVENT_SCHEMA_ID": { "S": "SCHEMA_TRANSACTION_CREATED_V1" },
    "PRODUCER_DOMAIN": { "S": "payments" },
    "EVENT_NAME": { "S": "transactionCreated" },
    "VERSION": { "S": "1.0" },
    "EVENT_SCHEMA_HEADER": { "S": "Standard event header with ID, domain, version, and timestamp" },

    "EVENT_SCHEMA_DEFINITION": { 
      "S": "{\"type\":\"record\",\"name\":\"TransactionCreatedEvent\",\"namespace\":\"com.webhooks.events\",\"doc\":\"Schema definition for transaction-created webhook event\",\"fields\":[{\"name\":\"eventHeader\",\"type\":{\"type\":\"record\",\"name\":\"EventHeader\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"eventName\",\"type\":\"string\"},{\"name\":\"producerDomain\",\"type\":\"string\"},{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]}},{\"name\":\"eventPayload\",\"type\":{\"type\":\"record\",\"name\":\"EventPayload\",\"fields\":[{\"name\":\"transactionId\",\"type\":\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"default\":{}}]}}]}" }
    },

    "EVENT_SAMPLE": { 
      "S": "{\"eventHeader\":{\"eventId\":\"evt-123\",\"eventName\":\"transactionCreated\",\"producerDomain\":\"payments\",\"version\":\"1.0\",\"timestamp\":\"2025-01-01T10:00:00Z\"},\"eventPayload\":{\"transactionId\":\"txn-999\",\"customerId\":\"cust-001\",\"amount\":150.75,\"currency\":\"USD\",\"status\":\"SUCCESS\",\"metadata\":{\"source\":\"mobile-app\"}}}"
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
}' \
--endpoint-url http://localhost:8000


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


curl -X POST http://localhost:8000 \                                                                                 │
│     -H "Content-Type: application/x-amz-json-1.0" \                                                                    │
│     -H "X-Amz-Target: DynamoDB_20120810.PutItem" \                                                                     │
│     -H "Authorization: AWS4-HMAC-SHA256 Credential=dummy/20231113/us-east-1/dynamodb/aws4_request,                     │
│   SignedHeaders=host;x-amz-date, Signature=dummy" \                                                                    │
│     -d '{                                                                                                              │
│       "TableName": "event_schema",                                                                                     │
│       "Item": {                                                                                                        │
│         "PK": { "S": "SCHEMA#payments#transactionCreated" },                                                           │
│         "SK": { "S": "v1.0" },                                                                                         │
│         "EVENT_SCHEMA_ID": { "S": "SCHEMA_TRANSACTION_CREATED_V1" },                                                   │
│         "PRODUCER_DOMAIN": { "S": "payments" },                                                                        │
│         "EVENT_NAME": { "S": "transactionCreated" },                                                                   │
│         "VERSION": { "S": "1.0" },                                                                                     │
│                                                                                                                        │
│         "EVENT_SCHEMA_DEFINITION_JSON": {                                                                              │
│           "S": "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[\"eventHe   │
│   ader\",\"eventPayload\"],\"properties\":{\"eventHeader\":{\"type\":\"object\",\"required\":[\"eventId\",\"eventNam   │
│   e\",\"producerDomain\",\"version\",\"timestamp\"],\"properties\":{\"eventId\":{\"type\":\"string\"},\"eventName\":   │
│   {\"type\":\"string\"},\"producerDomain\":{\"type\":\"string\"},\"version\":{\"type\":\"string\"},\"timestamp\":{\"   │
│   type\":\"string\"}}},\"eventPayload\":{\"type\":\"object\",\"required\":[\"transactionId\",\"customerId\",\"amount   │
│   \",\"currency\",\"status\"],\"properties\":{\"transactionId\":{\"type\":\"string\"},\"customerId\":{\"type\":\"str   │
│   ing\"},\"amount\":{\"type\":\"number\"},\"currency\":{\"type\":\"string\"},\"status\":{\"type\":\"string\"},\"meta   │
│   data\":{\"type\":\"object\",\"additionalProperties\":{\"type\":\"string\"}}}}}}"                                     │
│         },                                                                                                             │
│                                                                                                                        │
│         "EVENT_SCHEMA_DEFINITION_XML": {                                                                               │
│           "S": "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xs:schema                                                   │
│   xmlns:xs=\"http://www.w3.org/2001/XMLSchema\"><xs:element name=\"event\"><xs:complexType><xs:sequence><xs:element    │
│   name=\"eventHeader\"><xs:complexType><xs:sequence><xs:element name=\"eventId\" type=\"xs:string\"/><xs:element       │
│   name=\"eventName\" type=\"xs:string\"/><xs:element name=\"producerDomain\" type=\"xs:string\"/><xs:element           │
│   name=\"version\" type=\"xs:string\"/><xs:element name=\"timestamp\"                                                  │
│   type=\"xs:string\"/></xs:sequence></xs:complexType></xs:element><xs:element                                          │
│   name=\"eventPayload\"><xs:complexType><xs:sequence><xs:element name=\"transactionId\"                                │
│   type=\"xs:string\"/><xs:element name=\"customerId\" type=\"xs:string\"/><xs:element name=\"amount\"                  │
│   type=\"xs:decimal\"/><xs:element name=\"currency\" type=\"xs:string\"/><xs:element name=\"status\"                   │
│   type=\"xs:string\"/><xs:element name=\"metadata\" minOccurs=\"0\"><xs:complexType><xs:sequence><xs:any               │
│   maxOccurs=\"unbounded\" processContents=\"skip\"/></xs:sequence></xs:complexType></xs:element></xs:sequence></xs:c   │
│   omplexType></xs:element></xs:sequence></xs:complexType></xs:element></xs:schema>"                                    │
│         },                                                                                                             │
│                                                                                                                        │
│         "EVENT_SCHEMA_DEFINITION_AVRO": {                                                                              │
│           "S": "{\"type\":\"record\",\"name\":\"TransactionCreatedEvent\",\"namespace\":\"com.webhooks.events\",\"fi   │
│   elds\":[{\"name\":\"eventHeader\",\"type\":{\"type\":\"record\",\"name\":\"EventHeader\",\"fields\":[{\"name\":\"e   │
│   ventId\",\"type\":\"string\"},{\"name\":\"eventName\",\"type\":\"string\"},{\"name\":\"producerDomain\",\"type\":\   │
│   "string\"},{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"}]}},{\"name\":\"   │
│   eventPayload\",\"type\":{\"type\":\"record\",\"name\":\"EventPayload\",\"fields\":[{\"name\":\"transactionId\",\"t   │
│   ype\":\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\   │
│   ":\"currency\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"metadata\",\"type\":{\"t   │
│   ype\":\"map\",\"values\":\"string\"},\"default\":{}}]}}]}"                                                           │
│         },                                                                                                             │
│                                                                                                                        │
│         "EVENT_SCHEMA_STATUS": { "S": "ACTIVE" },                                                                      │
│         "HAS_SENSITIVE_DATA": { "S": "NO" },                                                                           │
│         "PRODUCER_SYSTEM_USERS_ID": { "S": "user123" },                                                                │
│         "TOPIC_NAME": { "S": "payments.transaction.created" },                                                         │
│         "TOPIC_STATUS": { "S": "ACTIVE" },                                                                             │
│         "INSERT_TS": { "S": "2025-11-13T20:00:00Z" },                                                                  │
│         "INSERT_USER": { "S": "system" },                                                                              │
│         "UPDATE_TS": { "S": "2025-11-13T20:00:00Z" },                                                                  │
│         "UPDATE_USER": { "S": "system" }                                                                               │
│       }                                                                                                                │
│     }'    



The record now contains:
- EVENT_SCHEMA_DEFINITION_JSON - JSON Schema for validation
- EVENT_SCHEMA_DEFINITION_XML - XSD Schema for XML validation
- EVENT_SCHEMA_DEFINITION_AVRO - Avro Schema for Avro validation

Ready to Test!

You can now send events in 3 different formats:

1. JSON (default)

curl -X POST http://localhost:8080/events/transactionCreated \
-H "Content-Type: application/json" \
-H "X-Producer-Domain: payments" \
-H "X-Event-Version: 1.0" \
-d '{
"eventHeader": {
"eventId": "evt-123",
"eventName": "transactionCreated",
"producerDomain": "payments",
"version": "1.0",
"timestamp": "2025-11-13T23:00:00Z"
},
"eventPayload": {
"transactionId": "txn-999",
"customerId": "cust-001",
"amount": 150.75,
"currency": "USD",
"status": "SUCCESS",
"metadata": {"source": "api"}
}
}'

2. XML

curl -X POST http://localhost:8080/events/transactionCreated \
-H "Content-Type: application/xml" \
-H "X-Producer-Domain: payments" \
-H "X-Event-Version: 1.0" \
-d '<event>
<eventHeader>
<eventId>evt-123</eventId>
<eventName>transactionCreated</eventName>
<producerDomain>payments</producerDomain>
<version>1.0</version>
<timestamp>2025-11-13T23:00:00Z</timestamp>
</eventHeader>
<eventPayload>
<transactionId>txn-999</transactionId>
<customerId>cust-001</customerId>
<amount>150.75</amount>
<currency>USD</currency>
<status>SUCCESS</status>
</eventPayload>
</event>'

Start the app and test it!

Multi-Format Support Architecture

Flow with Content Negotiation

HTTP Request
↓
Detect Content-Type header
↓
┌─────────────┬──────────────┬──────────────┐
│ JSON        │ XML          │ Avro         │
│ (application│ (application │ (application │
│ /json)      │ /xml)        │ /avro)       │
└─────────────┴──────────────┴──────────────┘
↓              ↓              ↓
JSON Schema    XML Schema     Avro Schema
Validation     (XSD)          Validation
↓              ↓              ↓
└──────────────┴──────────────┘
↓
Convert to Avro (canonical format)
↓
Publish to Kafka (Avro binary)

Implementation Strategy

Option 1: Accept Multiple Formats → Normalize to Avro

1. Accept: JSON, XML, Avro
2. Validate with format-specific validator
3. Convert ALL to Avro GenericRecord (canonical)
4. Publish to Kafka as Avro

DynamoDB Schema Storage:
EVENT_SCHEMA_DEFINITION_JSON → JSON Schema
EVENT_SCHEMA_DEFINITION_XML  → XSD Schema
EVENT_SCHEMA_DEFINITION_AVRO → Avro Schema
SCHEMA_FORMATS               → ["JSON", "XML", "AVRO"]

Option 2: Accept Any Format → Store as JSON

1. XML → Convert to JSON (using Jackson)
2. Validate JSON Schema
3. Convert to Avro
4. Publish to Kafka

Format Comparison

| Format | Validation  | Conversion to Avro | Kafka Payload |
  |--------|-------------|--------------------|---------------|
| JSON   | JSON Schema | ✅ Direct           | Avro Binary   |
| XML    | XSD Schema  | ✅ Via JSON         | Avro Binary   |
| Avro   | Avro Schema | ✅ Already Avro     | Avro Binary   |

Code Example

@PostMapping(value = "/events/{eventName}",
consumes = {MediaType.APPLICATION_JSON_VALUE,
MediaType.APPLICATION_XML_VALUE,
"application/avro"})
public Mono<ResponseEntity> publishEvent(
@RequestHeader("Content-Type") String contentType,
@RequestBody String rawBody) {

      return switch(contentType) {
          case "application/json" -> validateJson(rawBody)
                                     .flatMap(this::convertToAvro);
          case "application/xml"  -> validateXml(rawBody)
                                     .flatMap(this::xmlToJson)
                                     .flatMap(this::convertToAvro);
          case "application/avro" -> validateAvro(rawBody);
      }.flatMap(avroRecord -> publishToKafka(avroRecord));
}

Recommended Approach

Best Practice: "Write in any format, store in one"

1. Accept: JSON, XML, Avro (via Content-Type)
2. Validate: Format-specific validation
3. Normalize: Convert everything to Avro
4. Store in Kafka: Always Avro (efficient, schema-aware)

---

## EVENT_IDEMPOTENCY_LEDGER Table Migration

### Rename Attributes: EVENT_ID → eventID, EVENT_STATUS → eventStatus

#### Option 1: Using Python Script (Recommended)

```bash
# Make script executable
chmod +x postman/migrate_idempotency_ledger.py

# Run migration
python3 postman/migrate_idempotency_ledger.py
```

#### Option 2: Using Shell Script

```bash
# Make script executable
chmod +x postman/migrate_idempotency_ledger.sh

# Run migration
./postman/migrate_idempotency_ledger.sh
```

#### Option 3: Manual AWS CLI Commands

**Step 1: Check current table structure**
```bash
aws dynamodb describe-table \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --endpoint-url http://localhost:8000
```

**Step 2: Scan all items and view structure**
```bash
aws dynamodb scan \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --endpoint-url http://localhost:8000 \
  --limit 1
```

**Step 3: Update each item (if EVENT_ID is NOT the partition key)**

For each item, you'll need to:
1. Get the item's key
2. Update with new attribute names
3. Remove old attributes

Example for a single item (replace KEY_VALUE with actual key):
```bash
# Update item (assuming partition key is something other than EVENT_ID)
aws dynamodb update-item \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --key '{"YOUR_PARTITION_KEY": {"S": "KEY_VALUE"}}' \
  --update-expression "SET eventID = :eventId, eventStatus = :eventStatus, CREATION_TIMESTAMP = :creationTs, UPDATED_TIMESTAMP = :updatedTs" \
  --expression-attribute-values '{
    ":eventId": {"S": "EVENT_ID_VALUE"},
    ":eventStatus": {"S": "EVENT_STATUS_VALUE"},
    ":creationTs": {"S": "CREATION_TIMESTAMP_VALUE"},
    ":updatedTs": {"S": "UPDATED_TIMESTAMP_VALUE"}
  }' \
  --endpoint-url http://localhost:8000

# Remove old attributes
aws dynamodb update-item \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --key '{"YOUR_PARTITION_KEY": {"S": "KEY_VALUE"}}' \
  --update-expression "REMOVE EVENT_ID, EVENT_STATUS" \
  --endpoint-url http://localhost:8000
```

**Step 4: If EVENT_ID IS the partition key, recreate table**

```bash
# 1. Create new table with eventID as partition key
aws dynamodb create-table \
  --table-name EVENT_IDEMPOTENCY_LEDGER_new \
  --attribute-definitions \
    AttributeName=eventID,AttributeType=S \
  --key-schema \
    AttributeName=eventID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://localhost:8000

# 2. Wait for table to be active
aws dynamodb wait table-exists \
  --table-name EVENT_IDEMPOTENCY_LEDGER_new \
  --endpoint-url http://localhost:8000

# 3. Migrate data (use script or manual process)
# 4. Delete old table
aws dynamodb delete-table \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --endpoint-url http://localhost:8000

# 5. Recreate table with original name and eventID as key
aws dynamodb create-table \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --attribute-definitions \
    AttributeName=eventID,AttributeType=S \
  --key-schema \
    AttributeName=eventID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://localhost:8000

# 6. Copy data from _new to original table
# 7. Delete _new table
```

**Step 5: Verify migration**
```bash
aws dynamodb scan \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --endpoint-url http://localhost:8000 \
  --limit 5
```

### Create Table with New Structure (if starting fresh)

#### Option 1: Simple Table (No GSI)

**Important:** Only `eventID` is defined in `AttributeDefinitions` and `KeySchema` because it's the partition key. `eventStatus` is a regular attribute and does NOT need to be in `AttributeDefinitions` or `KeySchema`.

```bash
aws dynamodb create-table \
  --table-name EVENT_IDEMPOTENCY_LEDGER \
  --attribute-definitions \
    AttributeName=eventID,AttributeType=S \
  --key-schema \
    AttributeName=eventID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --endpoint-url http://localhost:8000
```

**Table Structure:**
- **Partition Key (HASH):** `eventID` (String) - Must be in both `AttributeDefinitions` and `KeySchema`
- **Regular Attributes:** `eventStatus` (String), `EVENT_SCHEMAID` (String), `CREATION_TIMESTAMP` (String), `UPDATED_TIMESTAMP` (String) - These are NOT in `AttributeDefinitions` or `KeySchema`

#### Option 2: Table with Global Secondary Index (GSI) - Recommended

**Important:** Both `eventId` and `eventStatus` must be in `AttributeDefinitions` because `eventStatus` is used as a sort key in the GSI.

```bash
aws dynamodb create-table \
  --table-name event-idempotency-ledger \
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
  --endpoint-url http://localhost:8000
```

**Table Structure with GSI:**
- **Table Name:** `event-idempotency-ledger`
- **Partition Key (HASH):** `eventId` (String) - Must be in `AttributeDefinitions` and `KeySchema`
- **GSI Name:** `event-status-index`
  - **GSI Partition Key (HASH):** `eventId` (String)
  - **GSI Sort Key (RANGE):** `eventStatus` (String) - Must be in `AttributeDefinitions` because it's used in GSI
- **Projection:** ALL (includes all attributes)
- **Billing Mode:** PAY_PER_REQUEST

**Using the script:**
```bash
chmod +x postman/create_idempotency_ledger_table.sh
./postman/create_idempotency_ledger_table.sh
```

**Or using JSON file (easier):**
```bash
aws dynamodb create-table \
  --cli-input-json file://postman/create_idempotency_ledger_table.json \
  --endpoint-url http://localhost:8000
```

### Sample Item with New Attribute Names

**For table `event-idempotency-ledger` (with GSI):**
```bash
aws dynamodb put-item \
  --table-name event-idempotency-ledger \
  --item '{
    "eventId": {"S": "f701560e-a31b-4748-927f-2655caf785f9"},
    "eventStatus": {"S": "EVENT_READY_FOR_DELIVERY"},
    "EVENT_SCHEMAID": {"S": "SCHEMA_PAYMENTS_TRANSACTIONCREATED_1_0"},
    "CREATION_TIMESTAMP": {"S": "2025-12-07T10:00:00Z"},
    "UPDATED_TIMESTAMP": {"S": "2025-12-07T10:00:00Z"}
  }' \
  --endpoint-url http://localhost:8000
```

**Query using GSI (event-status-index):**
```bash
aws dynamodb query \
  --table-name event-idempotency-ledger \
  --index-name event-status-index \
  --key-condition-expression "eventId = :eventId AND eventStatus = :eventStatus" \
  --expression-attribute-values '{
    ":eventId": {"S": "f701560e-a31b-4748-927f-2655caf785f9"},
    ":eventStatus": {"S": "EVENT_READY_FOR_DELIVERY"}
  }' \
  --endpoint-url http://localhost:8000
```

**Query all events with a specific status:**
```bash
aws dynamodb query \
  --table-name event-idempotency-ledger \
  --index-name event-status-index \
  --key-condition-expression "eventId = :eventId" \
  --expression-attribute-values '{
    ":eventId": {"S": "f701560e-a31b-4748-927f-2655caf785f9"}
  }' \
  --endpoint-url http://localhost:8000
```
