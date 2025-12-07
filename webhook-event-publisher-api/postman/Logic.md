Behavior:
Always queries DynamoDB: Performs a full table scan via DynamoDbClient.scan()
Does not read from cache: Unlike fetchSchema(), it doesn't check cache first
Populates cache as a side effect: When caching is enabled, results are stored in cache for future fetchSchema() calls
Comparison:
Endpoint	Cache Check	DynamoDB Query	Cache Population
/webhook/schema/all	❌ No	✅ Yes (Scan)	✅ Yes (side effect)
/webhook/schema/{domain}/{event}/{version}	✅ Yes	✅ Only if cache miss	✅ Yes
/webhook/schema/schema_id/{schemaId}	✅ Yes	✅ Only if cache miss	✅ Yes
Why This Design?
/webhook/schema/all returns all schemas, which can be large
Caching all schemas would use significant memory
Individual schema lookups are cached for performance
Ensures the list endpoint always reflects current DynamoDB state
Performance Impact:
Every call performs a DynamoDB Scan operation
Cost: Full table scan (can be expensive with many schemas)
Latency: Depends on table size
Use sparingly in production
Summary:
/webhook/schema/all always reads from DynamoDB and does not use cache. It populates the cache as a side effect to speed up individual schema lookups.



Behavior:
Scenario	Cache Status	DynamoDB Call	Performance
Cache HIT	✅ Schema found in cache	❌ No	⚡ Fast (in-memory)
Cache MISS	❌ Schema not in cache	✅ Yes	🐌 Slower (network call)
Cache Disabled	N/A	✅ Always	🐌 Always slower
Cache behavior:
Cache TTL: 24 hours (from application.yaml: schema-ttl: PT24H)
Cache size: Maximum 1000 entries
Cache key: SchemaReference(domain, eventName, version)
Example flow:
First Request (Cache MISS):
1. Request arrives → schemaService.fetchSchema()2. Cache check → Not found3. DynamoDB query → Get schema4. Cache store → Store in cache5. Validation → Use schema from DynamoDB
   Subsequent Requests (Cache HIT):
1. Request arrives → schemaService.fetchSchema()2. Cache check → Found! ✅3. Validation → Use schema from cache (no DynamoDB call)
   Performance impact:
   Cache HIT: ~1-5ms (in-memory lookup)
   Cache MISS: ~50-200ms (DynamoDB query + network latency)
   When cache is bypassed:
   Cache disabled: webhooks.cache.enabled: false
   Cache expired: After 24 hours TTL
   Cache evicted: After calling /webhook/schema/evict/all
   Cache full: When max entries (1000) reached, LRU eviction
   Summary:
   Both /webhook/event/publisher and /webhook/event/publisherCE:
   ✅ Check cache first
   ✅ Use cached schema if available (no DynamoDB call)
   ✅ Query DynamoDB only on cache miss
   ✅ Store result in cache for future requests
   This improves performance by avoiding DynamoDB calls for frequently used schemas.



IdempotencyLedgerService (also uses DynamoDB), not from schema lookup. The schema lookup likely uses cache, but the request fails when writing to the idempotency ledger.
Changes Made
1. Added cache logging
   Added debug logging in CachingSchemaService to track:
   Cache HIT/MISS
   When schemas are cached
   When DynamoDB is queried
2. Made idempotency ledger writes non-blocking
   Changed idempotency ledger writes to be fire-and-forget so they don't fail the request if DynamoDB is down:
   /webhook/event/publisher (line 164)
   /webhook/event/publisherCE (line 375)
   /webhook/schema/schema_id/{schemaId} (line 558)
   How Cache Works Now
   First Request (DynamoDB up):
1. Schema lookup → Cache MISS2. Query DynamoDB → Success3. Cache schema → Store in cache4. Validate → Success5. Publish to Kafka → Success6. Write to idempotency ledger → Success (non-blocking)
   Second Request (DynamoDB down):
1. Schema lookup → Cache HIT ✅ (Uses cached schema, NO DynamoDB call)2. Validate → Success (using cached schema)3. Publish to Kafka → Success4. Write to idempotency ledger → Fails silently (logged as warning, doesn't fail request)
   Verification
   Check application logs for:
   Cache HIT for schema reference - Schema found in cache
   Cache MISS for schema reference - Schema not in cache, querying DynamoDB
   Cached schema for reference - Schema stored in cache
   Failed to record event status in idempotency ledger - Ledger write failed (non-blocking)
   Summary
   Schema validation uses cache when available
   Idempotency ledger writes are non-blocking (won't fail the request)
   Added logging to track cache behavior
   Requests succeed even if DynamoDB is down (as long as schema is cached)
   The cache should work correctly. The previous error was from the idempotency ledger write, which is now non-blocking. Test again and check the logs to confirm cache hits.