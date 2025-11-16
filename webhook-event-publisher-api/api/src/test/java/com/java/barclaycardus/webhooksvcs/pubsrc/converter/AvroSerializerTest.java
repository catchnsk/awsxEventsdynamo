package com.java.barclaycardus.webhooksvcs.pubsrc.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;

class AvroSerializerTest {

    private AvroSerializer avroSerializer;
    private Schema testSchema;

    @BeforeEach
    void setUp() {
        avroSerializer = new AvroSerializer();
        
        // Create a simple test schema
        String schemaJson = """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": "int"}
                  ]
                }
                """;
        testSchema = new Schema.Parser().parse(schemaJson);
    }

    @Test
    void serializeToAvro_WithValidRecord_ReturnsByteArray() {
        // Create a test record
        GenericRecord record = new GenericData.Record(testSchema);
        record.put("id", "test-123");
        record.put("value", 42);

        // Serialize
        StepVerifier.create(avroSerializer.serializeToAvro(record, testSchema))
                .assertNext(bytes -> {
                    assertNotNull(bytes);
                    assertTrue(bytes.length > 0);
                })
                .verifyComplete();
    }

    @Test
    void serializeToAvro_WithEmptyRecord_ReturnsByteArray() {
        GenericRecord record = new GenericData.Record(testSchema);
        record.put("id", "");
        record.put("value", 0);

        StepVerifier.create(avroSerializer.serializeToAvro(record, testSchema))
                .assertNext(bytes -> {
                    assertNotNull(bytes);
                    assertTrue(bytes.length > 0);
                })
                .verifyComplete();
    }

    @Test
    void serializeToAvro_WithNullableField_HandlesNullValue() {
        // Create schema with nullable field
        String nullableSchemaJson = """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": ["null", "int"], "default": null}
                  ]
                }
                """;
        Schema nullableSchema = new Schema.Parser().parse(nullableSchemaJson);
        
        GenericRecord record = new GenericData.Record(nullableSchema);
        record.put("id", "test");
        record.put("value", null);

        // Avro should handle nulls when schema allows it
        StepVerifier.create(avroSerializer.serializeToAvro(record, nullableSchema))
                .assertNext(bytes -> {
                    assertNotNull(bytes);
                    assertTrue(bytes.length > 0);
                })
                .verifyComplete();
    }
}

