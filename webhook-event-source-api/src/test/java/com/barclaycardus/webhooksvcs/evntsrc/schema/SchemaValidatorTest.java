package com.barclaycardus.webhooksvcs.evntsrc.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.barclaycardus.webhooksvcs.evntsrc.error.SchemaValidationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaValidatorTest {

    private final SchemaValidator validator = new SchemaValidator(new ObjectMapper());

    @Test
    void validatesAvroSchemaSuccessfully() throws Exception {
        String avroSchemaDefinition = """
                {
                  "type": "record",
                  "name": "Customer",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"}
                  ]
                }
                """;

        SchemaMetadata schema = new SchemaMetadata(
                "customer-created",
                avroSchemaDefinition,
                SchemaMetadata.FormatType.AVRO,
                false,
                SchemaMetadata.Status.ACTIVE,
                "customer",
                "created"
        );

        // Create valid Avro binary payload
        byte[] avroPayload = createAvroPayload(avroSchemaDefinition, "123", "Ada");

        assertDoesNotThrow(() -> validator.validate(schema, avroPayload));
    }

    @Test
    void throwsOnInvalidAvro() throws Exception {
        String avroSchemaDefinition = """
                {
                  "type": "record",
                  "name": "Customer",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"}
                  ]
                }
                """;

        SchemaMetadata schema = new SchemaMetadata(
                "customer-created",
                avroSchemaDefinition,
                SchemaMetadata.FormatType.AVRO,
                false,
                SchemaMetadata.Status.ACTIVE,
                "customer",
                "created"
        );

        // Invalid Avro binary
        byte[] invalidPayload = new byte[]{0x00, 0x01, 0x02};

        assertThrows(SchemaValidationException.class, () -> validator.validate(schema, invalidPayload));
    }

    @Test
    void throwsOnInactiveSchema() throws Exception {
        String avroSchemaDefinition = """
                {
                  "type": "record",
                  "name": "Customer",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"}
                  ]
                }
                """;

        SchemaMetadata schema = new SchemaMetadata(
                "customer-created",
                avroSchemaDefinition,
                SchemaMetadata.FormatType.AVRO,
                false,
                SchemaMetadata.Status.INACTIVE,
                "customer",
                "created"
        );

        byte[] avroPayload = createAvroPayload(avroSchemaDefinition, "123", "Ada");

        assertThrows(SchemaValidationException.class, () -> validator.validate(schema, avroPayload));
    }

    private byte[] createAvroPayload(String schemaDefinition, String id, String name) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaDefinition);

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", id);
        if (schema.getField("name") != null) {
            record.put("name", name);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();

        return outputStream.toByteArray();
    }
}
