package com.webhooks.listener.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import com.webhooks.listener.error.SchemaValidationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;

@Component
public class SchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(SchemaValidator.class);

    private final ObjectMapper objectMapper;
    private final JsonSchemaFactory jsonSchemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909);

    public SchemaValidator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void validate(SchemaMetadata metadata, String payload) {
        if (!metadata.isActive()) {
            throw new SchemaValidationException("Schema " + metadata.schemaId() + " is inactive");
        }
        switch (metadata.formatType()) {
            case JSON -> validateJson(metadata, payload);
            case XML, SOAP -> validateXml(metadata, payload);
            case AVRO -> validateAvro(metadata, payload);
            default -> {
                log.warn("Unsupported format {} for schema {}", metadata.formatType(), metadata.schemaId());
            }
        }
    }

    private void validateJson(SchemaMetadata metadata, String payload) {
        try {
            JsonNode jsonNode = objectMapper.readTree(payload);
            JsonSchema schema = jsonSchemaFactory.getSchema(metadata.schemaDefinition());
            Set<ValidationMessage> messages = schema.validate(jsonNode);
            if (!messages.isEmpty()) {
                throw new SchemaValidationException("JSON schema validation failed: " + messages);
            }
        } catch (IOException e) {
            throw new SchemaValidationException("Unable to parse JSON payload", e);
        }
    }

    private void validateXml(SchemaMetadata metadata, String payload) {
        try {
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            javax.xml.validation.Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(metadata.schemaDefinition())));
            javax.xml.validation.Validator validator = schema.newValidator();
            validator.validate(new StreamSource(new StringReader(payload)));
        } catch (Exception e) {
            throw new SchemaValidationException("XML/SOAP validation failed", e);
        }
    }

    private void validateAvro(SchemaMetadata metadata, String payload) {
        try {
            Schema schema = new Schema.Parser().parse(metadata.schemaDefinition());
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            reader.read(null, DecoderFactory.get().jsonDecoder(schema, payload));
        } catch (Exception e) {
            throw new SchemaValidationException("Avro validation failed", e);
        }
    }
}
