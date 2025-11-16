package com.beewaxus.webhooksvcs.pubsrc.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Component
public class FormatConverter {

    private final XmlMapper xmlMapper = new XmlMapper();
    private final ObjectMapper jsonMapper = new ObjectMapper();

    /**
     * Convert XML string to JSON
     */
    public Mono<JsonNode> xmlToJson(String xmlPayload) {
        return Mono.fromCallable(() -> {
                    return (JsonNode) xmlMapper.readTree(xmlPayload);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Convert JSON to Avro GenericRecord
     */
    public Mono<GenericRecord> jsonToAvro(JsonNode jsonPayload, Schema avroSchema) {
        return Mono.fromCallable(() -> {
                    // This is a simplified conversion - in production you'd use Avro's JSON encoder
                    GenericRecord record = new GenericData.Record(avroSchema);

                    // Map fields from JSON to Avro
                    avroSchema.getFields().forEach(field -> {
                        JsonNode value = jsonPayload.get(field.name());
                        if (value != null && !value.isNull()) {
                            record.put(field.name(), convertValue(value, field.schema()));
                        }
                    });

                    return record;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Object convertValue(JsonNode value, Schema schema) {
        return switch (schema.getType()) {
            case STRING -> value.asText();
            case INT -> value.asInt();
            case LONG -> value.asLong();
            case FLOAT -> (float) value.asDouble();
            case DOUBLE -> value.asDouble();
            case BOOLEAN -> value.asBoolean();
            case RECORD -> {
                GenericRecord nested = new GenericData.Record(schema);
                schema.getFields().forEach(field -> {
                    JsonNode fieldValue = value.get(field.name());
                    if (fieldValue != null && !fieldValue.isNull()) {
                        nested.put(field.name(), convertValue(fieldValue, field.schema()));
                    }
                });
                yield nested;
            }
            case MAP -> {
                var map = new java.util.HashMap<>();
                value.fields().forEachRemaining(entry ->
                    map.put(entry.getKey(), entry.getValue().asText()));
                yield map;
            }
            default -> value.asText();
        };
    }
}
