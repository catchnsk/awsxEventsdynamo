package com.java.barclaycardus.webhooksvcs.pubsrc.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Component
public class AvroSchemaValidator {

    public Mono<GenericRecord> validate(JsonNode payload, SchemaDefinition schemaDefinition) {
        return Mono.fromCallable(() -> {
                    Schema schema = new Schema.Parser().parse(schemaDefinition.avroSchema());
                    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

                    String jsonString = payload.toString();
                    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);

                    GenericRecord record = reader.read(null, decoder);

                    // Validate the record
                    if (!GenericData.get().validate(schema, record)) {
                        throw new SchemaValidationException("Avro schema validation failed");
                    }

                    return record;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}
