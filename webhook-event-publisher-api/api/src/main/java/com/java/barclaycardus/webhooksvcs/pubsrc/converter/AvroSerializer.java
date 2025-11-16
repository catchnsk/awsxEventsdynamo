package com.java.barclaycardus.webhooksvcs.pubsrc.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Component
public class AvroSerializer {

    private static final Logger log = LoggerFactory.getLogger(AvroSerializer.class);

    /**
     * Serialize Avro GenericRecord to binary format
     */
    public Mono<byte[]> serializeToAvro(GenericRecord record, Schema schema) {
        return Mono.fromCallable(() -> {
                    try {
                        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                        
                        writer.write(record, encoder);
                        encoder.flush();
                        
                        byte[] avroBytes = outputStream.toByteArray();
                        log.debug("Serialized Avro record to {} bytes", avroBytes.length);
                        return avroBytes;
                    } catch (IOException e) {
                        log.error("Failed to serialize Avro record", e);
                        throw new RuntimeException("Failed to serialize Avro record: " + e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}

