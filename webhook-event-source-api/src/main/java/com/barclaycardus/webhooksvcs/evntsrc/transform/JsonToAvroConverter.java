package com.barclaycardus.webhooksvcs.evntsrc.transform;

import com.barclaycardus.webhooksvcs.evntsrc.error.TransformationException;
import com.barclaycardus.webhooksvcs.evntsrc.schema.SchemaMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;

@Component
public class JsonToAvroConverter {

    public byte[] convert(String jsonPayload, SchemaMetadata metadata) {
        try {
            Schema schema = new Schema.Parser().parse(metadata.schemaDefinition());
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            GenericRecord record = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonPayload));

            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            writer.write(record, EncoderFactory.get().binaryEncoder(baos, null));
            return baos.toByteArray();
        } catch (Exception e) {
            throw new TransformationException("Failed to convert JSON to Avro", e);
        }
    }
}
