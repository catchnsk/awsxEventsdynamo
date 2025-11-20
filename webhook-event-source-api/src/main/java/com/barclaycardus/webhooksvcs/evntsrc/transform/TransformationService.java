package com.barclaycardus.webhooksvcs.evntsrc.transform;

import com.barclaycardus.webhooksvcs.evntsrc.schema.SchemaMetadata;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class TransformationService {

    private final SoapToJsonConverter soapToJsonConverter;
    private final XmlToJsonConverter xmlToJsonConverter;
    private final JsonToAvroConverter jsonToAvroConverter;

    public TransformationService(SoapToJsonConverter soapToJsonConverter,
                                 XmlToJsonConverter xmlToJsonConverter,
                                 JsonToAvroConverter jsonToAvroConverter) {
        this.soapToJsonConverter = soapToJsonConverter;
        this.xmlToJsonConverter = xmlToJsonConverter;
        this.jsonToAvroConverter = jsonToAvroConverter;
    }

    public byte[] applyTransformations(SchemaMetadata metadata, String payload, String contentType) {
        // Step 1: Parse based on contentType and normalize to JSON
        String normalizedJson = parseToJson(payload, contentType);

        // Step 2: Convert JSON to Avro format
        return jsonToAvroConverter.convert(normalizedJson, metadata);
    }

    private String parseToJson(String payload, String contentType) {
        if (contentType == null || contentType.isBlank()) {
            throw new IllegalArgumentException("Content-Type is required");
        }

        String normalizedContentType = contentType.toLowerCase();

        if (normalizedContentType.startsWith("application/json")) {
            return payload;
        } else if (normalizedContentType.startsWith("application/xml") ||
                   normalizedContentType.startsWith("text/xml")) {
            return xmlToJsonConverter.convert(payload);
        } else if (normalizedContentType.startsWith("application/soap+xml")) {
            return soapToJsonConverter.convert(payload);
        } else {
            throw new IllegalArgumentException("Unsupported content-type: " + contentType);
        }
    }
}
