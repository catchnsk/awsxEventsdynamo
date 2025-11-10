package com.webhooks.listener.transform;

import com.webhooks.listener.schema.SchemaMetadata;
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

    public byte[] applyTransformations(SchemaMetadata metadata, String payload) {
        if (!metadata.transformationRequired()) {
            return payload.getBytes(StandardCharsets.UTF_8);
        }

        String normalizedJson = switch (metadata.formatType()) {
            case SOAP -> soapToJsonConverter.convert(payload);
            case XML -> xmlToJsonConverter.convert(payload);
            case JSON -> payload;
            case AVRO -> payload;
        };

        if (metadata.formatType() == SchemaMetadata.FormatType.AVRO) {
            return payload.getBytes(StandardCharsets.UTF_8);
        }

        return jsonToAvroConverter.convert(normalizedJson, metadata);
    }
}
