package com.webhooks.listener.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.webhooks.listener.error.TransformationException;
import org.springframework.stereotype.Component;

@Component
public class XmlToJsonConverter {

    private final XmlMapper xmlMapper = new XmlMapper();
    private final ObjectMapper objectMapper;

    public XmlToJsonConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String convert(String xmlPayload) {
        try {
            JsonNode node = xmlMapper.readTree(xmlPayload.getBytes());
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new TransformationException("Failed to convert XML to JSON", e);
        }
    }
}
