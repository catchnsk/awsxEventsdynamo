package com.barclaycardus.webhooksvcs.evntsrc.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.barclaycardus.webhooksvcs.evntsrc.error.TransformationException;
import org.springframework.stereotype.Component;

@Component
public class SoapToJsonConverter {

    private final XmlMapper xmlMapper = new XmlMapper();
    private final ObjectMapper objectMapper;

    public SoapToJsonConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String convert(String soapPayload) {
        try {
            JsonNode xmlTree = xmlMapper.readTree(soapPayload.getBytes());
            JsonNode body = findBodyNode(xmlTree);
            return objectMapper.writeValueAsString(body == null ? xmlTree : body);
        } catch (Exception e) {
            throw new TransformationException("Failed to convert SOAP to JSON", e);
        }
    }

    private JsonNode findBodyNode(JsonNode root) {
        if (root == null) {
            return null;
        }
        if (root.has("Body")) {
            return root.get("Body");
        }
        if (root.elements().hasNext()) {
            for (JsonNode child : root) {
                JsonNode body = findBodyNode(child);
                if (body != null) {
                    return body;
                }
            }
        }
        return null;
    }
}
