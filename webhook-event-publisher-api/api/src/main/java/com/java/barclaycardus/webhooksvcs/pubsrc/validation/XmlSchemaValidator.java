package com.java.barclaycardus.webhooksvcs.pubsrc.validation;

import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.StringReader;

@Component
public class XmlSchemaValidator {

    private static final Logger log = LoggerFactory.getLogger(XmlSchemaValidator.class);

    public Mono<Void> validate(String xmlPayload, SchemaDefinition schemaDefinition) {
        // Create final variables for use in lambda
        final String xmlSchema = schemaDefinition.xmlSchema();
        final String finalXmlPayload = xmlPayload;
        
        return Mono.fromCallable(() -> {
                    if (xmlSchema == null || xmlSchema.trim().isEmpty()) {
                        throw new SchemaValidationException("XML schema is not configured for this event");
                    }
                    
                    if (finalXmlPayload == null || finalXmlPayload.trim().isEmpty()) {
                        throw new SchemaValidationException("XML payload cannot be null or empty");
                    }
                    
                    // Trim and ensure proper formatting
                    final String trimmedXmlSchema = xmlSchema.trim();
                    final String trimmedXmlPayload = finalXmlPayload.trim();
                    
                    log.debug("Validating XML payload against XSD schema (schema length: {}, payload length: {})", 
                            trimmedXmlSchema.length(), trimmedXmlPayload.length());
                    
                    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    Schema schema = factory.newSchema(new StreamSource(new StringReader(trimmedXmlSchema)));
                    Validator validator = schema.newValidator();

                    validator.validate(new StreamSource(new StringReader(trimmedXmlPayload)));
                    log.debug("XML validation successful");
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then()
                .onErrorMap(SAXException.class, e -> {
                    log.error("XML schema validation failed", e);
                    return new SchemaValidationException("XML schema validation failed: " + e.getMessage());
                })
                .onErrorMap(Exception.class, e -> {
                    if (e instanceof SchemaValidationException) {
                        return e;
                    }
                    log.error("Unexpected error during XML validation", e);
                    return new SchemaValidationException("XML validation error: " + e.getMessage());
                });
    }
}
