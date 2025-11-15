package com.java.barclaycardus.webhooksvcs.pubsrc.validation;

import com.java.barclaycardus.webhooksvcs.pubsrc.schema.SchemaDefinition;
import org.springframework.stereotype.Component;
import org.w3c.dom.ls.LSResourceResolver;
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

    public Mono<Void> validate(String xmlPayload, SchemaDefinition schemaDefinition) {
        return Mono.fromCallable(() -> {
                    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    Schema schema = factory.newSchema(new StreamSource(new StringReader(schemaDefinition.xmlSchema())));
                    Validator validator = schema.newValidator();

                    validator.validate(new StreamSource(new StringReader(xmlPayload)));
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then()
                .onErrorMap(SAXException.class, e -> new SchemaValidationException("XML schema validation failed: " + e.getMessage()));
    }
}
