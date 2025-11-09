package com.webhooks.sdk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;

import java.util.Properties;
import java.util.function.Supplier;

/**
 * Factory helpers for IAM-authenticated Kafka producers.
 */
public final class KafkaProducerFactory {

    private KafkaProducerFactory() {}

    public static Supplier<KafkaProducer<String, String>> iamAuth(String bootstrapServers, String region) {
        Properties properties = baseProperties(bootstrapServers);
        properties.put("sasl.mechanism", "AWS_MSK_IAM");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
        properties.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, IAMClientCallbackHandler.class.getName());
        properties.put("region", region);
        properties.put("security.protocol", SecurityProtocol.SASL_SSL.name);
        return () -> new KafkaProducer<>(properties);
    }

    private static Properties baseProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        return props;
    }
}
