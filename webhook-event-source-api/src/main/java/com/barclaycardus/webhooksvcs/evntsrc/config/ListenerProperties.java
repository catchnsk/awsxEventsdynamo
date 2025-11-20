package com.barclaycardus.webhooksvcs.evntsrc.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "listener")
public class ListenerProperties {

    @NotNull
    private Source source = new Source();

    @NotNull
    private Aws aws = new Aws();

    @NotNull
    private Output output = new Output();

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Aws getAws() {
        return aws;
    }

    public void setAws(Aws aws) {
        this.aws = aws;
    }

    public Output getOutput() {
        return output;
    }

    public void setOutput(Output output) {
        this.output = output;
    }

    public static class Source {
        @NotNull
        private SourceType type = SourceType.AMQ;
        @NotNull
        private Amq amq = new Amq();
        @NotNull
        private Kafka kafka = new Kafka();

        public SourceType getType() {
            return type;
        }

        public void setType(SourceType type) {
            this.type = type;
        }

        public Amq getAmq() {
            return amq;
        }

        public void setAmq(Amq amq) {
            this.amq = amq;
        }

        public Kafka getKafka() {
            return kafka;
        }

        public void setKafka(Kafka kafka) {
            this.kafka = kafka;
        }
    }

    public enum SourceType {
        AMQ,
        KAFKA
    }

    public static class Amq {
        @NotBlank
        private String brokerUrl = "tcp://localhost:61616";
        private String username;
        private String password;
        @NotEmpty
        private List<QueueConfig> queues = List.of(new QueueConfig());

        public String getBrokerUrl() {
            return brokerUrl;
        }

        public void setBrokerUrl(String brokerUrl) {
            this.brokerUrl = brokerUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public List<QueueConfig> getQueues() {
            return queues;
        }

        public void setQueues(List<QueueConfig> queues) {
            this.queues = queues;
        }
    }

    public static class QueueConfig {
        @NotBlank
        private String name = "listener.queue";
        @NotBlank
        private String schemaId;
        @NotBlank
        private String contentType = "application/json";
        @NotBlank
        private String brokerUrl = "tcp://localhost:61616";
        private String username;
        private String password;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSchemaId() {
            return schemaId;
        }

        public void setSchemaId(String schemaId) {
            this.schemaId = schemaId;
        }

        public String getContentType() {
            return contentType;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        public String getBrokerUrl() {
            return brokerUrl;
        }

        public void setBrokerUrl(String brokerUrl) {
            this.brokerUrl = brokerUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class Kafka {
        @NotEmpty
        private List<TopicConfig> topics = List.of(new TopicConfig());
        @NotBlank
        private String bootstrapServers = "localhost:9092";
        @NotBlank
        private String groupId = "webhook-event-source-api";

        public List<TopicConfig> getTopics() {
            return topics;
        }

        public void setTopics(List<TopicConfig> topics) {
            this.topics = topics;
        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class TopicConfig {
        @NotBlank
        private String name = "listener.source.topic";
        @NotBlank
        private String schemaId;
        @NotBlank
        private String contentType = "application/json";
        @NotBlank
        private String bootstrapServers = "localhost:9092";
        @NotBlank
        private String groupId = "webhook-event-source-api";

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSchemaId() {
            return schemaId;
        }

        public void setSchemaId(String schemaId) {
            this.schemaId = schemaId;
        }

        public String getContentType() {
            return contentType;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class Aws {
        @NotBlank
        private String region = "us-east-1";
        @NotNull
        private DynamoDb dynamoDb = new DynamoDb();

        public String getRegion() {
            return region;
        }

        public void setRegion(String region) {
            this.region = region;
        }

        public DynamoDb getDynamoDb() {
            return dynamoDb;
        }

        public void setDynamoDb(DynamoDb dynamoDb) {
            this.dynamoDb = dynamoDb;
        }
    }

    public static class DynamoDb {
        @NotBlank
        private String table = "event_schema";
        @NotNull
        private Duration schemaCacheTtl = Duration.ofMinutes(15);

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public Duration getSchemaCacheTtl() {
            return schemaCacheTtl;
        }

        public void setSchemaCacheTtl(Duration schemaCacheTtl) {
            this.schemaCacheTtl = schemaCacheTtl;
        }
    }

    public static class Output {
        @NotNull
        private Msk msk = new Msk();

        public Msk getMsk() {
            return msk;
        }

        public void setMsk(Msk msk) {
            this.msk = msk;
        }
    }

    public static class Msk {
        @NotBlank
        private String bootstrapServers = "localhost:9098";
        @NotBlank
        private String topicPrefix = "wh.ingress";
        private boolean iamAuthEnabled = true;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getTopicPrefix() {
            return topicPrefix;
        }

        public void setTopicPrefix(String topicPrefix) {
            this.topicPrefix = topicPrefix;
        }

        public boolean isIamAuthEnabled() {
            return iamAuthEnabled;
        }

        public void setIamAuthEnabled(boolean iamAuthEnabled) {
            this.iamAuthEnabled = iamAuthEnabled;
        }
    }
}
