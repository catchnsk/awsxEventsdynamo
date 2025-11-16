package com.beewaxus.webhooksvcs.pubsrc.schema;

import java.time.Instant;

public record SchemaDetailResponse(
        String eventSchemaId,
        String producerDomain,
        String eventName,
        String version,
        String eventSchemaHeader,
        String eventSchemaDefinitionJson,
        String eventSchemaDefinitionXml,
        String eventSchemaDefinitionAvro,
        String eventSample,
        String eventSchemaStatus,
        String hasSensitiveData,
        String producerSystemUsersId,
        String topicName,
        String topicStatus,
        Instant insertTs,
        String insertUser,
        Instant updateTs,
        String updateUser
) {}

