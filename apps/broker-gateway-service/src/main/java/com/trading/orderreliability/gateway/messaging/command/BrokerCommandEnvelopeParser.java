package com.trading.orderreliability.gateway.messaging.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.MessageEnvelope;

import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
class BrokerCommandEnvelopeParser {

    private final ObjectMapper objectMapper;

    BrokerCommandEnvelopeParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    MessageEnvelope<JsonNode> parse(String rawPayload) throws Exception {
        JsonNode root = objectMapper.readTree(rawPayload);
        return new MessageEnvelope<>(
                UUID.fromString(root.path("messageId").asText()),
                root.path("messageType").asText(),
                root.path("messageKey").asText(),
                objectMapper.convertValue(root.path("occurredAt"), java.time.Instant.class),
                root.path("traceId").isMissingNode() ? null : root.path("traceId").asText(null),
                root.path("payload")
        );
    }
}
