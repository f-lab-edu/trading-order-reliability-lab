package com.trading.orderreliability.order.adapter.out.messaging.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.MessageEnvelope;

import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Component;

@Component
class BrokerEventEnvelopeParser {

    private final ObjectMapper objectMapper;

    BrokerEventEnvelopeParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    MessageEnvelope<JsonNode> parse(String rawPayload) throws Exception {
        JsonNode root = objectMapper.readTree(rawPayload);
        return new MessageEnvelope<>(
                UUID.fromString(root.path("messageId").asText()),
                root.path("messageType").asText(),
                root.path("messageKey").asText(),
                objectMapper.convertValue(root.path("occurredAt"), Instant.class),
                root.path("traceId").isMissingNode() ? null : root.path("traceId").asText(null),
                root.path("payload")
        );
    }
}
