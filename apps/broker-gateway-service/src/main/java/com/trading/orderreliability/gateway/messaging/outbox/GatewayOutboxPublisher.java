package com.trading.orderreliability.gateway.messaging.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GatewayOutboxPublisher {

    private final GatewayJdbcRepository repository;
    private final ObjectMapper objectMapper;
    private final GatewayOutboxMessageSender sender;
    private final GatewayOutboxPublisherProperties properties;
    private final Clock clock;
    private final String publisherId;

    @Autowired
    public GatewayOutboxPublisher(
            GatewayJdbcRepository repository,
            ObjectMapper objectMapper,
            GatewayOutboxMessageSender sender,
            GatewayOutboxPublisherProperties properties
    ) {
        this(repository, objectMapper, sender, properties, Clock.systemUTC(), "broker-gateway-" + UUID.randomUUID());
    }

    GatewayOutboxPublisher(
            GatewayJdbcRepository repository,
            ObjectMapper objectMapper,
            GatewayOutboxMessageSender sender,
            GatewayOutboxPublisherProperties properties,
            Clock clock,
            String publisherId
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.sender = sender;
        this.properties = properties;
        this.clock = clock;
        this.publisherId = publisherId;
    }

    public int publishAvailable() {
        Instant now = clock.instant();
        List<GatewayOutboxMessageRecord> messages = repository.claimPublishable(
                now,
                publisherId,
                now.plus(properties.getLockTtl()),
                properties.getBatchSize(),
                properties.getMaxRetryCount()
        );
        messages.forEach(this::publishOne);
        return messages.size();
    }

    private void publishOne(GatewayOutboxMessageRecord message) {
        try {
            MessageEnvelope<JsonNode> envelope = toEnvelope(message);
            sender.send(message.topicName(), message.messageKey(), envelope);
            repository.markOutboxSent(message.id(), publisherId, clock.instant());
        } catch (Exception e) {
            int nextRetryCount = message.retryCount() + 1;
            Instant nextRetryAt = clock.instant().plus(properties.nextRetryDelay(nextRetryCount));
            repository.markOutboxFailed(message.id(), publisherId, nextRetryCount, nextRetryAt, rootMessage(e));
        }
    }

    private MessageEnvelope<JsonNode> toEnvelope(GatewayOutboxMessageRecord message) throws Exception {
        JsonNode payload = objectMapper.readTree(message.payloadJson());
        JsonNode headers = objectMapper.readTree(message.headersJson());
        String traceId = headers.path("traceId").isMissingNode() ? null : headers.path("traceId").asText(null);
        return new MessageEnvelope<>(
                message.id(),
                message.messageType(),
                message.messageKey(),
                message.createdAt(),
                traceId,
                payload
        );
    }

    private static String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getName() : current.getMessage();
    }
}
