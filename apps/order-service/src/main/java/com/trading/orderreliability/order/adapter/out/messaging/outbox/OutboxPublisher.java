package com.trading.orderreliability.order.adapter.out.messaging.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.MessageEnvelope;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisher {

    private final OutboxMessageRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final OutboxMessageSender sender;
    private final OutboxPublisherProperties properties;
    private final Clock clock;
    private final String publisherId;

    @Autowired
    public OutboxPublisher(
            OutboxMessageRepository outboxRepository,
            ObjectMapper objectMapper,
            OutboxMessageSender sender,
            OutboxPublisherProperties properties
    ) {
        this(outboxRepository, objectMapper, sender, properties, Clock.systemUTC(), "order-service-" + UUID.randomUUID());
    }

    OutboxPublisher(
            OutboxMessageRepository outboxRepository,
            ObjectMapper objectMapper,
            OutboxMessageSender sender,
            OutboxPublisherProperties properties,
            Clock clock,
            String publisherId
    ) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.sender = sender;
        this.properties = properties;
        this.clock = clock;
        this.publisherId = publisherId;
    }

    public int publishAvailable() {
        Instant now = clock.instant();
        List<OutboxMessageRecord> messages = outboxRepository.claimPublishable(
                now,
                publisherId,
                now.plus(properties.getLockTtl()),
                properties.getBatchSize(),
                properties.getMaxRetryCount()
        );
        messages.forEach(this::publishOne);
        return messages.size();
    }

    private void publishOne(OutboxMessageRecord message) {
        try {
            MessageEnvelope<JsonNode> envelope = toEnvelope(message);
            sender.send(message.topicName(), message.messageKey(), envelope);
            outboxRepository.markSent(message.id(), publisherId, clock.instant());
        } catch (Exception e) {
            int nextRetryCount = message.retryCount() + 1;
            Instant nextRetryAt = clock.instant().plus(properties.nextRetryDelay(nextRetryCount));
            outboxRepository.markFailed(message.id(), publisherId, nextRetryCount, nextRetryAt, rootMessage(e));
        }
    }

    private MessageEnvelope<JsonNode> toEnvelope(OutboxMessageRecord message) throws Exception {
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
        String message = current.getMessage();
        return message == null ? current.getClass().getName() : message;
    }
}
