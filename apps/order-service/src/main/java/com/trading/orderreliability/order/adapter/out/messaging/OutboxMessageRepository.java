package com.trading.orderreliability.order.adapter.out.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.common.messaging.OutboxStatus;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OutboxMessageRepository {

    private static final String AGGREGATE_TYPE_ORDER = "ORDER";

    private final JpaOutboxMessageRepository jpaRepository;
    private final ObjectMapper objectMapper;

    public OutboxMessageRepository(JpaOutboxMessageRepository jpaRepository, ObjectMapper objectMapper) {
        this.jpaRepository = jpaRepository;
        this.objectMapper = objectMapper;
    }

    public void appendBrokerCommand(
            UUID messageId,
            UUID orderId,
            String messageType,
            Object payload,
            String traceId,
            Instant createdAt
    ) {
        jpaRepository.save(OutboxMessageEntity.builder()
                .id(messageId)
                .aggregateType(AGGREGATE_TYPE_ORDER)
                .aggregateId(orderId)
                .topicName(MessagingTopics.BROKER_COMMAND)
                .messageKey(orderId.toString())
                .messageType(messageType)
                .payloadJson(writeJson(payload))
                .headersJson(writeJson(new OutboxHeaders(traceId)))
                .status(OutboxStatus.READY.name())
                .retryCount(0)
                .createdAt(createdAt)
                .build());
    }

    @Transactional
    public List<OutboxMessageRecord> claimPublishable(
            Instant now,
            String lockedBy,
            Instant lockedUntil,
            int batchSize,
            int maxRetryCount
    ) {
        List<OutboxMessageEntity> messages = jpaRepository.findPublishable(
                now,
                maxRetryCount,
                PageRequest.of(0, batchSize)
        );
        messages.forEach(message -> message.markPublishing(lockedBy, lockedUntil));
        jpaRepository.flush();
        return messages.stream().map(this::toRecord).toList();
    }

    @Transactional
    public boolean markSent(UUID messageId, String lockedBy, Instant publishedAt) {
        return jpaRepository.markSentIfOwned(
                UuidBytes.toBytes(messageId),
                lockedBy,
                publishedAt
        ) == 1;
    }

    @Transactional
    public boolean markFailed(UUID messageId, String lockedBy, int retryCount, Instant nextRetryAt, String lastError) {
        return jpaRepository.markFailedIfOwned(
                UuidBytes.toBytes(messageId),
                lockedBy,
                retryCount,
                nextRetryAt,
                truncate(lastError, 512)
        ) == 1;
    }

    @Transactional(readOnly = true)
    public Optional<OutboxMessageRecord> findById(UUID messageId) {
        return jpaRepository.findById(messageId).map(this::toRecord);
    }

    @Transactional(readOnly = true)
    public List<OutboxMessageRecord> findByAggregateId(UUID aggregateId) {
        return jpaRepository.findByAggregateIdOrderByCreatedAtAsc(aggregateId)
                .stream()
                .map(this::toRecord)
                .toList();
    }

    @Transactional(readOnly = true)
    public long countByAggregateIdAndMessageType(UUID aggregateId, String messageType) {
        return jpaRepository.countByAggregateIdAndMessageType(aggregateId, messageType);
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox JSON", e);
        }
    }

    private OutboxMessageRecord toRecord(OutboxMessageEntity entity) {
        return new OutboxMessageRecord(
                entity.getId(),
                entity.getAggregateType(),
                entity.getAggregateId(),
                entity.getTopicName(),
                entity.getMessageKey(),
                entity.getMessageType(),
                entity.getPayloadJson(),
                entity.getHeadersJson(),
                entity.getStatus(),
                entity.getRetryCount(),
                entity.getNextRetryAt(),
                entity.getLockedBy(),
                entity.getLockedUntil(),
                entity.getCreatedAt(),
                entity.getPublishedAt(),
                entity.getLastError()
        );
    }

    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    private record OutboxHeaders(String traceId) {
    }
}
