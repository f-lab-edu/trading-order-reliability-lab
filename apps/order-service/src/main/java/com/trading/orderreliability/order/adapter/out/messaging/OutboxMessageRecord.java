package com.trading.orderreliability.order.adapter.out.messaging;

import java.time.Instant;
import java.util.UUID;

public record OutboxMessageRecord(
        UUID id,
        String aggregateType,
        UUID aggregateId,
        String topicName,
        String messageKey,
        String messageType,
        String payloadJson,
        String headersJson,
        String status,
        int retryCount,
        Instant nextRetryAt,
        String lockedBy,
        Instant lockedUntil,
        Instant createdAt,
        Instant publishedAt,
        String lastError
) {
}
