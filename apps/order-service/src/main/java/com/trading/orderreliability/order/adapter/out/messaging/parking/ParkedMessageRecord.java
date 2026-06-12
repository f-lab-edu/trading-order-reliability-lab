package com.trading.orderreliability.order.adapter.out.messaging.parking;

import java.time.Instant;
import java.util.UUID;

public record ParkedMessageRecord(
        UUID id,
        String sourceTopic,
        String consumerName,
        UUID messageId,
        String messageType,
        String messageKey,
        String traceId,
        String errorCode,
        int retryCount,
        String payloadText,
        String errorMessage,
        Instant failedAt,
        Instant parkedAt
) {
}
