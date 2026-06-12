package com.trading.orderreliability.common.messaging;

import java.time.Instant;
import java.util.UUID;

public record MessageEnvelope<T>(
        UUID messageId,
        String messageType,
        String messageKey,
        Instant occurredAt,
        String traceId,
        T payload
) {
}
