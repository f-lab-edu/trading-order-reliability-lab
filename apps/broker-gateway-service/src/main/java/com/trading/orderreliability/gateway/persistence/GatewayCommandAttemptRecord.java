package com.trading.orderreliability.gateway.persistence;

import java.time.Instant;
import java.util.UUID;

public record GatewayCommandAttemptRecord(
        UUID id,
        UUID sourceMessageId,
        UUID orderId,
        String commandType,
        String brokerCode,
        String wireMessageId,
        String traceId,
        String brokerOrderId,
        String payloadJson,
        Instant createdAt
) {
}
