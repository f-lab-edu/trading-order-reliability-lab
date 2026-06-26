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
        String dispatchToken,
        String dispatchOwner,
        Instant dispatchLockedUntil,
        Instant createdAt
) {
    GatewayCommandAttemptRecord withDispatchLock(String dispatchToken, String dispatchOwner, Instant dispatchLockedUntil) {
        return new GatewayCommandAttemptRecord(
                id,
                sourceMessageId,
                orderId,
                commandType,
                brokerCode,
                wireMessageId,
                traceId,
                brokerOrderId,
                payloadJson,
                dispatchToken,
                dispatchOwner,
                dispatchLockedUntil,
                createdAt
        );
    }
}
