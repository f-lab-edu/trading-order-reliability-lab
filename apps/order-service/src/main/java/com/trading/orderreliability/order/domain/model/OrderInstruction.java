package com.trading.orderreliability.order.domain.model;

import java.time.Instant;
import java.util.Objects;

public record OrderInstruction(
        OrderInstructionId instructionId,
        OrderId orderId,
        AccountId accountId,
        InstructionType instructionType,
        String clientInstructionId,
        OrderInstructionStatus status,
        int retryCount,
        String requestPayloadHash,
        String resultCode,
        String resultMessage,
        String traceId,
        Instant createdAt,
        Instant updatedAt,
        Instant resolvedAt
) {

    public OrderInstruction {
        Objects.requireNonNull(instructionId, "instructionId must not be null");
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(instructionType, "instructionType must not be null");
        Objects.requireNonNull(clientInstructionId, "clientInstructionId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(requestPayloadHash, "requestPayloadHash must not be null");
        Objects.requireNonNull(traceId, "traceId must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (clientInstructionId.isBlank()) {
            throw new IllegalArgumentException("client instruction id must not be blank");
        }
        if (retryCount < 0) {
            throw new IllegalArgumentException("retry count must not be negative");
        }
    }
}
