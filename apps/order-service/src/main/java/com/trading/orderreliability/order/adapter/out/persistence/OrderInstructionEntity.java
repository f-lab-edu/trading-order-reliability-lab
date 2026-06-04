package com.trading.orderreliability.order.adapter.out.persistence;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_instruction")
class OrderInstructionEntity {

    @Id
    @Column(name = "id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID id;

    @Column(name = "order_id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID orderId;

    @Column(name = "account_id", nullable = false, length = 64)
    private String accountId;

    @Column(name = "instruction_type", nullable = false, length = 32)
    private String instructionType;

    @Column(name = "client_instruction_id", nullable = false, length = 64)
    private String clientInstructionId;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "request_payload_json", columnDefinition = "JSON", nullable = false)
    private String requestPayloadJson;

    @Column(name = "request_payload_hash", nullable = false, length = 64)
    private String requestPayloadHash;

    @Column(name = "result_code", length = 64)
    private String resultCode;

    @Column(name = "result_message", length = 512)
    private String resultMessage;

    @Column(name = "trace_id", length = 64)
    private String traceId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "resolved_at")
    private Instant resolvedAt;

    protected OrderInstructionEntity() {
    }

    UUID getId() {
        return id;
    }

    UUID getOrderId() {
        return orderId;
    }

    String getAccountId() {
        return accountId;
    }

    String getInstructionType() {
        return instructionType;
    }

    String getClientInstructionId() {
        return clientInstructionId;
    }

    String getStatus() {
        return status;
    }

    int getRetryCount() {
        return retryCount;
    }

    String getRequestPayloadHash() {
        return requestPayloadHash;
    }

    String getResultCode() {
        return resultCode;
    }

    String getResultMessage() {
        return resultMessage;
    }

    String getTraceId() {
        return traceId;
    }

    Instant getCreatedAt() {
        return createdAt;
    }

    Instant getUpdatedAt() {
        return updatedAt;
    }

    Instant getResolvedAt() {
        return resolvedAt;
    }
}
