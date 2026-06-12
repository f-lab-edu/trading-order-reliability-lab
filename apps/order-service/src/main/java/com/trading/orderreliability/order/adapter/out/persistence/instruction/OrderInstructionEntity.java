package com.trading.orderreliability.order.adapter.out.persistence.instruction;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "order_instruction")
@Getter(AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
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
}
