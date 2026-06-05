package com.trading.orderreliability.order.adapter.out.persistence;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface JpaOrderInstructionRepository extends JpaRepository<OrderInstructionEntity, UUID> {

    @Modifying
    @Query(value = """
            INSERT IGNORE INTO order_instruction (
                id, order_id, account_id, instruction_type, client_instruction_id,
                status, retry_count, request_payload_json, request_payload_hash,
                result_code, result_message, trace_id, created_at, updated_at, resolved_at
            )
            VALUES (
                :id, :orderId, :accountId, :instructionType, :clientInstructionId,
                :status, :retryCount, :requestPayloadJson, :requestPayloadHash,
                :resultCode, :resultMessage, :traceId, :createdAt, :updatedAt, :resolvedAt
            )
            """, nativeQuery = true)
    int insertIgnore(
            @Param("id") byte[] id,
            @Param("orderId") byte[] orderId,
            @Param("accountId") String accountId,
            @Param("instructionType") String instructionType,
            @Param("clientInstructionId") String clientInstructionId,
            @Param("status") String status,
            @Param("retryCount") int retryCount,
            @Param("requestPayloadJson") String requestPayloadJson,
            @Param("requestPayloadHash") String requestPayloadHash,
            @Param("resultCode") String resultCode,
            @Param("resultMessage") String resultMessage,
            @Param("traceId") String traceId,
            @Param("createdAt") Instant createdAt,
            @Param("updatedAt") Instant updatedAt,
            @Param("resolvedAt") Instant resolvedAt
    );

    Optional<OrderInstructionEntity> findByAccountIdAndInstructionTypeAndClientInstructionId(
            String accountId,
            String instructionType,
            String clientInstructionId
    );

    Optional<OrderInstructionEntity> findByOrderIdAndInstructionTypeAndStatus(UUID orderId, String instructionType, String status);
}
