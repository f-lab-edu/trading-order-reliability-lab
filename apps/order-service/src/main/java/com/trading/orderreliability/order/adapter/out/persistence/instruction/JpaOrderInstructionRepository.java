package com.trading.orderreliability.order.adapter.out.persistence.instruction;

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
            INSERT INTO order_instruction (
                id, order_id, account_id, instruction_type, client_instruction_id,
                status, retry_count, request_payload_json, request_payload_hash,
                result_code, result_message, trace_id, created_at, updated_at, resolved_at
            )
            VALUES (
                :id, :orderId, :accountId, :instructionType, :clientInstructionId,
                :status, :retryCount, :requestPayloadJson, :requestPayloadHash,
                :resultCode, :resultMessage, :traceId, :createdAt, :updatedAt, :resolvedAt
            )
            -- 멱등키 중복 경합만 흡수하고, 다른 insert 오류는 그대로 실패시키기 위한 no-op update다.
            ON DUPLICATE KEY UPDATE id = id
            """, nativeQuery = true)
    int insertOrKeepExisting(
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

    @Modifying
    @Query(value = """
            UPDATE order_instruction
            SET status = :status,
                result_code = :resultCode,
                result_message = :resultMessage,
                updated_at = :updatedAt,
                resolved_at = :resolvedAt
            WHERE order_id = :orderId
              AND instruction_type = 'PLACE'
              AND status = 'REQUESTED'
            """, nativeQuery = true)
    int resolveRequestedPlaceInstruction(
            @Param("orderId") byte[] orderId,
            @Param("status") String status,
            @Param("resultCode") String resultCode,
            @Param("resultMessage") String resultMessage,
            @Param("updatedAt") Instant updatedAt,
            @Param("resolvedAt") Instant resolvedAt
    );
}
