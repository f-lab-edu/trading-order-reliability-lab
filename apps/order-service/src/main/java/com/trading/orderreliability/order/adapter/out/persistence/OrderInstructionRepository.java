package com.trading.orderreliability.order.adapter.out.persistence;

import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.InstructionType;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderInstruction;
import com.trading.orderreliability.order.domain.model.OrderInstructionId;
import com.trading.orderreliability.order.domain.model.OrderInstructionStatus;

import java.util.Optional;

import jakarta.persistence.EntityManager;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OrderInstructionRepository {

    private final JpaOrderInstructionRepository jpaRepository;
    private final EntityManager entityManager;

    public OrderInstructionRepository(JpaOrderInstructionRepository jpaRepository, EntityManager entityManager) {
        this.jpaRepository = jpaRepository;
        this.entityManager = entityManager;
    }

    @Transactional(noRollbackFor = DuplicateKeyException.class)
    public void insert(OrderInstruction instruction, String payloadJson) {
        int inserted = entityManager.createNativeQuery("""
                        INSERT IGNORE INTO order_instruction (
                            id, order_id, account_id, instruction_type, client_instruction_id,
                            status, retry_count, request_payload_json, request_payload_hash,
                            result_code, result_message, trace_id, created_at, updated_at, resolved_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """)
                .setParameter(1, UuidBytes.toBytes(instruction.instructionId().value()))
                .setParameter(2, UuidBytes.toBytes(instruction.orderId().value()))
                .setParameter(3, instruction.accountId().value())
                .setParameter(4, instruction.instructionType().name())
                .setParameter(5, instruction.clientInstructionId())
                .setParameter(6, instruction.status().name())
                .setParameter(7, instruction.retryCount())
                .setParameter(8, payloadJson)
                .setParameter(9, instruction.requestPayloadHash())
                .setParameter(10, instruction.resultCode())
                .setParameter(11, instruction.resultMessage())
                .setParameter(12, instruction.traceId())
                .setParameter(13, instruction.createdAt())
                .setParameter(14, instruction.updatedAt())
                .setParameter(15, instruction.resolvedAt())
                .executeUpdate();
        if (inserted == 0) {
            throw new DuplicateKeyException("Duplicate order instruction idempotency key");
        }
    }

    public Optional<OrderInstruction> findByIdempotencyKey(String accountId, InstructionType instructionType, String clientInstructionId) {
        return jpaRepository.findByAccountIdAndInstructionTypeAndClientInstructionId(
                        accountId,
                        instructionType.name(),
                        clientInstructionId
                )
                .map(this::toDomain);
    }

    public Optional<OrderInstruction> findByOrderAndTypeAndClientInstructionId(
            OrderId orderId,
            InstructionType instructionType,
            String clientInstructionId
    ) {
        return jpaRepository.findByOrderIdAndInstructionTypeAndClientInstructionId(
                        orderId.value(),
                        instructionType.name(),
                        clientInstructionId
                )
                .map(this::toDomain);
    }

    public Optional<OrderInstruction> findActiveCancel(OrderId orderId) {
        return jpaRepository.findByOrderIdAndInstructionTypeAndStatus(
                        orderId.value(),
                        InstructionType.CANCEL.name(),
                        OrderInstructionStatus.REQUESTED.name()
                )
                .map(this::toDomain);
    }

    private OrderInstruction toDomain(OrderInstructionEntity entity) {
        return new OrderInstruction(
                new OrderInstructionId(entity.getId()),
                new OrderId(entity.getOrderId()),
                new AccountId(entity.getAccountId()),
                InstructionType.valueOf(entity.getInstructionType()),
                entity.getClientInstructionId(),
                OrderInstructionStatus.valueOf(entity.getStatus()),
                entity.getRetryCount(),
                entity.getRequestPayloadHash(),
                entity.getResultCode(),
                entity.getResultMessage(),
                entity.getTraceId(),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getResolvedAt()
        );
    }
}
