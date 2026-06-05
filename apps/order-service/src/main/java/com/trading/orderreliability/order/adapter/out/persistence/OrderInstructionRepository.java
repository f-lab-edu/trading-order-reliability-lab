package com.trading.orderreliability.order.adapter.out.persistence;

import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.InstructionType;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderInstruction;
import com.trading.orderreliability.order.domain.model.OrderInstructionId;
import com.trading.orderreliability.order.domain.model.OrderInstructionStatus;

import java.util.Optional;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OrderInstructionRepository {

    private final JpaOrderInstructionRepository jpaRepository;

    public OrderInstructionRepository(JpaOrderInstructionRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Transactional(noRollbackFor = DuplicateKeyException.class)
    public void insert(OrderInstruction instruction, String payloadJson) {
        int inserted = jpaRepository.insertIgnore(
                UuidBytes.toBytes(instruction.instructionId().value()),
                UuidBytes.toBytes(instruction.orderId().value()),
                instruction.accountId().value(),
                instruction.instructionType().name(),
                instruction.clientInstructionId(),
                instruction.status().name(),
                instruction.retryCount(),
                payloadJson,
                instruction.requestPayloadHash(),
                instruction.resultCode(),
                instruction.resultMessage(),
                instruction.traceId(),
                instruction.createdAt(),
                instruction.updatedAt(),
                instruction.resolvedAt()
        );
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
