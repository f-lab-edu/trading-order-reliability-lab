package com.trading.orderreliability.order.adapter.out.persistence;

import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

public interface JpaOrderInstructionRepository extends JpaRepository<OrderInstructionEntity, UUID> {

    Optional<OrderInstructionEntity> findByAccountIdAndInstructionTypeAndClientInstructionId(
            String accountId,
            String instructionType,
            String clientInstructionId
    );

    Optional<OrderInstructionEntity> findByOrderIdAndInstructionTypeAndClientInstructionId(
            UUID orderId,
            String instructionType,
            String clientInstructionId
    );

    Optional<OrderInstructionEntity> findByOrderIdAndInstructionTypeAndStatus(UUID orderId, String instructionType, String status);
}
