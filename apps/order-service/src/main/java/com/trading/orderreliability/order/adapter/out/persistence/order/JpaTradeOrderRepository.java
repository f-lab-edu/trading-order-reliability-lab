package com.trading.orderreliability.order.adapter.out.persistence.order;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import jakarta.persistence.LockModeType;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface JpaTradeOrderRepository extends JpaRepository<TradeOrderEntity, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select o from TradeOrderEntity o where o.id = :id")
    Optional<TradeOrderEntity> findByIdForUpdate(@Param("id") UUID id);

    List<TradeOrderEntity> findByAccountIdOrderByCreatedAtDesc(String accountId, Pageable pageable);

    List<TradeOrderEntity> findByAccountIdAndStatusOrderByCreatedAtDesc(String accountId, String status, Pageable pageable);
}
