package com.trading.orderreliability.order.adapter.out.persistence;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

public interface JpaOrderEventRepository extends JpaRepository<OrderEventEntity, UUID> {
}
