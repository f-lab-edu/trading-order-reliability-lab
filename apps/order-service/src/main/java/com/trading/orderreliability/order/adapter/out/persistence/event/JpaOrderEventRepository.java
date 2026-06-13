package com.trading.orderreliability.order.adapter.out.persistence.event;

import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface JpaOrderEventRepository extends JpaRepository<OrderEventEntity, UUID> {

    @Query("select e.payloadHash from OrderEventEntity e where e.dedupKey = :dedupKey")
    java.util.Optional<String> findPayloadHashByDedupKey(@Param("dedupKey") String dedupKey);
}
