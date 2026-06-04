package com.trading.orderreliability.order.adapter.out.persistence;

import com.trading.orderreliability.order.domain.model.OrderId;

import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Repository;

@Repository
public class OrderEventRepository {

    private final JpaOrderEventRepository jpaRepository;

    public OrderEventRepository(JpaOrderEventRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    public void insert(
            UUID eventId,
            OrderId orderId,
            String eventType,
            String source,
            String dedupKey,
            String payloadHash,
            String traceId,
            String payloadJson,
            Instant occurredAt
    ) {
        OrderEventEntity entity = new OrderEventEntity();
        entity.setId(eventId);
        entity.setOrderId(orderId.value());
        entity.setEventType(eventType);
        entity.setEventVersion(1);
        entity.setSource(source);
        entity.setSourceMessageId(null);
        entity.setDedupKey(dedupKey);
        entity.setPayloadHash(payloadHash);
        entity.setTraceId(traceId);
        entity.setPayloadJson(payloadJson);
        entity.setOccurredAt(occurredAt);
        entity.setRecordedAt(Instant.now());
        jpaRepository.save(entity);
    }
}
