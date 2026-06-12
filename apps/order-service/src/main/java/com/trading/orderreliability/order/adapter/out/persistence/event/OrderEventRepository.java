package com.trading.orderreliability.order.adapter.out.persistence.event;

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
        jpaRepository.save(OrderEventEntity.builder()
                .id(eventId)
                .orderId(orderId.value())
                .eventType(eventType)
                .eventVersion(1)
                .source(source)
                .sourceMessageId(null)
                .dedupKey(dedupKey)
                .payloadHash(payloadHash)
                .traceId(traceId)
                .payloadJson(payloadJson)
                .occurredAt(occurredAt)
                .recordedAt(Instant.now())
                .build());
    }
}
