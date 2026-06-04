package com.trading.orderreliability.order.adapter.out.persistence;

import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.order.domain.model.OrderId;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OrderEventRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
        jdbcTemplate.update("""
                        INSERT INTO order_event (
                            id, order_id, event_type, event_version, source,
                            source_message_id, dedup_key, payload_hash, trace_id,
                            payload_json, occurred_at, recorded_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                UuidBytes.toBytes(eventId),
                UuidBytes.toBytes(orderId.value()),
                eventType,
                1,
                source,
                null,
                dedupKey,
                payloadHash,
                traceId,
                payloadJson,
                Timestamp.from(occurredAt),
                Timestamp.from(Instant.now())
        );
    }
}
