package com.trading.orderreliability.order.adapter.out.persistence;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "order_event")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class OrderEventEntity {

    @Id
    @Column(name = "id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID id;

    @Column(name = "order_id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID orderId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "event_version", nullable = false)
    private int eventVersion;

    @Column(name = "source", nullable = false, length = 64)
    private String source;

    @Column(name = "source_message_id", columnDefinition = "BINARY(16)")
    private UUID sourceMessageId;

    @Column(name = "dedup_key", length = 192)
    private String dedupKey;

    @Column(name = "payload_hash", length = 64)
    private String payloadHash;

    @Column(name = "trace_id", length = 64)
    private String traceId;

    @Column(name = "payload_json", columnDefinition = "JSON", nullable = false)
    private String payloadJson;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "recorded_at", nullable = false)
    private Instant recordedAt;

    @Builder(access = AccessLevel.PACKAGE)
    private OrderEventEntity(
            UUID id,
            UUID orderId,
            String eventType,
            int eventVersion,
            String source,
            UUID sourceMessageId,
            String dedupKey,
            String payloadHash,
            String traceId,
            String payloadJson,
            Instant occurredAt,
            Instant recordedAt
    ) {
        this.id = id;
        this.orderId = orderId;
        this.eventType = eventType;
        this.eventVersion = eventVersion;
        this.source = source;
        this.sourceMessageId = sourceMessageId;
        this.dedupKey = dedupKey;
        this.payloadHash = payloadHash;
        this.traceId = traceId;
        this.payloadJson = payloadJson;
        this.occurredAt = occurredAt;
        this.recordedAt = recordedAt;
    }
}
