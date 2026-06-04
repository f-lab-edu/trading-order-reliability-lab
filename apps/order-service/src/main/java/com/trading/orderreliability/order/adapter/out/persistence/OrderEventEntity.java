package com.trading.orderreliability.order.adapter.out.persistence;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "order_event")
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

    protected OrderEventEntity() {
    }

    void setId(UUID id) {
        this.id = id;
    }

    void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    void setEventType(String eventType) {
        this.eventType = eventType;
    }

    void setEventVersion(int eventVersion) {
        this.eventVersion = eventVersion;
    }

    void setSource(String source) {
        this.source = source;
    }

    void setSourceMessageId(UUID sourceMessageId) {
        this.sourceMessageId = sourceMessageId;
    }

    void setDedupKey(String dedupKey) {
        this.dedupKey = dedupKey;
    }

    void setPayloadHash(String payloadHash) {
        this.payloadHash = payloadHash;
    }

    void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    void setPayloadJson(String payloadJson) {
        this.payloadJson = payloadJson;
    }

    void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    void setRecordedAt(Instant recordedAt) {
        this.recordedAt = recordedAt;
    }
}
