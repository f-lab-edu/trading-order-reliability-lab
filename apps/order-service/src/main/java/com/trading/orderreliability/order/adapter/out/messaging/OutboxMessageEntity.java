package com.trading.orderreliability.order.adapter.out.messaging;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "outbox_message")
@Getter(AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class OutboxMessageEntity {

    @Id
    @Column(name = "id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, length = 32)
    private String aggregateType;

    @Column(name = "aggregate_id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID aggregateId;

    @Column(name = "topic_name", nullable = false, length = 128)
    private String topicName;

    @Column(name = "message_key", nullable = false, length = 128)
    private String messageKey;

    @Column(name = "message_type", nullable = false, length = 64)
    private String messageType;

    @Column(name = "payload_json", columnDefinition = "JSON", nullable = false)
    private String payloadJson;

    @Column(name = "headers_json", columnDefinition = "JSON", nullable = false)
    private String headersJson;

    @Column(name = "status", nullable = false, length = 16)
    private String status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "next_retry_at")
    private Instant nextRetryAt;

    @Column(name = "locked_by", length = 64)
    private String lockedBy;

    @Column(name = "locked_until")
    private Instant lockedUntil;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "last_error", length = 512)
    private String lastError;

    @Builder(access = AccessLevel.PACKAGE)
    private OutboxMessageEntity(
            UUID id,
            String aggregateType,
            UUID aggregateId,
            String topicName,
            String messageKey,
            String messageType,
            String payloadJson,
            String headersJson,
            String status,
            int retryCount,
            Instant nextRetryAt,
            String lockedBy,
            Instant lockedUntil,
            Instant createdAt,
            Instant publishedAt,
            String lastError
    ) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.topicName = topicName;
        this.messageKey = messageKey;
        this.messageType = messageType;
        this.payloadJson = payloadJson;
        this.headersJson = headersJson;
        this.status = status;
        this.retryCount = retryCount;
        this.nextRetryAt = nextRetryAt;
        this.lockedBy = lockedBy;
        this.lockedUntil = lockedUntil;
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
        this.lastError = lastError;
    }

    void markPublishing(String lockedBy, Instant lockedUntil) {
        this.status = "PUBLISHING";
        this.lockedBy = lockedBy;
        this.lockedUntil = lockedUntil;
    }

    void markSent(Instant publishedAt) {
        this.status = "SENT";
        this.publishedAt = publishedAt;
        this.lockedBy = null;
        this.lockedUntil = null;
        this.nextRetryAt = null;
        this.lastError = null;
    }

    void markFailed(int retryCount, Instant nextRetryAt, String lastError) {
        this.status = "FAILED";
        this.retryCount = retryCount;
        this.nextRetryAt = nextRetryAt;
        this.lastError = lastError;
        this.lockedBy = null;
        this.lockedUntil = null;
    }
}
