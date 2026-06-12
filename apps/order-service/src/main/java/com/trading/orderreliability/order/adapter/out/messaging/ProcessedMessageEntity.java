package com.trading.orderreliability.order.adapter.out.messaging;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "processed_message")
@IdClass(ProcessedMessageId.class)
@Getter(AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class ProcessedMessageEntity {

    @Id
    @Column(name = "consumer_name", nullable = false, length = 64)
    private String consumerName;

    @Id
    @Convert(disableConversion = true)
    @Column(name = "message_id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID messageId;

    @Column(name = "message_type", nullable = false, length = 64)
    private String messageType;

    @Column(name = "message_key", nullable = false, length = 128)
    private String messageKey;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
