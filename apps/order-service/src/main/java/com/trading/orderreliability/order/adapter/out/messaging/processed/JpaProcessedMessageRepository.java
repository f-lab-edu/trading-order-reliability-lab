package com.trading.orderreliability.order.adapter.out.messaging.processed;

import com.trading.orderreliability.common.id.UuidBytes;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface JpaProcessedMessageRepository extends JpaRepository<ProcessedMessageEntity, ProcessedMessageId> {

    default int insertIfAbsent(
            String consumerName,
            UUID messageId,
            String messageType,
            String messageKey,
            Instant processedAt
    ) {
        return insertIfAbsent(
                consumerName,
                UuidBytes.toBytes(messageId),
                messageType,
                messageKey,
                processedAt
        );
    }

    @Modifying
    @Query(value = """
            INSERT IGNORE INTO processed_message (
                consumer_name,
                message_id,
                message_type,
                message_key,
                processed_at
            )
            VALUES (
                :consumerName,
                :messageId,
                :messageType,
                :messageKey,
                :processedAt
            )
            """, nativeQuery = true)
    int insertIfAbsent(
            @Param("consumerName") String consumerName,
            @Param("messageId") byte[] messageId,
            @Param("messageType") String messageType,
            @Param("messageKey") String messageKey,
            @Param("processedAt") Instant processedAt
    );
}
