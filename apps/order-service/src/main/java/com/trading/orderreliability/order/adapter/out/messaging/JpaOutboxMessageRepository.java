package com.trading.orderreliability.order.adapter.out.messaging;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.persistence.LockModeType;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

interface JpaOutboxMessageRepository extends JpaRepository<OutboxMessageEntity, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("""
            select m from OutboxMessageEntity m
            where m.status = 'READY'
               or (m.status = 'FAILED' and m.retryCount < :maxRetryCount and (m.nextRetryAt is null or m.nextRetryAt <= :now))
               or (m.status = 'PUBLISHING' and m.retryCount < :maxRetryCount and m.lockedUntil <= :now)
            order by m.createdAt asc
            """)
    List<OutboxMessageEntity> findPublishable(
            @Param("now") Instant now,
            @Param("maxRetryCount") int maxRetryCount,
            Pageable pageable
    );

    @Query(value = """
            UPDATE outbox_message
            SET status = 'SENT',
                published_at = :publishedAt,
                locked_by = NULL,
                locked_until = NULL,
                next_retry_at = NULL,
                last_error = NULL
            WHERE id = :messageId
              AND status = 'PUBLISHING'
              AND locked_by = :lockedBy
            """, nativeQuery = true)
    @org.springframework.data.jpa.repository.Modifying
    int markSentIfOwned(
            @Param("messageId") byte[] messageId,
            @Param("lockedBy") String lockedBy,
            @Param("publishedAt") Instant publishedAt
    );

    @Query(value = """
            UPDATE outbox_message
            SET status = 'FAILED',
                retry_count = :retryCount,
                next_retry_at = :nextRetryAt,
                last_error = :lastError,
                locked_by = NULL,
                locked_until = NULL
            WHERE id = :messageId
              AND status = 'PUBLISHING'
              AND locked_by = :lockedBy
            """, nativeQuery = true)
    @org.springframework.data.jpa.repository.Modifying
    int markFailedIfOwned(
            @Param("messageId") byte[] messageId,
            @Param("lockedBy") String lockedBy,
            @Param("retryCount") int retryCount,
            @Param("nextRetryAt") Instant nextRetryAt,
            @Param("lastError") String lastError
    );

    List<OutboxMessageEntity> findByAggregateIdOrderByCreatedAtAsc(UUID aggregateId);

    long countByAggregateIdAndMessageType(UUID aggregateId, String messageType);
}
