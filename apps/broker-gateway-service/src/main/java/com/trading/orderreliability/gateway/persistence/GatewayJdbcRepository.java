package com.trading.orderreliability.gateway.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.common.messaging.OutboxStatus;
import com.trading.orderreliability.gateway.messaging.outbox.GatewayOutboxMessageRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class GatewayJdbcRepository {

    private static final String AGGREGATE_TYPE_ORDER = "ORDER";
    private static final int MAX_ERROR_MESSAGE_LENGTH = 512;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public GatewayJdbcRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public int insertProcessedIfAbsent(
            String consumerName,
            UUID messageId,
            String messageType,
            String messageKey,
            Instant processedAt
    ) {
        return jdbcTemplate.update(
                """
                        INSERT IGNORE INTO processed_message (
                            consumer_name, message_id, message_type, message_key, processed_at
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                consumerName,
                UuidBytes.toBytes(messageId),
                messageType,
                messageKey,
                processedAt
        );
    }

    @Transactional
    public void insertOrKeepBinding(UUID bindingId, UUID orderId, String brokerCode, Instant boundAt) {
        jdbcTemplate.update(
                """
                        INSERT INTO broker_order_binding (
                            id, order_id, broker_code, broker_order_id, bound_at, accepted_at
                        )
                        VALUES (?, ?, ?, NULL, ?, NULL)
                        ON DUPLICATE KEY UPDATE id = id
                        """,
                UuidBytes.toBytes(bindingId),
                UuidBytes.toBytes(orderId),
                brokerCode,
                boundAt
        );
    }

    @Transactional
    public void insertSubmitAttempt(
            UUID attemptId,
            UUID sourceMessageId,
            UUID orderId,
            String brokerCode,
            String wireMessageId,
            String traceId,
            Object payload,
            Instant createdAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO broker_command_attempt (
                            id, source_message_id, order_id, command_type, broker_code, wire_message_id,
                            trace_id, broker_order_id, payload_json, transport_state, sent_at, ack_deadline_at,
                            completed_at, error_code, error_message, created_at, updated_at
                        )
                        VALUES (?, ?, ?, 'SUBMIT', ?, ?, ?, NULL, ?, 'CREATED', NULL, NULL, NULL, NULL, NULL, ?, ?)
                        """,
                UuidBytes.toBytes(attemptId),
                UuidBytes.toBytes(sourceMessageId),
                UuidBytes.toBytes(orderId),
                brokerCode,
                wireMessageId,
                traceId,
                writeJson(payload),
                createdAt,
                createdAt
        );
    }

    @Transactional
    public void insertCancelAttempt(
            UUID attemptId,
            UUID sourceMessageId,
            UUID orderId,
            String brokerCode,
            String wireMessageId,
            String traceId,
            Object payload,
            Instant createdAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO broker_command_attempt (
                            id, source_message_id, order_id, command_type, broker_code, wire_message_id,
                            trace_id, broker_order_id, payload_json, transport_state, sent_at, ack_deadline_at,
                            completed_at, error_code, error_message, created_at, updated_at
                        )
                        VALUES (?, ?, ?, 'CANCEL', ?, ?, ?, NULL, ?, 'CREATED', NULL, NULL, NULL, NULL, NULL, ?, ?)
                        """,
                UuidBytes.toBytes(attemptId),
                UuidBytes.toBytes(sourceMessageId),
                UuidBytes.toBytes(orderId),
                brokerCode,
                wireMessageId,
                traceId,
                writeJson(payload),
                createdAt,
                createdAt
        );
    }

    @Transactional(readOnly = true)
    public List<GatewayCommandAttemptRecord> findCreatedSubmitAttempts(int limit) {
        return findCreatedSubmitAttempts(Instant.now(), limit);
    }

    @Transactional(readOnly = true)
    public List<GatewayCommandAttemptRecord> findCreatedSubmitAttempts(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                        SELECT id, source_message_id, order_id, command_type, broker_code, wire_message_id,
                               trace_id, broker_order_id, payload_json, dispatch_token, dispatch_owner,
                               dispatch_locked_until, created_at
                        FROM broker_command_attempt
                        WHERE command_type = 'SUBMIT'
                          AND transport_state = 'CREATED'
                          AND (dispatch_locked_until IS NULL OR dispatch_locked_until <= ?)
                          AND NOT EXISTS (
                              SELECT 1
                              FROM broker_message_journal j
                              WHERE j.broker_code = broker_command_attempt.broker_code
                                AND j.wire_message_id = broker_command_attempt.wire_message_id
                                AND j.direction = 'OUT'
                                AND j.msg_id = 'ORDR'
                          )
                        ORDER BY created_at
                        LIMIT ?
                        """,
                (rs, rowNum) -> toAttempt(rs),
                now,
                limit
        );
    }

    @Transactional(readOnly = true)
    public List<GatewayCommandAttemptRecord> findDispatchableCancelAttempts(int limit) {
        return findDispatchableCancelAttempts(Instant.now(), limit);
    }

    @Transactional(readOnly = true)
    public List<GatewayCommandAttemptRecord> findDispatchableCancelAttempts(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                        SELECT a.id, a.source_message_id, a.order_id, a.command_type, a.broker_code, a.wire_message_id,
                               a.trace_id, b.broker_order_id, a.payload_json, a.dispatch_token, a.dispatch_owner,
                               a.dispatch_locked_until, a.created_at
                        FROM broker_command_attempt a
                        JOIN broker_order_binding b
                          ON b.order_id = a.order_id
                         AND b.broker_code = a.broker_code
                        WHERE a.command_type = 'CANCEL'
                          AND a.transport_state = 'CREATED'
                          AND (a.dispatch_locked_until IS NULL OR a.dispatch_locked_until <= ?)
                          AND b.broker_order_id IS NOT NULL
                          AND b.accepted_at IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1
                              FROM broker_message_journal j
                              WHERE j.broker_code = a.broker_code
                                AND j.wire_message_id = a.wire_message_id
                                AND j.direction = 'OUT'
                                AND j.msg_id = 'CXLQ'
                          )
                        ORDER BY a.created_at
                        LIMIT ?
                        """,
                (rs, rowNum) -> toAttempt(rs),
                now,
                limit
        );
    }

    @Transactional
    public List<GatewayCommandAttemptRecord> claimCreatedSubmitAttempts(
            int limit,
            Instant claimedAt,
            Instant dispatchLockedUntil,
            String dispatchOwner
    ) {
        List<GatewayCommandAttemptRecord> records = findCreatedSubmitAttempts(claimedAt, limit);
        return records.stream()
                .map(record -> {
                    String dispatchToken = UUID.randomUUID().toString();
                    int updated = jdbcTemplate.update(
                            """
                                    UPDATE broker_command_attempt
                                    SET dispatch_token = ?,
                                        dispatch_owner = ?,
                                        dispatch_locked_until = ?,
                                        updated_at = ?
                                    WHERE id = ?
                                      AND transport_state = 'CREATED'
                                      AND (dispatch_locked_until IS NULL OR dispatch_locked_until <= ?)
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM broker_message_journal j
                                          WHERE j.broker_code = broker_command_attempt.broker_code
                                            AND j.wire_message_id = broker_command_attempt.wire_message_id
                                            AND j.direction = 'OUT'
                                            AND j.msg_id = 'ORDR'
                                      )
                                    """,
                            dispatchToken,
                            dispatchOwner,
                            dispatchLockedUntil,
                            claimedAt,
                            UuidBytes.toBytes(record.id()),
                            claimedAt
                    );
                    return updated == 1
                            ? record.withDispatchLock(dispatchToken, dispatchOwner, dispatchLockedUntil)
                            : null;
                })
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    @Transactional
    public List<GatewayCommandAttemptRecord> claimDispatchableCancelAttempts(
            int limit,
            Instant claimedAt,
            Instant dispatchLockedUntil,
            String dispatchOwner
    ) {
        List<GatewayCommandAttemptRecord> records = findDispatchableCancelAttempts(claimedAt, limit);
        return records.stream()
                .map(record -> {
                    String dispatchToken = UUID.randomUUID().toString();
                    int updated = jdbcTemplate.update(
                            """
                                    UPDATE broker_command_attempt
                                    SET dispatch_token = ?,
                                        dispatch_owner = ?,
                                        dispatch_locked_until = ?,
                                        broker_order_id = ?,
                                        updated_at = ?
                                    WHERE id = ?
                                      AND command_type = 'CANCEL'
                                      AND transport_state = 'CREATED'
                                      AND (dispatch_locked_until IS NULL OR dispatch_locked_until <= ?)
                                      AND NOT EXISTS (
                                          SELECT 1
                                          FROM broker_message_journal j
                                          WHERE j.broker_code = broker_command_attempt.broker_code
                                            AND j.wire_message_id = broker_command_attempt.wire_message_id
                                            AND j.direction = 'OUT'
                                            AND j.msg_id = 'CXLQ'
                                      )
                                    """,
                            dispatchToken,
                            dispatchOwner,
                            dispatchLockedUntil,
                            record.brokerOrderId(),
                            claimedAt,
                            UuidBytes.toBytes(record.id()),
                            claimedAt
                    );
                    return updated == 1
                            ? record.withDispatchLock(dispatchToken, dispatchOwner, dispatchLockedUntil)
                            : null;
                })
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<GatewayCommandAttemptRecord> findExpiredSubmitOutcomeAttempts(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                        SELECT id, source_message_id, order_id, command_type, broker_code, wire_message_id,
                               trace_id, broker_order_id, payload_json, dispatch_token, dispatch_owner,
                               dispatch_locked_until, created_at
                        FROM broker_command_attempt
                        WHERE command_type = 'SUBMIT'
                          AND (
                              (transport_state = 'SENT'
                                  AND ack_deadline_at IS NOT NULL
                                  AND ack_deadline_at <= ?)
                              OR
                              (transport_state = 'CREATED'
                                  AND dispatch_locked_until IS NOT NULL
                                  AND dispatch_locked_until <= ?
                                  AND EXISTS (
                                      SELECT 1
                                      FROM broker_message_journal j
                                      WHERE j.broker_code = broker_command_attempt.broker_code
                                        AND j.wire_message_id = broker_command_attempt.wire_message_id
                                        AND j.direction = 'OUT'
                                        AND j.msg_id = 'ORDR'
                                  ))
                          )
                        ORDER BY COALESCE(ack_deadline_at, dispatch_locked_until)
                        LIMIT ?
                        """,
                (rs, rowNum) -> toAttempt(rs),
                now,
                now,
                limit
        );
    }

    @Transactional(readOnly = true)
    public List<GatewayCommandAttemptRecord> findExpiredCancelOutcomeAttempts(Instant now, int limit) {
        return jdbcTemplate.query(
                """
                        SELECT id, source_message_id, order_id, command_type, broker_code, wire_message_id,
                               trace_id, broker_order_id, payload_json, dispatch_token, dispatch_owner,
                               dispatch_locked_until, created_at
                        FROM broker_command_attempt
                        WHERE command_type = 'CANCEL'
                          AND (
                              (transport_state = 'SENT'
                                  AND ack_deadline_at IS NOT NULL
                                  AND ack_deadline_at <= ?)
                              OR
                              (transport_state = 'CREATED'
                                  AND dispatch_locked_until IS NOT NULL
                                  AND dispatch_locked_until <= ?
                                  AND EXISTS (
                                      SELECT 1
                                      FROM broker_message_journal j
                                      WHERE j.broker_code = broker_command_attempt.broker_code
                                        AND j.wire_message_id = broker_command_attempt.wire_message_id
                                        AND j.direction = 'OUT'
                                        AND j.msg_id = 'CXLQ'
                                  ))
                          )
                        ORDER BY COALESCE(ack_deadline_at, dispatch_locked_until)
                        LIMIT ?
                        """,
                (rs, rowNum) -> toAttempt(rs),
                now,
                now,
                limit
        );
    }

    @Transactional
    public boolean markAttemptSent(UUID attemptId, String dispatchToken, Instant sentAt, Instant ackDeadlineAt) {
        return jdbcTemplate.update(
                """
                        UPDATE broker_command_attempt
                        SET transport_state = 'SENT',
                            sent_at = ?,
                            ack_deadline_at = ?,
                            dispatch_token = NULL,
                            dispatch_owner = NULL,
                            dispatch_locked_until = NULL,
                            updated_at = ?
                        WHERE id = ?
                          AND transport_state = 'CREATED'
                          AND dispatch_token = ?
                        """,
                sentAt,
                ackDeadlineAt,
                sentAt,
                UuidBytes.toBytes(attemptId),
                dispatchToken
        ) == 1;
    }

    @Transactional(readOnly = true)
    public boolean attemptStateIs(UUID attemptId, String state) {
        Integer count = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_command_attempt
                        WHERE id = ?
                          AND transport_state = ?
                        """,
                Integer.class,
                UuidBytes.toBytes(attemptId),
                state
        );
        return count != null && count == 1;
    }

    @Transactional(readOnly = true)
    public Optional<Instant> findAttemptAckDeadline(UUID attemptId) {
        return jdbcTemplate.query(
                """
                        SELECT ack_deadline_at
                        FROM broker_command_attempt
                        WHERE id = ?
                        """,
                ps -> ps.setBytes(1, UuidBytes.toBytes(attemptId)),
                rs -> rs.next() ? Optional.ofNullable(toInstant(rs.getTimestamp("ack_deadline_at"))) : Optional.empty()
        );
    }

    @Transactional
    public boolean markAttemptFailed(
            UUID attemptId,
            String dispatchToken,
            String errorCode,
            String errorMessage,
            Instant failedAt
    ) {
        return jdbcTemplate.update(
                """
                        UPDATE broker_command_attempt
                        SET transport_state = 'FAILED',
                            error_code = ?,
                            error_message = ?,
                            completed_at = ?,
                            dispatch_token = NULL,
                            dispatch_owner = NULL,
                            dispatch_locked_until = NULL,
                            updated_at = ?
                        WHERE id = ?
                          AND transport_state = 'CREATED'
                          AND dispatch_token = ?
                        """,
                errorCode,
                truncate(errorMessage),
                failedAt,
                failedAt,
                UuidBytes.toBytes(attemptId),
                dispatchToken
        ) == 1;
    }

    @Transactional
    public boolean markAttemptAcked(String brokerCode, String wireMessageId, String brokerOrderId, Instant completedAt) {
        return jdbcTemplate.update(
                """
                        UPDATE broker_command_attempt
                        SET transport_state = 'ACKED',
                            broker_order_id = ?,
                            completed_at = ?,
                            dispatch_token = NULL,
                            dispatch_owner = NULL,
                            dispatch_locked_until = NULL,
                            updated_at = ?
                        WHERE broker_code = ?
                          AND wire_message_id = ?
                          AND (
                              transport_state = 'SENT'
                              OR (transport_state = 'CREATED' AND dispatch_token IS NOT NULL)
                          )
                        """,
                blankToNull(brokerOrderId),
                completedAt,
                completedAt,
                brokerCode,
                wireMessageId
        ) == 1;
    }

    @Transactional
    public int markCreatedCancelAttemptsFailed(
            UUID orderId,
            String brokerCode,
            String errorCode,
            String errorMessage,
            Instant failedAt
    ) {
        return jdbcTemplate.update(
                """
                        UPDATE broker_command_attempt
                        SET transport_state = 'FAILED',
                            error_code = ?,
                            error_message = ?,
                            completed_at = ?,
                            dispatch_token = NULL,
                            dispatch_owner = NULL,
                            dispatch_locked_until = NULL,
                            updated_at = ?
                        WHERE order_id = ?
                          AND broker_code = ?
                          AND command_type = 'CANCEL'
                          AND transport_state = 'CREATED'
                        """,
                errorCode,
                truncate(errorMessage),
                failedAt,
                failedAt,
                UuidBytes.toBytes(orderId),
                brokerCode
        );
    }

    @Transactional
    public boolean markAttemptUnknown(UUID attemptId, String errorCode, String errorMessage, Instant updatedAt) {
        return jdbcTemplate.update(
                """
                        UPDATE broker_command_attempt
                        SET transport_state = 'UNKNOWN',
                            error_code = ?,
                            error_message = ?,
                            dispatch_token = NULL,
                            dispatch_owner = NULL,
                            dispatch_locked_until = NULL,
                            updated_at = ?
                        WHERE id = ?
                          AND transport_state IN ('CREATED', 'SENT')
                        """,
                errorCode,
                truncate(errorMessage),
                updatedAt,
                UuidBytes.toBytes(attemptId)
        ) == 1;
    }

    @Transactional
    public boolean updateBindingAccepted(UUID orderId, String brokerCode, String brokerOrderId, Instant acceptedAt) {
        return jdbcTemplate.update(
                """
                        UPDATE broker_order_binding
                        SET broker_order_id = CASE
                                WHEN broker_order_id IS NULL THEN ?
                                ELSE broker_order_id
                            END,
                            accepted_at = CASE
                                WHEN accepted_at IS NULL THEN ?
                                ELSE accepted_at
                            END
                        WHERE order_id = ?
                          AND broker_code = ?
                          AND (broker_order_id IS NULL OR broker_order_id = ?)
                        """,
                brokerOrderId,
                acceptedAt,
                UuidBytes.toBytes(orderId),
                brokerCode,
                brokerOrderId
        ) == 1;
    }

    @Transactional
    public boolean acceptSubmitAcknowledgement(
            UUID orderId,
            String brokerCode,
            String wireMessageId,
            String brokerOrderId,
            Instant acceptedAt,
            Instant completedAt
    ) {
        return jdbcTemplate.update(
                """
                        UPDATE broker_order_binding b
                        JOIN broker_command_attempt a
                          ON a.order_id = b.order_id
                         AND a.broker_code = b.broker_code
                        SET b.broker_order_id = CASE
                                WHEN b.broker_order_id IS NULL THEN ?
                                ELSE b.broker_order_id
                            END,
                            b.accepted_at = CASE
                                WHEN b.accepted_at IS NULL THEN ?
                                ELSE b.accepted_at
                            END,
                            a.transport_state = 'ACKED',
                            a.broker_order_id = ?,
                            a.completed_at = ?,
                            a.dispatch_token = NULL,
                            a.dispatch_owner = NULL,
                            a.dispatch_locked_until = NULL,
                            a.updated_at = ?
                        WHERE b.order_id = ?
                          AND b.broker_code = ?
                          AND a.wire_message_id = ?
                          AND a.command_type = 'SUBMIT'
                          AND (
                              a.transport_state = 'SENT'
                              OR (a.transport_state = 'CREATED' AND a.dispatch_token IS NOT NULL)
                          )
                          AND (b.broker_order_id IS NULL OR b.broker_order_id = ?)
                        """,
                brokerOrderId,
                acceptedAt,
                brokerOrderId,
                completedAt,
                completedAt,
                UuidBytes.toBytes(orderId),
                brokerCode,
                wireMessageId,
                brokerOrderId
        ) > 0;
    }

    @Transactional(readOnly = true)
    public boolean submitAttemptMatches(String brokerCode, String wireMessageId, UUID orderId) {
        Integer count = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_command_attempt
                        WHERE broker_code = ?
                          AND wire_message_id = ?
                          AND order_id = ?
                          AND command_type = 'SUBMIT'
                        """,
                Integer.class,
                brokerCode,
                wireMessageId,
                UuidBytes.toBytes(orderId)
        );
        return count != null && count > 0;
    }

    @Transactional(readOnly = true)
    public Optional<String> findSubmitAttemptResponseState(String brokerCode, String wireMessageId, UUID orderId) {
        return jdbcTemplate.query(
                """
                        SELECT CASE
                            WHEN transport_state = 'ACKED' THEN 'ACKED'
                            WHEN transport_state = 'SENT'
                                OR (transport_state = 'CREATED' AND dispatch_token IS NOT NULL)
                                THEN 'ELIGIBLE'
                            ELSE 'INELIGIBLE'
                        END AS response_state
                        FROM broker_command_attempt
                        WHERE broker_code = ?
                          AND wire_message_id = ?
                          AND order_id = ?
                          AND command_type = 'SUBMIT'
                        LIMIT 1
                        """,
                ps -> {
                    ps.setString(1, brokerCode);
                    ps.setString(2, wireMessageId);
                    ps.setBytes(3, UuidBytes.toBytes(orderId));
                },
                rs -> rs.next() ? Optional.of(rs.getString("response_state")) : Optional.empty()
        );
    }

    @Transactional(readOnly = true)
    public boolean cancelAttemptMatches(String brokerCode, String wireMessageId, UUID orderId) {
        Integer count = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_command_attempt
                        WHERE broker_code = ?
                          AND wire_message_id = ?
                          AND order_id = ?
                          AND command_type = 'CANCEL'
                          AND (
                              transport_state = 'SENT'
                              OR (transport_state = 'CREATED' AND dispatch_token IS NOT NULL)
                          )
                        """,
                Integer.class,
                brokerCode,
                wireMessageId,
                UuidBytes.toBytes(orderId)
        );
        return count != null && count > 0;
    }

    @Transactional(readOnly = true)
    public Optional<String> findCancelAttemptResponseState(String brokerCode, String wireMessageId, UUID orderId) {
        return jdbcTemplate.query(
                """
                        SELECT CASE
                            WHEN transport_state = 'ACKED' THEN 'ACKED'
                            WHEN transport_state = 'SENT'
                                OR (transport_state = 'CREATED' AND dispatch_token IS NOT NULL)
                                THEN 'ELIGIBLE'
                            ELSE 'INELIGIBLE'
                        END AS response_state
                        FROM broker_command_attempt
                        WHERE broker_code = ?
                          AND wire_message_id = ?
                          AND order_id = ?
                          AND command_type = 'CANCEL'
                        LIMIT 1
                        """,
                ps -> {
                    ps.setString(1, brokerCode);
                    ps.setString(2, wireMessageId);
                    ps.setBytes(3, UuidBytes.toBytes(orderId));
                },
                rs -> rs.next() ? Optional.of(rs.getString("response_state")) : Optional.empty()
        );
    }

    @Transactional(readOnly = true)
    public Optional<UUID> findOrderIdByBrokerOrderId(String brokerCode, String brokerOrderId) {
        return jdbcTemplate.query(
                """
                        SELECT order_id
                        FROM broker_order_binding
                        WHERE broker_code = ?
                          AND broker_order_id = ?
                        """,
                ps -> {
                    ps.setString(1, brokerCode);
                    ps.setString(2, brokerOrderId);
                },
                rs -> rs.next() ? Optional.of(UuidBytes.fromBytes(rs.getBytes("order_id"))) : Optional.empty()
        );
    }

    @Transactional
    public boolean insertOutboundJournalIfDispatchTokenMatches(
            UUID attemptId,
            String dispatchToken,
            UUID journalId,
            String brokerCode,
            String msgId,
            String wireMessageId,
            String traceId,
            String brokerOrderId,
            UUID orderId,
            byte[] rawMessage,
            Object parsedPayload,
            Instant recordedAt
    ) {
        try {
            return jdbcTemplate.update(
                    """
                            INSERT INTO broker_message_journal (
                                id, broker_code, direction, msg_id, wire_message_id, trace_id, broker_order_id,
                                order_id, parse_status, error_code, error_message, raw_message, parsed_payload_json,
                                payload_hash, recorded_at
                            )
                            SELECT ?, ?, 'OUT', ?, ?, ?, ?, ?, 'PARSED', NULL, NULL, ?, ?, NULL, ?
                            FROM broker_command_attempt a
                            WHERE a.id = ?
                              AND a.transport_state = 'CREATED'
                              AND a.dispatch_token = ?
                              AND a.dispatch_locked_until > ?
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM broker_message_journal j
                                  WHERE j.broker_code = ?
                                    AND j.direction = 'OUT'
                                    AND j.msg_id = ?
                                    AND j.wire_message_id = ?
                              )
                            """,
                    UuidBytes.toBytes(journalId),
                    brokerCode,
                    msgId,
                    wireMessageId,
                    traceId,
                    brokerOrderId,
                    UuidBytes.toBytes(orderId),
                    Base64.getEncoder().encodeToString(rawMessage),
                    writeJson(parsedPayload),
                    recordedAt,
                    UuidBytes.toBytes(attemptId),
                    dispatchToken,
                    recordedAt,
                    brokerCode,
                    msgId,
                    wireMessageId
            ) == 1;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    @Transactional(readOnly = true)
    public boolean dispatchTokenIsCurrent(UUID attemptId, String dispatchToken, Instant checkedAt) {
        Integer count = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_command_attempt
                        WHERE id = ?
                          AND transport_state = 'CREATED'
                          AND dispatch_token = ?
                          AND dispatch_locked_until > ?
                        """,
                Integer.class,
                UuidBytes.toBytes(attemptId),
                dispatchToken,
                checkedAt
        );
        return count != null && count == 1;
    }

    @Transactional
    public void insertInboundJournal(
            UUID journalId,
            String brokerCode,
            String msgId,
            String wireMessageId,
            String traceId,
            String brokerOrderId,
            UUID orderId,
            String parseStatus,
            String errorCode,
            String errorMessage,
            byte[] rawMessage,
            Object parsedPayload,
            String payloadHash,
            Instant recordedAt
    ) {
        insertJournal(
                journalId,
                brokerCode,
                "IN",
                msgId,
                wireMessageId,
                traceId,
                brokerOrderId,
                orderId,
                parseStatus,
                errorCode,
                errorMessage,
                rawMessage,
                parsedPayload,
                payloadHash,
                recordedAt
        );
    }

    private void insertJournal(
            UUID journalId,
            String brokerCode,
            String direction,
            String msgId,
            String wireMessageId,
            String traceId,
            String brokerOrderId,
            UUID orderId,
            String parseStatus,
            String errorCode,
            String errorMessage,
            byte[] rawMessage,
            Object parsedPayload,
            String payloadHash,
            Instant recordedAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO broker_message_journal (
                            id, broker_code, direction, msg_id, wire_message_id, trace_id, broker_order_id,
                            order_id, parse_status, error_code, error_message, raw_message, parsed_payload_json,
                            payload_hash, recorded_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                UuidBytes.toBytes(journalId),
                brokerCode,
                direction,
                msgId,
                wireMessageId,
                blankToNull(traceId),
                blankToNull(brokerOrderId),
                orderId == null ? null : UuidBytes.toBytes(orderId),
                parseStatus,
                errorCode,
                truncate(errorMessage),
                Base64.getEncoder().encodeToString(rawMessage),
                parsedPayload == null ? null : writeJson(parsedPayload),
                payloadHash,
                recordedAt
        );
    }

    @Transactional
    public void appendBrokerEvent(
            UUID messageId,
            UUID orderId,
            String messageType,
            Object payload,
            String traceId,
            Instant createdAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO outbox_message (
                            id, aggregate_type, aggregate_id, topic_name, message_key, message_type,
                            payload_json, headers_json, status, retry_count, next_retry_at, locked_by,
                            locked_until, created_at, published_at, last_error
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'READY', 0, NULL, NULL, NULL, ?, NULL, NULL)
                        """,
                UuidBytes.toBytes(messageId),
                AGGREGATE_TYPE_ORDER,
                UuidBytes.toBytes(orderId),
                MessagingTopics.BROKER_EVENT,
                orderId.toString(),
                messageType,
                writeJson(payload),
                writeJson(new OutboxHeaders(traceId)),
                createdAt
        );
    }

    @Transactional
    public UUID parkMessage(
            UUID parkingId,
            String sourceTopic,
            String consumerName,
            MessageEnvelope<?> envelope,
            String errorCode,
            String errorMessage,
            Instant parkedAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO parked_message (
                            id, source_topic, consumer_name, message_id, message_type, message_key,
                            trace_id, error_code, retry_count, payload_text, error_message, failed_at, parked_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                        """,
                UuidBytes.toBytes(parkingId),
                sourceTopic,
                consumerName,
                envelope == null ? null : UuidBytes.toBytes(envelope.messageId()),
                envelope == null ? null : envelope.messageType(),
                envelope == null ? null : envelope.messageKey(),
                envelope == null ? null : envelope.traceId(),
                errorCode,
                envelope == null ? "<null>" : writeJson(envelope),
                truncate(errorMessage),
                parkedAt,
                parkedAt
        );
        return parkingId;
    }

    @Transactional
    public UUID parkRawMessage(
            UUID parkingId,
            String sourceTopic,
            String consumerName,
            String rawPayload,
            String errorCode,
            String errorMessage,
            Instant parkedAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO parked_message (
                            id, source_topic, consumer_name, message_id, message_type, message_key,
                            trace_id, error_code, retry_count, payload_text, error_message, failed_at, parked_at
                        )
                        VALUES (?, ?, ?, NULL, NULL, NULL, NULL, ?, 0, ?, ?, ?, ?)
                        """,
                UuidBytes.toBytes(parkingId),
                sourceTopic,
                consumerName,
                errorCode,
                rawPayload == null ? "<null>" : rawPayload,
                truncate(errorMessage),
                parkedAt,
                parkedAt
        );
        return parkingId;
    }

    @Transactional
    public List<GatewayOutboxMessageRecord> claimPublishable(
            Instant now,
            String lockedBy,
            Instant lockedUntil,
            int batchSize,
            int maxRetryCount
    ) {
        List<GatewayOutboxMessageRecord> records = jdbcTemplate.query(
                """
                        SELECT id, aggregate_type, aggregate_id, topic_name, message_key, message_type,
                               payload_json, headers_json, status, retry_count, next_retry_at, locked_by,
                               locked_until, created_at, published_at, last_error
                        FROM outbox_message
                        WHERE status = 'READY'
                           OR (status = 'FAILED' AND retry_count < ? AND (next_retry_at IS NULL OR next_retry_at <= ?))
                           OR (status = 'PUBLISHING' AND retry_count < ? AND locked_until <= ?)
                        ORDER BY created_at
                        LIMIT ?
                        """,
                (rs, rowNum) -> toOutboxRecord(rs),
                maxRetryCount,
                now,
                maxRetryCount,
                now,
                batchSize
        );
        List<GatewayOutboxMessageRecord> claimed = new ArrayList<>();
        for (GatewayOutboxMessageRecord record : records) {
            int updated = jdbcTemplate.update(
                    """
                            UPDATE outbox_message
                            SET status = 'PUBLISHING',
                                locked_by = ?,
                                locked_until = ?
                            WHERE id = ?
                              AND (
                                  status = 'READY'
                                  OR (status = 'FAILED' AND retry_count < ? AND (next_retry_at IS NULL OR next_retry_at <= ?))
                                  OR (status = 'PUBLISHING' AND retry_count < ? AND locked_until <= ?)
                              )
                            """,
                    lockedBy,
                    lockedUntil,
                    UuidBytes.toBytes(record.id()),
                    maxRetryCount,
                    now,
                    maxRetryCount,
                    now
            );
            if (updated == 1) {
                claimed.add(record);
            }
        }
        return claimed;
    }

    @Transactional
    public boolean markOutboxSent(UUID messageId, String lockedBy, Instant publishedAt) {
        return jdbcTemplate.update(
                """
                        UPDATE outbox_message
                        SET status = 'SENT',
                            published_at = ?,
                            locked_by = NULL,
                            locked_until = NULL,
                            last_error = NULL
                        WHERE id = ?
                          AND locked_by = ?
                          AND status = 'PUBLISHING'
                        """,
                publishedAt,
                UuidBytes.toBytes(messageId),
                lockedBy
        ) == 1;
    }

    @Transactional
    public boolean markOutboxFailed(UUID messageId, String lockedBy, int retryCount, Instant nextRetryAt, String lastError) {
        return jdbcTemplate.update(
                """
                        UPDATE outbox_message
                        SET status = 'FAILED',
                            retry_count = ?,
                            next_retry_at = ?,
                            locked_by = NULL,
                            locked_until = NULL,
                            last_error = ?
                        WHERE id = ?
                          AND locked_by = ?
                          AND status = 'PUBLISHING'
                        """,
                retryCount,
                nextRetryAt,
                truncate(lastError),
                UuidBytes.toBytes(messageId),
                lockedBy
        ) == 1;
    }

    @Transactional(readOnly = true)
    public long countJournalByOrderId(UUID orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM broker_message_journal WHERE order_id = ?",
                Long.class,
                UuidBytes.toBytes(orderId)
        );
    }

    @Transactional(readOnly = true)
    public Optional<GatewayOutboxMessageRecord> findOutboxByAggregateIdAndMessageType(UUID aggregateId, String messageType) {
        return jdbcTemplate.query(
                """
                        SELECT id, aggregate_type, aggregate_id, topic_name, message_key, message_type,
                               payload_json, headers_json, status, retry_count, next_retry_at, locked_by,
                               locked_until, created_at, published_at, last_error
                        FROM outbox_message
                        WHERE aggregate_id = ?
                          AND message_type = ?
                        ORDER BY created_at
                        LIMIT 1
                        """,
                ps -> {
                    ps.setBytes(1, UuidBytes.toBytes(aggregateId));
                    ps.setString(2, messageType);
                },
                rs -> rs.next() ? Optional.of(toOutboxRecord(rs)) : Optional.empty()
        );
    }

    @Transactional(readOnly = true)
    public long countOutboxByAggregateIdAndMessageType(UUID aggregateId, String messageType) {
        return jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM outbox_message
                        WHERE aggregate_id = ?
                          AND message_type = ?
                        """,
                Long.class,
                UuidBytes.toBytes(aggregateId),
                messageType
        );
    }

    @Transactional(readOnly = true)
    public long countParkedByErrorCode(String errorCode) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM parked_message WHERE error_code = ?",
                Long.class,
                errorCode
        );
    }

    @Transactional(readOnly = true)
    public long countAttemptsByOrderIdAndState(UUID orderId, String state) {
        return jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_command_attempt
                        WHERE order_id = ?
                          AND transport_state = ?
                        """,
                Long.class,
                UuidBytes.toBytes(orderId),
                state
        );
    }

    @Transactional(readOnly = true)
    public long countAttemptsByOrderIdTypeAndState(UUID orderId, String commandType, String state) {
        return jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*)
                        FROM broker_command_attempt
                        WHERE order_id = ?
                          AND command_type = ?
                          AND transport_state = ?
                        """,
                Long.class,
                UuidBytes.toBytes(orderId),
                commandType,
                state
        );
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize gateway JSON", e);
        }
    }

    private static GatewayCommandAttemptRecord toAttempt(ResultSet rs) throws SQLException {
        return new GatewayCommandAttemptRecord(
                UuidBytes.fromBytes(rs.getBytes("id")),
                UuidBytes.fromBytes(rs.getBytes("source_message_id")),
                UuidBytes.fromBytes(rs.getBytes("order_id")),
                rs.getString("command_type"),
                rs.getString("broker_code"),
                rs.getString("wire_message_id"),
                rs.getString("trace_id"),
                rs.getString("broker_order_id"),
                rs.getString("payload_json"),
                rs.getString("dispatch_token"),
                rs.getString("dispatch_owner"),
                toInstant(rs.getTimestamp("dispatch_locked_until")),
                rs.getTimestamp("created_at").toInstant()
        );
    }

    private static GatewayOutboxMessageRecord toOutboxRecord(ResultSet rs) throws SQLException {
        return new GatewayOutboxMessageRecord(
                UuidBytes.fromBytes(rs.getBytes("id")),
                rs.getString("aggregate_type"),
                UuidBytes.fromBytes(rs.getBytes("aggregate_id")),
                rs.getString("topic_name"),
                rs.getString("message_key"),
                rs.getString("message_type"),
                rs.getString("payload_json"),
                rs.getString("headers_json"),
                rs.getString("status"),
                rs.getInt("retry_count"),
                toInstant(rs.getTimestamp("next_retry_at")),
                rs.getString("locked_by"),
                toInstant(rs.getTimestamp("locked_until")),
                rs.getTimestamp("created_at").toInstant(),
                toInstant(rs.getTimestamp("published_at")),
                rs.getString("last_error")
        );
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static String truncate(String value) {
        if (value == null || value.length() <= MAX_ERROR_MESSAGE_LENGTH) {
            return value;
        }
        return value.substring(0, MAX_ERROR_MESSAGE_LENGTH);
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private record OutboxHeaders(String traceId) {
    }
}
