package com.trading.orderreliability.order.adapter.out.messaging.parking;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.MessageEnvelope;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class MessageParkingLot {

    public static final String ERROR_CODE_SCHEMA_PARSE_FAILED = "SCHEMA_PARSE_FAILED";
    public static final String ERROR_CODE_REPEATED_FAILURE = "REPEATED_FAILURE";
    public static final String NULL_PAYLOAD_TEXT = "<null>";
    private static final int MAX_ERROR_MESSAGE_LENGTH = 512;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final UuidV7Generator uuidGenerator;

    public MessageParkingLot(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            UuidV7Generator uuidGenerator
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.uuidGenerator = uuidGenerator;
    }

    @Transactional
    public UUID parkParseFailure(
            String sourceTopic,
            String consumerName,
            String rawPayload,
            String errorMessage,
            Instant parkedAt
    ) {
        UUID id = uuidGenerator.generate();
        insert(
                id,
                sourceTopic,
                consumerName,
                null,
                null,
                null,
                null,
                ERROR_CODE_SCHEMA_PARSE_FAILED,
                0,
                rawPayload,
                errorMessage,
                parkedAt,
                parkedAt
        );
        return id;
    }

    @Transactional
    public UUID parkRepeatedFailure(
            String sourceTopic,
            String consumerName,
            MessageEnvelope<?> envelope,
            int retryCount,
            String errorMessage,
            Instant parkedAt
    ) {
        UUID id = uuidGenerator.generate();
        insert(
                id,
                sourceTopic,
                consumerName,
                envelope.messageId(),
                envelope.messageType(),
                envelope.messageKey(),
                envelope.traceId(),
                ERROR_CODE_REPEATED_FAILURE,
                retryCount,
                writeJson(envelope),
                errorMessage,
                parkedAt,
                parkedAt
        );
        return id;
    }

    @Transactional(readOnly = true)
    public Optional<ParkedMessageRecord> findById(UUID id) {
        return jdbcTemplate.query(
                """
                        SELECT id, source_topic, consumer_name, message_id, message_type, message_key,
                               trace_id, error_code, retry_count, payload_text, error_message, failed_at, parked_at
                        FROM parked_message
                        WHERE id = ?
                        """,
                ps -> ps.setBytes(1, UuidBytes.toBytes(id)),
                rs -> rs.next() ? Optional.of(toRecord(rs)) : Optional.empty()
        );
    }

    private void insert(
            UUID id,
            String sourceTopic,
            String consumerName,
            UUID messageId,
            String messageType,
            String messageKey,
            String traceId,
            String errorCode,
            int retryCount,
            String payloadText,
            String errorMessage,
            Instant failedAt,
            Instant parkedAt
    ) {
        jdbcTemplate.update(
                """
                        INSERT INTO parked_message (
                            id, source_topic, consumer_name, message_id, message_type, message_key,
                            trace_id, error_code, retry_count, payload_text, error_message, failed_at, parked_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                UuidBytes.toBytes(id),
                sourceTopic,
                consumerName,
                messageId == null ? null : UuidBytes.toBytes(messageId),
                messageType,
                messageKey,
                traceId,
                errorCode,
                retryCount,
                normalizePayloadText(payloadText),
                normalizeErrorMessage(errorMessage, errorCode),
                failedAt,
                parkedAt
        );
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize parked message payload", e);
        }
    }

    private static ParkedMessageRecord toRecord(ResultSet rs) throws SQLException {
        byte[] messageId = rs.getBytes("message_id");
        return new ParkedMessageRecord(
                UuidBytes.fromBytes(rs.getBytes("id")),
                rs.getString("source_topic"),
                rs.getString("consumer_name"),
                messageId == null ? null : UuidBytes.fromBytes(messageId),
                rs.getString("message_type"),
                rs.getString("message_key"),
                rs.getString("trace_id"),
                rs.getString("error_code"),
                rs.getInt("retry_count"),
                rs.getString("payload_text"),
                rs.getString("error_message"),
                rs.getTimestamp("failed_at").toInstant(),
                rs.getTimestamp("parked_at").toInstant()
        );
    }

    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }

    private static String normalizePayloadText(String payloadText) {
        return payloadText == null ? NULL_PAYLOAD_TEXT : payloadText;
    }

    private static String normalizeErrorMessage(String errorMessage, String errorCode) {
        String nonNullErrorMessage = errorMessage == null ? errorCode : errorMessage;
        return truncate(nonNullErrorMessage, MAX_ERROR_MESSAGE_LENGTH);
    }
}
