package com.trading.orderreliability.order.adapter.out.messaging.parking;

import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("message parking lot 통합 흐름")
class MessageParkingLotIntegrationTest extends MySqlTestContainerSupport {

    @Autowired
    private MessageParkingLot messageParkingLot;

    @Test
    @DisplayName("parse failure는 messageId 없이 parking 된다")
    void parseFailureIsParkedWithoutMessageId() {
        UUID parkedId = messageParkingLot.parkParseFailure(
                MessagingTopics.BROKER_EVENT,
                "order-broker-event-consumer",
                "{not-valid-json",
                "JSON parse failed",
                Instant.parse("2026-06-10T01:00:00Z")
        );

        ParkedMessageRecord parked = messageParkingLot.findById(parkedId).orElseThrow();

        assertThat(parked.errorCode()).isEqualTo(MessageParkingLot.ERROR_CODE_SCHEMA_PARSE_FAILED);
        assertThat(parked.messageId()).isNull();
        assertThat(parked.traceId()).isNull();
        assertThat(parked.payloadText()).isEqualTo("{not-valid-json");
        assertThat(parked.errorMessage()).isEqualTo("JSON parse failed");
        assertThat(parked.failedAt()).isEqualTo(Instant.parse("2026-06-10T01:00:00Z"));
        assertThat(parked.parkedAt()).isEqualTo(Instant.parse("2026-06-10T01:00:00Z"));
    }

    @Test
    @DisplayName("parse failure의 null payload와 null error message는 기본값으로 정규화된다")
    void parseFailureNormalizesNullPayloadAndNullErrorMessage() {
        UUID parkedId = messageParkingLot.parkParseFailure(
                MessagingTopics.BROKER_EVENT,
                "order-broker-event-consumer",
                null,
                null,
                Instant.parse("2026-06-10T01:00:30Z")
        );

        ParkedMessageRecord parked = messageParkingLot.findById(parkedId).orElseThrow();

        assertThat(parked.payloadText()).isEqualTo(MessageParkingLot.NULL_PAYLOAD_TEXT);
        assertThat(parked.errorMessage()).isEqualTo(MessageParkingLot.ERROR_CODE_SCHEMA_PARSE_FAILED);
    }

    @Test
    @DisplayName("parse failure의 긴 error message는 DB 컬럼 길이로 잘린다")
    void parseFailureTruncatesLongErrorMessageToColumnLength() {
        String longErrorMessage = "x".repeat(600);

        UUID parkedId = messageParkingLot.parkParseFailure(
                MessagingTopics.BROKER_EVENT,
                "order-broker-event-consumer",
                "{not-valid-json",
                longErrorMessage,
                Instant.parse("2026-06-10T01:00:45Z")
        );

        ParkedMessageRecord parked = messageParkingLot.findById(parkedId).orElseThrow();

        assertThat(parked.errorMessage()).hasSize(512);
        assertThat(parked.errorMessage()).isEqualTo("x".repeat(512));
    }

    @Test
    @DisplayName("반복 실패는 envelope 식별자와 함께 parking 된다")
    void repeatedFailureIsParkedWithEnvelopeIdentity() {
        UUID messageId = UUID.randomUUID();
        MessageEnvelope<String> envelope = new MessageEnvelope<>(
                messageId,
                MessageTypes.SUBMIT_ORDER_COMMAND,
                "order-parking-001",
                Instant.parse("2026-06-10T01:01:00Z"),
                "trace-parking-001",
                "{\"orderId\":\"order-parking-001\"}"
        );

        UUID parkedId = messageParkingLot.parkRepeatedFailure(
                MessagingTopics.BROKER_COMMAND,
                "broker-gateway-command-consumer",
                envelope,
                5,
                "business handler failed repeatedly",
                Instant.parse("2026-06-10T01:02:00Z")
        );

        ParkedMessageRecord parked = messageParkingLot.findById(parkedId).orElseThrow();

        assertThat(parked.errorCode()).isEqualTo(MessageParkingLot.ERROR_CODE_REPEATED_FAILURE);
        assertThat(parked.messageId()).isEqualTo(messageId);
        assertThat(parked.messageType()).isEqualTo(MessageTypes.SUBMIT_ORDER_COMMAND);
        assertThat(parked.messageKey()).isEqualTo("order-parking-001");
        assertThat(parked.traceId()).isEqualTo("trace-parking-001");
        assertThat(parked.retryCount()).isEqualTo(5);
        assertThat(parked.payloadText()).contains("\"messageId\":\"" + messageId + "\"");
        assertThat(parked.failedAt()).isEqualTo(Instant.parse("2026-06-10T01:02:00Z"));
        assertThat(parked.parkedAt()).isEqualTo(Instant.parse("2026-06-10T01:02:00Z"));
    }

    @Test
    @DisplayName("반복 실패의 null error message는 errorCode로 정규화된다")
    void repeatedFailureNormalizesNullErrorMessageToErrorCode() {
        UUID messageId = UUID.randomUUID();
        MessageEnvelope<String> envelope = new MessageEnvelope<>(
                messageId,
                MessageTypes.SUBMIT_ORDER_COMMAND,
                "order-parking-null-error",
                Instant.parse("2026-06-10T01:02:30Z"),
                "trace-parking-null-error",
                "{\"orderId\":\"order-parking-null-error\"}"
        );

        UUID parkedId = messageParkingLot.parkRepeatedFailure(
                MessagingTopics.BROKER_COMMAND,
                "broker-gateway-command-consumer",
                envelope,
                5,
                null,
                Instant.parse("2026-06-10T01:02:45Z")
        );

        ParkedMessageRecord parked = messageParkingLot.findById(parkedId).orElseThrow();

        assertThat(parked.errorCode()).isEqualTo(MessageParkingLot.ERROR_CODE_REPEATED_FAILURE);
        assertThat(parked.errorMessage()).isEqualTo(MessageParkingLot.ERROR_CODE_REPEATED_FAILURE);
    }

    @Test
    @DisplayName("알려진 envelope도 감사 이력을 위해 여러 번 parking 할 수 있다")
    void knownEnvelopeCanBeParkedMoreThanOnceForAuditHistory() {
        UUID messageId = UUID.randomUUID();
        MessageEnvelope<String> envelope = new MessageEnvelope<>(
                messageId,
                MessageTypes.SUBMIT_ORDER_COMMAND,
                "order-parking-duplicate",
                Instant.parse("2026-06-10T01:03:00Z"),
                "trace-parking-duplicate",
                "{\"orderId\":\"order-parking-duplicate\"}"
        );

        UUID firstParkedId = messageParkingLot.parkRepeatedFailure(
                MessagingTopics.BROKER_COMMAND,
                "broker-gateway-command-consumer",
                envelope,
                5,
                "business handler failed repeatedly",
                Instant.parse("2026-06-10T01:04:00Z")
        );
        UUID secondParkedId = messageParkingLot.parkRepeatedFailure(
                MessagingTopics.BROKER_COMMAND,
                "broker-gateway-command-consumer",
                envelope,
                6,
                "business handler failed repeatedly again",
                Instant.parse("2026-06-10T01:05:00Z")
        );

        ParkedMessageRecord firstParked = messageParkingLot.findById(firstParkedId).orElseThrow();
        ParkedMessageRecord secondParked = messageParkingLot.findById(secondParkedId).orElseThrow();

        assertThat(firstParkedId).isNotEqualTo(secondParkedId);
        assertThat(firstParked.messageId()).isEqualTo(messageId);
        assertThat(secondParked.messageId()).isEqualTo(messageId);
        assertThat(firstParked.traceId()).isEqualTo("trace-parking-duplicate");
        assertThat(secondParked.traceId()).isEqualTo("trace-parking-duplicate");
        assertThat(firstParked.retryCount()).isEqualTo(5);
        assertThat(secondParked.retryCount()).isEqualTo(6);
    }
}
