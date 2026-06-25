package com.trading.orderreliability.gateway.messaging.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.CancelOrderCommandPayload;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.SubmitOrderCommandPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.gateway.persistence.GatewayCommandAttemptRecord;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;
import com.trading.orderreliability.gateway.support.GatewayMySqlTestContainerSupport;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.jdbc.Sql;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "gateway.broker.command-dispatch-enabled=false",
        "gateway.messaging.kafka.consumer-enabled=false",
        "gateway.messaging.outbox.enabled=false"
})
@ActiveProfiles("test")
@Sql(statements = {
        "DELETE FROM outbox_message",
        "DELETE FROM broker_message_journal",
        "DELETE FROM broker_command_attempt",
        "DELETE FROM broker_order_binding",
        "DELETE FROM processed_message",
        "DELETE FROM parked_message"
})
@DisplayName("Broker Gateway command consumer 통합 흐름")
class BrokerCommandServiceIntegrationTest extends GatewayMySqlTestContainerSupport {

    @Autowired
    private BrokerCommandService commandService;

    @Autowired
    private GatewayJdbcRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("SubmitOrderCommand는 processed message와 SUBMIT attempt를 한 번만 기록한다")
    void submitOrderCommandCreatesProcessedMessageAndSubmitAttemptOnce() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = submitEnvelope(orderId, UUID.randomUUID());

        BrokerCommandHandlingResult first = commandService.handle(envelope);
        BrokerCommandHandlingResult second = commandService.handle(envelope);

        assertThat(first).isEqualTo(BrokerCommandHandlingResult.HANDLED);
        assertThat(second).isEqualTo(BrokerCommandHandlingResult.DUPLICATE_SKIPPED);
        List<GatewayCommandAttemptRecord> attempts = repository.findCreatedSubmitAttempts(10);
        assertThat(attempts)
                .extracting(GatewayCommandAttemptRecord::orderId)
                .contains(orderId);
        assertThat(attempts.stream().filter(attempt -> attempt.orderId().equals(orderId))).hasSize(1);
    }

    @Test
    @DisplayName("CancelOrderCommand는 processed message와 CANCEL attempt를 한 번만 기록하고 접수 binding 전에는 dispatch 후보가 아니다")
    void cancelOrderCommandCreatesProcessedMessageAndCancelAttemptOnce() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        BrokerCommandHandlingResult first = commandService.handle(envelope);
        BrokerCommandHandlingResult second = commandService.handle(envelope);

        assertThat(first).isEqualTo(BrokerCommandHandlingResult.HANDLED);
        assertThat(second).isEqualTo(BrokerCommandHandlingResult.DUPLICATE_SKIPPED);
        assertThat(repository.findCreatedSubmitAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "CREATED")).isEqualTo(1);
        assertThat(repository.countParkedByErrorCode("UNSUPPORTED_COMMAND")).isZero();
    }

    @Test
    @DisplayName("접수 binding이 생긴 CancelOrderCommand만 dispatch 후보로 조회된다")
    void cancelOrderCommandBecomesDispatchableAfterAcceptedBinding() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-binding-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        commandService.handle(envelope);
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));

        repository.updateBindingAccepted(orderId, "SIM", "BRK-CANCEL-BINDING-001", Instant.parse("2026-06-13T01:01:00Z"));

        assertThat(repository.findDispatchableCancelAttempts(10))
                .filteredOn(attempt -> attempt.orderId().equals(orderId))
                .singleElement()
                .extracting(GatewayCommandAttemptRecord::brokerOrderId)
                .isEqualTo("BRK-CANCEL-BINDING-001");
    }

    @Test
    @DisplayName("claim된 CANCEL attempt는 deadline 전에는 숨겨지고 deadline 이후 다시 dispatch 후보가 된다")
    void claimedCancelAttemptIsDispatchableAgainAfterAckDeadline() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-lease-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        commandService.handle(envelope);
        repository.updateBindingAccepted(orderId, "SIM", "BRK-CANCEL-LEASE-001", Instant.parse("2026-06-13T01:01:00Z"));

        assertThat(repository.claimDispatchableCancelAttempts(
                10,
                Instant.parse("2026-06-13T01:02:00Z"),
                Instant.parse("2026-06-13T01:02:30Z")
        )).filteredOn(attempt -> attempt.orderId().equals(orderId)).hasSize(1);

        assertThat(repository.findDispatchableCancelAttempts(Instant.parse("2026-06-13T01:02:10Z"), 10))
                .noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.findDispatchableCancelAttempts(Instant.parse("2026-06-13T01:02:31Z"), 10))
                .filteredOn(attempt -> attempt.orderId().equals(orderId))
                .singleElement()
                .extracting(GatewayCommandAttemptRecord::brokerOrderId)
                .isEqualTo("BRK-CANCEL-LEASE-001");
    }

    @Test
    @DisplayName("OUT CXLQ journal이 있는 CANCEL attempt는 deadline 이후에도 재송신 후보가 아니다")
    void cancelAttemptWithOutboundJournalIsNotDispatchableAgainAfterAckDeadline() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-out-journal-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        commandService.handle(envelope);
        repository.updateBindingAccepted(orderId, "SIM", "BRK-CANCEL-JOURNAL-001", Instant.parse("2026-06-13T01:01:00Z"));
        GatewayCommandAttemptRecord attempt = repository.claimDispatchableCancelAttempts(
                10,
                Instant.parse("2026-06-13T01:02:00Z"),
                Instant.parse("2026-06-13T01:02:30Z")
        ).stream()
                .filter(candidate -> candidate.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        repository.insertJournal(
                UUID.randomUUID(),
                attempt.brokerCode(),
                "OUT",
                "CXLQ",
                attempt.wireMessageId(),
                attempt.traceId(),
                attempt.brokerOrderId(),
                orderId,
                "PARSED",
                null,
                null,
                "cxlq".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                objectMapper.createObjectNode().put("brokerOrderId", attempt.brokerOrderId()),
                null,
                Instant.parse("2026-06-13T01:02:01Z")
        );

        assertThat(repository.findDispatchableCancelAttempts(Instant.parse("2026-06-13T01:02:31Z"), 10))
                .noneMatch(candidate -> candidate.orderId().equals(orderId));
    }

    @Test
    @DisplayName("지원하지 않는 command는 TCP로 보내지 않고 parked_message로 격리한다")
    void unsupportedCommandIsParkedWithoutTcpDispatch() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.QUERY_ORDER_STATUS_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-query-test",
                objectMapper.createObjectNode().put("orderId", orderId.toString())
        );

        BrokerCommandHandlingResult result = commandService.handle(envelope);

        assertThat(result).isEqualTo(BrokerCommandHandlingResult.PARKED_UNSUPPORTED);
        assertThat(repository.findCreatedSubmitAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.countParkedByErrorCode("UNSUPPORTED_COMMAND")).isEqualTo(1);
    }

    private MessageEnvelope<JsonNode> submitEnvelope(UUID orderId, UUID messageId) {
        return new MessageEnvelope<>(
                messageId,
                MessageTypes.SUBMIT_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-submit-test",
                objectMapper.valueToTree(new SubmitOrderCommandPayload(
                        orderId,
                        "ACC-GW",
                        "US",
                        "AAPL",
                        "BUY",
                        "LIMIT",
                        "DAY",
                        100,
                        "189.50"
                ))
        );
    }
}
