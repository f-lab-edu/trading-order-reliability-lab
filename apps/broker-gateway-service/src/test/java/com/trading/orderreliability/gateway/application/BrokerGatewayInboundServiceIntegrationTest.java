package com.trading.orderreliability.gateway.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.CancelAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.CancelRejected;
import com.trading.orderreliability.broker.protocol.BrokerMessages.Fill;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRejected;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.CancelOrderCommandPayload;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.SubmitOrderCommandPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.gateway.messaging.command.BrokerCommandService;
import com.trading.orderreliability.gateway.messaging.outbox.GatewayOutboxMessageRecord;
import com.trading.orderreliability.gateway.persistence.GatewayCommandAttemptRecord;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;
import com.trading.orderreliability.gateway.support.GatewayMySqlTestContainerSupport;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.jdbc.Sql;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
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
@DisplayName("Broker Gateway inbound TCP event 변환")
class BrokerGatewayInboundServiceIntegrationTest extends GatewayMySqlTestContainerSupport {

    private final BrokerFrameCodec codec = new BrokerFrameCodec();

    @Autowired
    private BrokerGatewayInboundService inboundService;

    @Autowired
    private BrokerCommandService commandService;

    @Autowired
    private GatewayJdbcRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("ACKN 수신은 journal을 기록하고 BrokerOrderAcknowledged outbox를 생성한다")
    void ackCreatesJournalAndAcknowledgedOutbox() throws Exception {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createSubmitAttempt(orderId);
        OrderAccepted accepted = new OrderAccepted(
                header(BrokerMessageId.ACKN, attempt.wireMessageId(), orderId),
                "BRK-SIM-ACK-001",
                Instant.parse("2026-06-13T01:01:00Z")
        );

        inboundService.handleFrame(codec.encode(accepted));

        GatewayOutboxMessageRecord outbox = repository
                .findOutboxByAggregateIdAndMessageType(orderId, MessageTypes.BROKER_ORDER_ACKNOWLEDGED)
                .orElseThrow();
        JsonNode payload = objectMapper.readTree(outbox.payloadJson());
        assertThat(payload.path("brokerOrderId").asText()).isEqualTo("BRK-SIM-ACK-001");
        assertThat(payload.path("brokerEventDedupKey").asText()).contains("ACKN", attempt.wireMessageId());
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
    }

    @Test
    @DisplayName("ACKED 된 wireMessageId의 다른 brokerOrderId ACKN은 binding을 덮지 않고 parking 된다")
    void duplicateAckWithDifferentBrokerOrderIdDoesNotOverwriteBinding() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createSubmitAttempt(orderId);
        OrderAccepted accepted = new OrderAccepted(
                header(BrokerMessageId.ACKN, attempt.wireMessageId(), orderId),
                "BRK-SIM-ACK-ORIGINAL",
                Instant.parse("2026-06-13T01:01:00Z")
        );
        OrderAccepted mutatedDuplicate = new OrderAccepted(
                header(BrokerMessageId.ACKN, attempt.wireMessageId(), orderId),
                "BRK-SIM-ACK-MUTATED",
                Instant.parse("2026-06-13T01:01:01Z")
        );

        inboundService.handleFrame(codec.encode(accepted));
        inboundService.handleFrame(codec.encode(mutatedDuplicate));

        assertThat(repository.countOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED
        )).isEqualTo(1);
        assertThat(repository.findOrderIdByBrokerOrderId("SIM", "BRK-SIM-ACK-ORIGINAL")).contains(orderId);
        assertThat(repository.findOrderIdByBrokerOrderId("SIM", "BRK-SIM-ACK-MUTATED")).isEmpty();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_BINDING_MISMATCH")).isEqualTo(1);
    }

    @Test
    @DisplayName("UNKNOWN submit attempt의 늦은 ACKN은 binding을 만들지 않고 cancel dispatch 후보도 만들지 않는다")
    void lateAckForUnknownSubmitAttemptDoesNotCreateBindingOrDispatchableCancel() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createSubmitAttempt(orderId);
        commandService.handle(new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:30Z"),
                "trace-gateway-late-ack-cancel",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        ));
        assertThat(repository.markAttemptUnknown(
                attempt.id(),
                "SUBMIT_OUTCOME_UNKNOWN",
                "submit outcome unknown before late ack",
                Instant.parse("2026-06-13T01:01:00Z")
        )).isTrue();

        inboundService.handleFrame(codec.encode(new OrderAccepted(
                header(BrokerMessageId.ACKN, attempt.wireMessageId(), orderId),
                "BRK-SIM-LATE-ACK",
                Instant.parse("2026-06-13T01:02:00Z")
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED
        )).isEmpty();
        assertThat(repository.findOrderIdByBrokerOrderId("SIM", "BRK-SIM-LATE-ACK")).isEmpty();
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(cancelAttempt -> cancelAttempt.orderId().equals(orderId));
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_ATTEMPT_MISMATCH")).isEqualTo(1);
    }

    @Test
    @DisplayName("claim되지 않은 CREATED submit attempt의 ACKN은 binding과 outbox 없이 parking 된다")
    void ackForUnclaimedCreatedSubmitAttemptIsParkedWithoutBindingOrOutbox() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createUnclaimedSubmitAttempt(orderId);

        inboundService.handleFrame(codec.encode(new OrderAccepted(
                header(BrokerMessageId.ACKN, attempt.wireMessageId(), orderId),
                "BRK-SIM-UNCLAIMED-ACK",
                Instant.parse("2026-06-13T01:02:00Z")
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED
        )).isEmpty();
        assertThat(repository.findOrderIdByBrokerOrderId("SIM", "BRK-SIM-UNCLAIMED-ACK")).isEmpty();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_ATTEMPT_MISMATCH")).isEqualTo(1);
    }

    @Test
    @DisplayName("claim되지 않은 CREATED submit attempt의 RJCT는 reject outbox 없이 parking 된다")
    void rejectForUnclaimedCreatedSubmitAttemptIsParkedWithoutOutbox() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createUnclaimedSubmitAttempt(orderId);

        inboundService.handleFrame(codec.encode(new OrderRejected(
                header(BrokerMessageId.RJCT, attempt.wireMessageId(), orderId),
                "INVALID_PRICE",
                "invalid price"
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_ORDER_REJECTED
        )).isEmpty();
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "SUBMIT", "ACKED")).isZero();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_ATTEMPT_MISMATCH")).isEqualTo(1);
    }

    @Test
    @DisplayName("RJCT 수신은 BrokerOrderRejected outbox를 생성한다")
    void rejectCreatesRejectedOutbox() throws Exception {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createSubmitAttempt(orderId);
        OrderRejected rejected = new OrderRejected(
                header(BrokerMessageId.RJCT, attempt.wireMessageId(), orderId),
                "INVALID_PRICE",
                "invalid price"
        );

        inboundService.handleFrame(codec.encode(rejected));

        GatewayOutboxMessageRecord outbox = repository
                .findOutboxByAggregateIdAndMessageType(orderId, MessageTypes.BROKER_ORDER_REJECTED)
                .orElseThrow();
        JsonNode payload = objectMapper.readTree(outbox.payloadJson());
        assertThat(payload.path("rejectCode").asText()).isEqualTo("INVALID_PRICE");
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
    }

    @Test
    @DisplayName("PENDING_ACK 취소 후 RJCT 수신은 미송신 CANCEL attempt를 FAILED로 종결한다")
    void rejectAfterPendingAckCancelFailsUndispatchedCancelAttempt() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord submitAttempt = createSubmitAttempt(orderId);
        commandService.handle(new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:30Z"),
                "trace-gateway-inbound-cancel-before-reject",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        ));
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "CREATED")).isEqualTo(1);

        inboundService.handleFrame(codec.encode(new OrderRejected(
                header(BrokerMessageId.RJCT, submitAttempt.wireMessageId(), orderId),
                "INVALID_PRICE",
                "invalid price"
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_ORDER_REJECTED
        )).isPresent();
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "FAILED")).isEqualTo(1);
        assertThat(repository.findDispatchableCancelAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
    }

    @Test
    @DisplayName("FILL 수신은 leavesQty 기준으로 부분체결과 완전체결 event를 구분한다")
    void fillCreatesPartialOrFullOutboxByLeavesQty() {
        UUID partialOrderId = UUID.randomUUID();
        UUID filledOrderId = UUID.randomUUID();
        acknowledgeBinding(partialOrderId, "BRK-SIM-FILL-P");
        acknowledgeBinding(filledOrderId, "BRK-SIM-FILL-F");

        inboundService.handleFrame(codec.encode(new Fill(
                header(BrokerMessageId.FILL, "W-SIM-FILL-P-001", partialOrderId),
                "BRK-SIM-FILL-P",
                "EXEC-P",
                "P",
                40,
                40,
                60,
                Instant.parse("2026-06-13T01:02:00Z")
        )));
        inboundService.handleFrame(codec.encode(new Fill(
                header(BrokerMessageId.FILL, "W-SIM-FILL-F-001", filledOrderId),
                "BRK-SIM-FILL-F",
                "EXEC-F",
                "F",
                60,
                100,
                0,
                Instant.parse("2026-06-13T01:03:00Z")
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                partialOrderId,
                MessageTypes.BROKER_ORDER_PARTIALLY_FILLED
        )).isPresent();
        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                filledOrderId,
                MessageTypes.BROKER_ORDER_FILLED
        )).isPresent();
    }

    @Test
    @DisplayName("알 수 없는 wireMessageId ACKN은 broker event outbox 없이 parking 된다")
    void unknownAckIsParkedWithoutBrokerEventOutbox() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createSubmitAttempt(orderId);
        String unknownWireMessageId = "W-GW-UNKNOWN-ACK";
        OrderAccepted accepted = new OrderAccepted(
                header(BrokerMessageId.ACKN, unknownWireMessageId, orderId),
                "BRK-SIM-UNKNOWN-ACK",
                Instant.parse("2026-06-13T01:01:00Z")
        );

        assertThat(unknownWireMessageId).isNotEqualTo(attempt.wireMessageId());
        inboundService.handleFrame(codec.encode(accepted));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED
        )).isEmpty();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_ATTEMPT_MISMATCH")).isEqualTo(1);
    }

    @Test
    @DisplayName("binding 없는 FILL은 broker event outbox 없이 parking 된다")
    void fillWithoutBindingIsParkedWithoutBrokerEventOutbox() {
        UUID orderId = UUID.randomUUID();

        inboundService.handleFrame(codec.encode(new Fill(
                header(BrokerMessageId.FILL, "W-SIM-FILL-UNKNOWN", orderId),
                "BRK-SIM-UNKNOWN-FILL",
                "EXEC-UNKNOWN",
                "P",
                40,
                40,
                60,
                Instant.parse("2026-06-13T01:02:00Z")
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_ORDER_PARTIALLY_FILLED
        )).isEmpty();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_BINDING_MISMATCH")).isEqualTo(1);
    }

    @Test
    @DisplayName("CXLA 수신은 BrokerCancelAcknowledged outbox를 생성한다")
    void cancelAckCreatesCancelAcknowledgedOutbox() throws Exception {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createCancelAttemptWithBinding(orderId, "BRK-SIM-CANCEL-ACK");
        CancelAccepted accepted = new CancelAccepted(
                header(BrokerMessageId.CXLA, attempt.wireMessageId(), orderId),
                "BRK-SIM-CANCEL-ACK",
                Instant.parse("2026-06-13T01:04:00Z")
        );

        inboundService.handleFrame(codec.encode(accepted));

        GatewayOutboxMessageRecord outbox = repository
                .findOutboxByAggregateIdAndMessageType(orderId, MessageTypes.BROKER_CANCEL_ACKNOWLEDGED)
                .orElseThrow();
        JsonNode payload = objectMapper.readTree(outbox.payloadJson());
        assertThat(payload.path("brokerOrderId").asText()).isEqualTo("BRK-SIM-CANCEL-ACK");
        assertThat(payload.path("brokerEventDedupKey").asText()).contains("CXLA", attempt.wireMessageId());
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(1);
    }

    @Test
    @DisplayName("동일 CXLA 재수신은 journal만 남기고 BrokerCancelAcknowledged outbox를 중복 생성하지 않는다")
    void duplicateCancelAckDoesNotCreateDuplicateCancelAcknowledgedOutbox() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createCancelAttemptWithBinding(orderId, "BRK-SIM-CANCEL-DUP");
        CancelAccepted accepted = new CancelAccepted(
                header(BrokerMessageId.CXLA, attempt.wireMessageId(), orderId),
                "BRK-SIM-CANCEL-DUP",
                Instant.parse("2026-06-13T01:04:00Z")
        );

        inboundService.handleFrame(codec.encode(accepted));
        inboundService.handleFrame(codec.encode(accepted));

        assertThat(repository.countOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_CANCEL_ACKNOWLEDGED
        )).isEqualTo(1);
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(2);
    }

    @Test
    @DisplayName("CXLR 수신은 BrokerCancelRejected outbox를 생성한다")
    void cancelRejectCreatesCancelRejectedOutbox() throws Exception {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createCancelAttemptWithBinding(orderId, "BRK-SIM-CANCEL-REJECT");
        CancelRejected rejected = new CancelRejected(
                header(BrokerMessageId.CXLR, attempt.wireMessageId(), orderId),
                "BRK-SIM-CANCEL-REJECT",
                "TOO_LATE_CANCEL",
                "too late to cancel"
        );

        inboundService.handleFrame(codec.encode(rejected));

        GatewayOutboxMessageRecord outbox = repository
                .findOutboxByAggregateIdAndMessageType(orderId, MessageTypes.BROKER_CANCEL_REJECTED)
                .orElseThrow();
        JsonNode payload = objectMapper.readTree(outbox.payloadJson());
        assertThat(payload.path("rejectCode").asText()).isEqualTo("TOO_LATE_CANCEL");
        assertThat(payload.path("rejectMessage").asText()).isEqualTo("too late to cancel");
    }

    @Test
    @DisplayName("동일 CXLR 재수신은 journal만 남기고 BrokerCancelRejected outbox를 중복 생성하지 않는다")
    void duplicateCancelRejectDoesNotCreateDuplicateCancelRejectedOutbox() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createCancelAttemptWithBinding(orderId, "BRK-SIM-CANCEL-REJECT-DUP");
        CancelRejected rejected = new CancelRejected(
                header(BrokerMessageId.CXLR, attempt.wireMessageId(), orderId),
                "BRK-SIM-CANCEL-REJECT-DUP",
                "TOO_LATE_CANCEL",
                "too late to cancel"
        );

        inboundService.handleFrame(codec.encode(rejected));
        inboundService.handleFrame(codec.encode(rejected));

        assertThat(repository.countOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_CANCEL_REJECTED
        )).isEqualTo(1);
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(2);
    }

    @Test
    @DisplayName("brokerOrderId가 다른 CXLA는 broker event outbox 없이 binding mismatch로 parking 된다")
    void cancelAckWithMismatchedBrokerOrderIdIsParkedWithoutBrokerEventOutbox() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createCancelAttemptWithBinding(orderId, "BRK-SIM-CANCEL-MATCHED");

        inboundService.handleFrame(codec.encode(new CancelAccepted(
                header(BrokerMessageId.CXLA, attempt.wireMessageId(), orderId),
                "BRK-SIM-CANCEL-MISMATCHED",
                Instant.parse("2026-06-13T01:04:00Z")
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_CANCEL_ACKNOWLEDGED
        )).isEmpty();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_BINDING_MISMATCH")).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "ACKED")).isZero();
    }

    @Test
    @DisplayName("claim되지 않은 CANCEL attempt의 CXLA는 broker event outbox 없이 parking 된다")
    void cancelAckForUnclaimedCancelAttemptIsParkedWithoutBrokerEventOutbox() {
        UUID orderId = UUID.randomUUID();
        GatewayCommandAttemptRecord attempt = createUnclaimedCancelAttemptWithBinding(orderId, "BRK-SIM-CANCEL-UNCLAIMED");

        inboundService.handleFrame(codec.encode(new CancelAccepted(
                header(BrokerMessageId.CXLA, attempt.wireMessageId(), orderId),
                "BRK-SIM-CANCEL-UNCLAIMED",
                Instant.parse("2026-06-13T01:04:00Z")
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_CANCEL_ACKNOWLEDGED
        )).isEmpty();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_ATTEMPT_MISMATCH")).isEqualTo(1);
        assertThat(repository.countAttemptsByOrderIdTypeAndState(orderId, "CANCEL", "ACKED")).isZero();
    }

    @Test
    @DisplayName("알 수 없는 wireMessageId CXLA는 broker event outbox 없이 parking 된다")
    void unknownCancelAckIsParkedWithoutBrokerEventOutbox() {
        UUID orderId = UUID.randomUUID();
        createCancelAttemptWithBinding(orderId, "BRK-SIM-CANCEL-UNKNOWN");

        inboundService.handleFrame(codec.encode(new CancelAccepted(
                header(BrokerMessageId.CXLA, "W-GW-UNKNOWN-CANCEL-ACK", orderId),
                "BRK-SIM-CANCEL-UNKNOWN",
                Instant.parse("2026-06-13T01:04:00Z")
        )));

        assertThat(repository.findOutboxByAggregateIdAndMessageType(
                orderId,
                MessageTypes.BROKER_CANCEL_ACKNOWLEDGED
        )).isEmpty();
        assertThat(repository.countParkedByErrorCode("BROKER_EVENT_ATTEMPT_MISMATCH")).isEqualTo(1);
    }

    private static BrokerCommonHeader header(BrokerMessageId messageId, String wireMessageId, UUID orderId) {
        return BrokerCommonHeader.of(
                messageId,
                wireMessageId,
                orderId,
                "trace-gateway-inbound-test",
                Instant.parse("2026-06-13T01:00:00Z")
        );
    }

    private GatewayCommandAttemptRecord createSubmitAttempt(UUID orderId) {
        createUnclaimedSubmitAttempt(orderId);
        return repository.claimCreatedSubmitAttempts(
                        10,
                        Instant.parse("2026-06-13T01:00:30Z"),
                        Instant.parse("2026-06-13T01:01:00Z"),
                        "inbound-test-worker"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
    }

    private GatewayCommandAttemptRecord createUnclaimedSubmitAttempt(UUID orderId) {
        commandService.handle(new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.SUBMIT_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-inbound-test",
                objectMapper.valueToTree(new SubmitOrderCommandPayload(
                        orderId,
                        "ACC-GW-INBOUND",
                        "US",
                        "AAPL",
                        "BUY",
                        "LIMIT",
                        "DAY",
                        100,
                        "189.50"
                ))
        ));
        return repository.findCreatedSubmitAttempts(10)
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
    }

    private void acknowledgeBinding(UUID orderId, String brokerOrderId) {
        GatewayCommandAttemptRecord attempt = createSubmitAttempt(orderId);
        inboundService.handleFrame(codec.encode(new OrderAccepted(
                header(BrokerMessageId.ACKN, attempt.wireMessageId(), orderId),
                brokerOrderId,
                Instant.parse("2026-06-13T01:01:00Z")
        )));
    }

    private GatewayCommandAttemptRecord createCancelAttemptWithBinding(UUID orderId, String brokerOrderId) {
        createUnclaimedCancelAttemptWithBinding(orderId, brokerOrderId);
        return repository.claimDispatchableCancelAttempts(
                        10,
                        Instant.parse("2026-06-13T01:02:00Z"),
                        Instant.parse("2026-06-13T01:02:30Z"),
                        "inbound-test-worker"
                )
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
    }

    private GatewayCommandAttemptRecord createUnclaimedCancelAttemptWithBinding(UUID orderId, String brokerOrderId) {
        commandService.handle(new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-inbound-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        ));
        repository.updateBindingAccepted(orderId, "SIM", brokerOrderId, Instant.parse("2026-06-13T01:01:00Z"));
        return repository.findDispatchableCancelAttempts(10)
                .stream()
                .filter(attempt -> attempt.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
    }
}
