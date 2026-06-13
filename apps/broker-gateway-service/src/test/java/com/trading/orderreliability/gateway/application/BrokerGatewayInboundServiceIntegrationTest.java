package com.trading.orderreliability.gateway.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.Fill;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRejected;
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
}
