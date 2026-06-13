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
    @DisplayName("지원하지 않는 command는 TCP로 보내지 않고 parked_message로 격리한다")
    void unsupportedCommandIsParkedForM4() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> envelope = new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-cancel-test",
                objectMapper.valueToTree(new CancelOrderCommandPayload(orderId))
        );

        BrokerCommandHandlingResult result = commandService.handle(envelope);

        assertThat(result).isEqualTo(BrokerCommandHandlingResult.PARKED_UNSUPPORTED);
        assertThat(repository.findCreatedSubmitAttempts(10)).noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.countParkedByErrorCode("UNSUPPORTED_COMMAND_FOR_M4")).isEqualTo(1);
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
