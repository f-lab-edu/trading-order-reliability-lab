package com.trading.orderreliability.order.application.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerCancelAcknowledgedPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerCancelRejectedPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderAcknowledgedPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderFilledPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderPartiallyFilledPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderRejectedPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.order.application.OrderApplicationService;
import com.trading.orderreliability.order.application.command.CancelOrderCommand;
import com.trading.orderreliability.order.application.command.PlaceOrderCommand;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(properties = {
        "order-service.messaging.kafka.broker-event-consumer-enabled=false",
        "order-service.messaging.outbox.enabled=false"
})
@ActiveProfiles("test")
@DisplayName("Order Service broker event 적용 통합 흐름")
class BrokerEventApplicationServiceIntegrationTest extends MySqlTestContainerSupport {

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Autowired
    private BrokerEventApplicationService brokerEventApplicationService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("BrokerOrderAcknowledged는 PENDING_ACK 주문을 LIVE로 전환하고 PLACE instruction을 완료한다")
    void acknowledgedMovesPendingAckOrderToLive() {
        Order order = createOrder("broker-ack-order");

        BrokerEventApplyResult result = brokerEventApplicationService.apply(ackEnvelope(order.orderId().value(), "dedup-ack-1", "hash-ack-1"));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.LIVE);
        assertThat(instructionStatus(order.orderId().value())).isEqualTo("COMPLETED");
        assertThat(orderEventCount(order.orderId().value(), "BrokerOrderAcknowledgedApplied")).isEqualTo(1);
    }

    @Test
    @DisplayName("BrokerOrderRejected는 PENDING_ACK 주문을 REJECTED로 종결한다")
    void rejectedMovesPendingAckOrderToRejected() {
        Order order = createOrder("broker-reject-order");

        BrokerEventApplyResult result = brokerEventApplicationService.apply(rejectEnvelope(order.orderId().value(), "dedup-reject-1", "hash-reject-1"));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(persisted.terminalAt()).isNotNull();
        assertThat(instructionStatus(order.orderId().value())).isEqualTo("REJECTED");
    }

    @Test
    @DisplayName("BrokerOrderPartiallyFilled는 수량 불변식을 유지하며 PARTIALLY_FILLED로 수렴한다")
    void partiallyFilledUpdatesQuantitiesAndStatus() {
        Order order = createOrder("broker-partial-order");

        BrokerEventApplyResult result = brokerEventApplicationService.apply(partialEnvelope(order.orderId().value(), "dedup-fill-p-1", "hash-fill-p-1"));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(persisted.cumQty().value()).isEqualTo(40);
        assertThat(persisted.leavesQty().value()).isEqualTo(60);
    }

    @Test
    @DisplayName("BrokerOrderPartiallyFilled의 cumQty와 leavesQty가 주문 수량과 맞지 않으면 상태를 바꾸지 않는다")
    void inconsistentPartialFillQuantitiesDoNotMutateOrder() {
        Order order = createOrder("broker-partial-invalid-order");

        assertThatThrownBy(() -> brokerEventApplicationService.apply(partialEnvelope(
                order.orderId().value(),
                "dedup-fill-invalid",
                "hash-fill-invalid",
                40,
                40,
                70
        ))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cumQty and leavesQty");

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(persisted.status()).isEqualTo(OrderStatus.PENDING_ACK);
        assertThat(orderEventCount(order.orderId().value(), "BrokerOrderPartiallyFilledApplied")).isZero();
    }

    @Test
    @DisplayName("BrokerOrderFilled는 주문을 FILLED terminal 상태로 수렴한다")
    void filledMovesOrderToFilledTerminalStatus() {
        Order order = createOrder("broker-filled-order");

        BrokerEventApplyResult result = brokerEventApplicationService.apply(filledEnvelope(order.orderId().value(), "dedup-fill-f-1", "hash-fill-f-1"));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(persisted.cumQty().value()).isEqualTo(100);
        assertThat(persisted.leavesQty().value()).isEqualTo(0);
        assertThat(persisted.terminalAt()).isNotNull();
    }

    @Test
    @DisplayName("동일 brokerEventDedupKey와 동일 payloadHash 재수신은 상태 변경을 건너뛴다")
    void duplicateBrokerEventWithSamePayloadHashIsSkipped() {
        Order order = createOrder("broker-duplicate-order");
        MessageEnvelope<JsonNode> envelope = ackEnvelope(order.orderId().value(), "dedup-ack-duplicate", "hash-ack-duplicate");

        BrokerEventApplyResult first = brokerEventApplicationService.apply(envelope);
        BrokerEventApplyResult second = brokerEventApplicationService.apply(envelope);

        assertThat(first).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(second).isEqualTo(BrokerEventApplyResult.DUPLICATE_SKIPPED);
        assertThat(orderEventCount(order.orderId().value(), "BrokerOrderAcknowledgedApplied")).isEqualTo(1);
    }

    @Test
    @DisplayName("동일 brokerEventDedupKey와 다른 payloadHash 재수신은 상태 변경 없이 parking한다")
    void brokerEventPayloadHashMismatchIsParked() {
        Order order = createOrder("broker-mismatch-order");
        brokerEventApplicationService.apply(ackEnvelope(order.orderId().value(), "dedup-ack-mismatch", "hash-ack-original"));

        BrokerEventApplyResult mismatch = brokerEventApplicationService.apply(ackEnvelope(order.orderId().value(), "dedup-ack-mismatch", "hash-ack-different"));

        assertThat(mismatch).isEqualTo(BrokerEventApplyResult.PAYLOAD_MISMATCH_PARKED);
        assertThat(orderEventCount(order.orderId().value(), "BrokerOrderAcknowledgedApplied")).isEqualTo(1);
        Long parkedCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM parked_message WHERE error_code = 'BROKER_EVENT_DEDUP_PAYLOAD_MISMATCH'",
                Long.class
        );
        assertThat(parkedCount).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("BrokerCancelAcknowledged는 PENDING_CANCEL 주문을 CANCELED로 종결하고 CANCEL instruction을 완료한다")
    void cancelAcknowledgedMovesPendingCancelOrderToCanceled() {
        Order order = createLiveOrder("broker-cancel-ack-order");
        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(order.accountId(), "cancel-ack-001", "trace-cancel-ack-001")
        );

        BrokerEventApplyResult result = brokerEventApplicationService.apply(cancelAckEnvelope(
                order.orderId().value(),
                "dedup-cancel-ack-1",
                "hash-cancel-ack-1"
        ));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.CANCELED);
        assertThat(persisted.leavesQty().value()).isZero();
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("COMPLETED");
        assertThat(orderEventCount(order.orderId().value(), "BrokerCancelAcknowledgedApplied")).isEqualTo(1);
    }

    @Test
    @DisplayName("BrokerCancelRejected는 PENDING_CANCEL 주문을 LIVE로 되돌리고 CANCEL instruction을 거절한다")
    void cancelRejectedMovesPendingCancelOrderBackToLive() {
        Order order = createLiveOrder("broker-cancel-reject-order");
        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(order.accountId(), "cancel-reject-001", "trace-cancel-reject-001")
        );

        BrokerEventApplyResult result = brokerEventApplicationService.apply(cancelRejectEnvelope(
                order.orderId().value(),
                "dedup-cancel-reject-1",
                "hash-cancel-reject-1"
        ));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.LIVE);
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("REJECTED");
        assertThat(orderEventCount(order.orderId().value(), "BrokerCancelRejectedApplied")).isEqualTo(1);
    }

    @Test
    @DisplayName("PENDING_ACK 취소 후 BrokerOrderAcknowledged가 도착하면 주문은 PENDING_CANCEL을 유지하고 PLACE만 완료한다")
    void acknowledgedAfterPendingAckCancelKeepsPendingCancelIntent() {
        Order order = createOrder("broker-pending-cancel-ack-order");
        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(order.accountId(), "cancel-before-ack-001", "trace-cancel-before-ack-001")
        );

        BrokerEventApplyResult result = brokerEventApplicationService.apply(ackEnvelope(
                order.orderId().value(),
                "dedup-ack-after-cancel",
                "hash-ack-after-cancel"
        ));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.PENDING_CANCEL);
        assertThat(instructionStatus(order.orderId().value(), "PLACE")).isEqualTo("COMPLETED");
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("REQUESTED");
    }

    @Test
    @DisplayName("PENDING_ACK 취소 후 BrokerOrderRejected가 도착하면 주문은 REJECTED가 되고 CANCEL instruction은 NOT_APPLIED가 된다")
    void rejectedAfterPendingAckCancelMarksCancelNotApplied() {
        Order order = createOrder("broker-pending-cancel-reject-order");
        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(order.accountId(), "cancel-before-reject-001", "trace-cancel-before-reject-001")
        );

        BrokerEventApplyResult result = brokerEventApplicationService.apply(rejectEnvelope(
                order.orderId().value(),
                "dedup-reject-after-cancel",
                "hash-reject-after-cancel"
        ));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.REJECTED);
        assertThat(instructionStatus(order.orderId().value(), "PLACE")).isEqualTo("REJECTED");
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("NOT_APPLIED");
    }

    @Test
    @DisplayName("FILLED 종결 후 늦은 BrokerCancelAcknowledged는 주문 상태를 유지하고 CANCEL instruction을 NOT_APPLIED로 정리한다")
    void lateCancelAcknowledgedAfterFilledMarksCancelNotApplied() {
        Order order = createLiveOrder("broker-late-cancel-ack-after-fill-order");
        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(order.accountId(), "cancel-late-ack-after-fill-001", "trace-cancel-late-ack-after-fill-001")
        );
        brokerEventApplicationService.apply(filledEnvelope(
                order.orderId().value(),
                "dedup-fill-before-late-cancel-ack",
                "hash-fill-before-late-cancel-ack"
        ));
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("NOT_APPLIED");

        BrokerEventApplyResult result = brokerEventApplicationService.apply(cancelAckEnvelope(
                order.orderId().value(),
                "dedup-late-cancel-ack-after-fill",
                "hash-late-cancel-ack-after-fill"
        ));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("NOT_APPLIED");
    }

    @Test
    @DisplayName("FILLED 종결 후 늦은 BrokerCancelRejected는 주문 상태를 유지하고 CANCEL instruction을 NOT_APPLIED로 정리한다")
    void lateCancelRejectedAfterFilledMarksCancelNotApplied() {
        Order order = createLiveOrder("broker-late-cancel-reject-after-fill-order");
        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(order.accountId(), "cancel-late-reject-after-fill-001", "trace-cancel-late-reject-after-fill-001")
        );
        brokerEventApplicationService.apply(filledEnvelope(
                order.orderId().value(),
                "dedup-fill-before-late-cancel-reject",
                "hash-fill-before-late-cancel-reject"
        ));
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("NOT_APPLIED");

        BrokerEventApplyResult result = brokerEventApplicationService.apply(cancelRejectEnvelope(
                order.orderId().value(),
                "dedup-late-cancel-reject-after-fill",
                "hash-late-cancel-reject-after-fill"
        ));

        Order persisted = orderApplicationService.getOrder(order.orderId().value());
        assertThat(result).isEqualTo(BrokerEventApplyResult.APPLIED);
        assertThat(persisted.status()).isEqualTo(OrderStatus.FILLED);
        assertThat(instructionStatus(order.orderId().value(), "CANCEL")).isEqualTo("NOT_APPLIED");
    }

    private Order createOrder(String clientOrderId) {
        return orderApplicationService.createOrder(new PlaceOrderCommand(
                clientOrderId,
                new AccountId("ACC-BROKER-EVENT"),
                Market.US,
                new Symbol("AAPL"),
                OrderSide.BUY,
                OrderType.LIMIT,
                TimeInForce.DAY,
                OrderQuantity.positive(100),
                new OrderPrice(new BigDecimal("189.50")),
                "trace-" + clientOrderId
        )).order();
    }

    private Order createLiveOrder(String clientOrderId) {
        Order order = createOrder(clientOrderId);
        brokerEventApplicationService.apply(ackEnvelope(
                order.orderId().value(),
                "dedup-live-" + clientOrderId,
                "hash-live-" + clientOrderId
        ));
        return orderApplicationService.getOrder(order.orderId().value());
    }

    private MessageEnvelope<JsonNode> ackEnvelope(UUID orderId, String dedupKey, String payloadHash) {
        return envelope(
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED,
                orderId,
                new BrokerOrderAcknowledgedPayload(
                        orderId,
                        dedupKey,
                        payloadHash,
                        "BRK-" + orderId.toString().substring(0, 8),
                        Instant.parse("2026-06-13T01:00:00Z")
                )
        );
    }

    private MessageEnvelope<JsonNode> rejectEnvelope(UUID orderId, String dedupKey, String payloadHash) {
        return envelope(
                MessageTypes.BROKER_ORDER_REJECTED,
                orderId,
                new BrokerOrderRejectedPayload(
                        orderId,
                        dedupKey,
                        payloadHash,
                        "INVALID_PRICE",
                        "invalid price",
                        Instant.parse("2026-06-13T01:01:00Z")
                )
        );
    }

    private MessageEnvelope<JsonNode> partialEnvelope(UUID orderId, String dedupKey, String payloadHash) {
        return partialEnvelope(orderId, dedupKey, payloadHash, 40, 40, 60);
    }

    private MessageEnvelope<JsonNode> partialEnvelope(
            UUID orderId,
            String dedupKey,
            String payloadHash,
            long lastFillQty,
            long cumQty,
            long leavesQty
    ) {
        return envelope(
                MessageTypes.BROKER_ORDER_PARTIALLY_FILLED,
                orderId,
                new BrokerOrderPartiallyFilledPayload(
                        orderId,
                        dedupKey,
                        payloadHash,
                        lastFillQty,
                        cumQty,
                        leavesQty,
                        "BRK-" + orderId.toString().substring(0, 8),
                        "EXEC-P",
                        Instant.parse("2026-06-13T01:02:00Z")
                )
        );
    }

    private MessageEnvelope<JsonNode> filledEnvelope(UUID orderId, String dedupKey, String payloadHash) {
        return envelope(
                MessageTypes.BROKER_ORDER_FILLED,
                orderId,
                new BrokerOrderFilledPayload(
                        orderId,
                        dedupKey,
                        payloadHash,
                        100,
                        100,
                        0,
                        "BRK-" + orderId.toString().substring(0, 8),
                        "EXEC-F",
                        Instant.parse("2026-06-13T01:03:00Z")
                )
        );
    }

    private MessageEnvelope<JsonNode> cancelAckEnvelope(UUID orderId, String dedupKey, String payloadHash) {
        return envelope(
                MessageTypes.BROKER_CANCEL_ACKNOWLEDGED,
                orderId,
                new BrokerCancelAcknowledgedPayload(
                        orderId,
                        dedupKey,
                        payloadHash,
                        "BRK-" + orderId.toString().substring(0, 8),
                        Instant.parse("2026-06-13T01:04:00Z")
                )
        );
    }

    private MessageEnvelope<JsonNode> cancelRejectEnvelope(UUID orderId, String dedupKey, String payloadHash) {
        return envelope(
                MessageTypes.BROKER_CANCEL_REJECTED,
                orderId,
                new BrokerCancelRejectedPayload(
                        orderId,
                        dedupKey,
                        payloadHash,
                        "BRK-" + orderId.toString().substring(0, 8),
                        "TOO_LATE_CANCEL",
                        "too late to cancel",
                        Instant.parse("2026-06-13T01:05:00Z")
                )
        );
    }

    private MessageEnvelope<JsonNode> envelope(String messageType, UUID orderId, Object payload) {
        return new MessageEnvelope<>(
                UUID.randomUUID(),
                messageType,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-broker-event-test",
                objectMapper.valueToTree(payload)
        );
    }

    private String instructionStatus(UUID orderId) {
        return instructionStatus(orderId, "PLACE");
    }

    private String instructionStatus(UUID orderId, String instructionType) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM order_instruction WHERE order_id = ? AND instruction_type = ?",
                String.class,
                com.trading.orderreliability.common.id.UuidBytes.toBytes(orderId),
                instructionType
        );
    }

    private Long orderEventCount(UUID orderId, String eventType) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_event WHERE order_id = ? AND event_type = ?",
                Long.class,
                com.trading.orderreliability.common.id.UuidBytes.toBytes(orderId),
                eventType
        );
    }
}
