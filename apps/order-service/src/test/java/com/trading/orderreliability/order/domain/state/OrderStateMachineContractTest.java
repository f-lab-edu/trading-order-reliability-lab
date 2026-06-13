package com.trading.orderreliability.order.domain.state;

import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("주문 상태머신 계약")
class OrderStateMachineContractTest {

    private final OrderStateMachine stateMachine = new DefaultOrderStateMachine();
    private final Instant now = Instant.parse("2026-06-04T00:00:00Z");

    @Test
    @DisplayName("PENDING_ACK 주문은 브로커 접수 이벤트로 LIVE 상태가 된다")
    void pendingAckOrderAcknowledgedTransitionsToLive() {
        Order order = pendingAckOrder();

        OrderTransition transition = stateMachine.transition(
                order,
                OrderTransitionRequest.of(OrderTransitionTrigger.BROKER_ORDER_ACKNOWLEDGED, now, "ack")
        );

        assertThat(transition.nextStatus()).isEqualTo(OrderStatus.LIVE);
        assertThat(transition.nextOrder().terminalAt()).isNull();
    }

    @Test
    @DisplayName("PENDING_ACK 주문은 브로커 거절 이벤트로 REJECTED 상태가 된다")
    void pendingAckOrderRejectedTransitionsToRejected() {
        Order order = pendingAckOrder();

        OrderTransition transition = stateMachine.transition(
                order,
                OrderTransitionRequest.of(OrderTransitionTrigger.BROKER_ORDER_REJECTED, now, "reject")
        );

        assertThat(transition.nextStatus()).isEqualTo(OrderStatus.REJECTED);
        assertThat(transition.nextOrder().terminalAt()).isEqualTo(now);
    }

    @Test
    @DisplayName("LIVE 주문은 부분체결 이벤트로 PARTIALLY_FILLED 상태가 되고 체결 수량을 반영한다")
    void liveOrderPartiallyFilledTransitionsToPartiallyFilled() {
        Order order = pendingAckOrder().withStatus(OrderStatus.LIVE, now);

        OrderTransition transition = stateMachine.transition(
                order,
                OrderTransitionRequest.fill(
                        OrderTransitionTrigger.BROKER_ORDER_PARTIALLY_FILLED,
                        new OrderQuantity(40),
                        now,
                        "partial fill"
                )
        );

        assertThat(transition.nextStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(transition.nextOrder().cumQty().value()).isEqualTo(40);
        assertThat(transition.nextOrder().leavesQty().value()).isEqualTo(60);
    }

    @Test
    @DisplayName("LIVE 주문은 전량체결 이벤트로 FILLED 상태가 되고 잔량을 0으로 만든다")
    void liveOrderFilledTransitionsToFilled() {
        Order order = pendingAckOrder().withStatus(OrderStatus.LIVE, now);

        OrderTransition transition = stateMachine.transition(
                order,
                OrderTransitionRequest.fill(
                        OrderTransitionTrigger.BROKER_ORDER_FILLED,
                        new OrderQuantity(100),
                        now,
                        "full fill"
                )
        );

        assertThat(transition.nextStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(transition.nextOrder().cumQty().value()).isEqualTo(100);
        assertThat(transition.nextOrder().leavesQty().value()).isZero();
        assertThat(transition.nextOrder().terminalAt()).isEqualTo(now);
    }

    @Test
    @DisplayName("종결 주문은 늦게 도착한 비 reconciliation 이벤트로 상태가 바뀌지 않는다")
    void terminalOrderIgnoresLateNonReconciliationEvents() {
        Order order = pendingAckOrder().withFill(new OrderQuantity(100), now);

        OrderTransition transition = stateMachine.transition(
                order,
                OrderTransitionRequest.of(OrderTransitionTrigger.BROKER_ORDER_ACKNOWLEDGED, now.plusSeconds(1), "late ack")
        );

        assertThat(transition.changed()).isFalse();
        assertThat(transition.nextOrder()).isEqualTo(order);
    }

    private Order pendingAckOrder() {
        return Order.createPendingAck(
                new OrderId(UUID.randomUUID()),
                new AccountId("ACC-001"),
                Market.US,
                new Symbol("AAPL"),
                OrderSide.BUY,
                OrderType.LIMIT,
                TimeInForce.DAY,
                OrderQuantity.positive(100),
                new OrderPrice(new BigDecimal("189.50")),
                Instant.parse("2026-06-04T00:00:00Z")
        );
    }
}
