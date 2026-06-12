package com.trading.orderreliability.order.application;

import com.trading.orderreliability.order.application.command.CancelOrderCommand;
import com.trading.orderreliability.order.application.command.PlaceOrderCommand;
import com.trading.orderreliability.order.application.exception.ActiveCancelConflictException;
import com.trading.orderreliability.order.application.exception.IdempotencyConflictException;
import com.trading.orderreliability.order.application.result.CancelOrderResult;
import com.trading.orderreliability.order.application.result.PlaceOrderResult;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;
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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class OrderApplicationServiceIntegrationTest extends MySqlTestContainerSupport {

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Test
    void createOrderCreatesPendingAckOrder() {
        PlaceOrderResult result = orderApplicationService.createOrder(placeCommand("client-order-001", "AAPL"));
        Order order = result.order();

        assertThat(result.created()).isTrue();
        assertThat(order.status()).isEqualTo(OrderStatus.PENDING_ACK);
        assertThat(order.cumQty().value()).isZero();
        assertThat(order.leavesQty().value()).isEqualTo(100);
    }

    @Test
    void duplicatePlaceOrderWithSamePayloadReturnsExistingOrder() {
        PlaceOrderResult first = orderApplicationService.createOrder(placeCommand("client-order-002", "MSFT"));

        PlaceOrderResult second = orderApplicationService.createOrder(placeCommand("client-order-002", "MSFT"));

        assertThat(second.created()).isFalse();
        assertThat(second.order().orderId()).isEqualTo(first.order().orderId());
    }

    @Test
    void duplicatePlaceOrderWithDifferentPayloadIsConflict() {
        orderApplicationService.createOrder(placeCommand("client-order-003", "AAPL"));

        assertThatThrownBy(() -> orderApplicationService.createOrder(placeCommand("client-order-003", "MSFT")))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void cancelOrderCreatesCancelInstructionAndMovesOrderToPendingCancel() {
        Order order = orderApplicationService.createOrder(placeCommand("client-order-004", "TSLA")).order();

        CancelOrderResult result = orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(new AccountId("ACC-001"), "cancel-001", "trace-cancel-001")
        );

        assertThat(result.order().status()).isEqualTo(OrderStatus.PENDING_CANCEL);
        assertThat(result.instruction().clientInstructionId()).isEqualTo("cancel-001");
    }

    @Test
    void secondActiveCancelRequestIsConflict() {
        Order order = orderApplicationService.createOrder(placeCommand("client-order-005", "NVDA")).order();
        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(new AccountId("ACC-001"), "cancel-002", "trace-cancel-002")
        );

        assertThatThrownBy(() -> orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(new AccountId("ACC-001"), "cancel-003", "trace-cancel-003")
        )).isInstanceOf(ActiveCancelConflictException.class);
    }

    @Test
    void 같은_계좌의_다른_주문에_이미_사용한_취소_멱등키를_쓰면_도메인_충돌로_거절한다() {
        // given: DB unique key는 accountId + instructionType + clientInstructionId 기준이다.
        // 같은 계좌의 두 주문을 준비하고 첫 주문에서 cancel-shared-key를 이미 사용한다.
        Order firstOrder = orderApplicationService.createOrder(placeCommand("client-order-006", "META")).order();
        Order secondOrder = orderApplicationService.createOrder(placeCommand("client-order-007", "AMZN")).order();
        orderApplicationService.cancelOrder(
                firstOrder.orderId().value(),
                new CancelOrderCommand(new AccountId("ACC-001"), "cancel-shared-key", "trace-cancel-first")
        );

        // when/then: 두 번째 주문에서 같은 cancel key를 쓰면 DB DuplicateKeyException이 아니라
        // 주문 서비스의 멱등성 정책 위반으로 해석되어 IdempotencyConflictException이 발생해야 한다.
        assertThatThrownBy(() -> orderApplicationService.cancelOrder(
                secondOrder.orderId().value(),
                new CancelOrderCommand(new AccountId("ACC-001"), "cancel-shared-key", "trace-cancel-second")
        )).isInstanceOf(IdempotencyConflictException.class);
    }

    private static PlaceOrderCommand placeCommand(String clientOrderId, String symbol) {
        return new PlaceOrderCommand(
                clientOrderId,
                new AccountId("ACC-001"),
                Market.US,
                new Symbol(symbol),
                OrderSide.BUY,
                OrderType.LIMIT,
                TimeInForce.DAY,
                OrderQuantity.positive(100),
                new OrderPrice(new BigDecimal("189.50")),
                "trace-" + clientOrderId
        );
    }
}
