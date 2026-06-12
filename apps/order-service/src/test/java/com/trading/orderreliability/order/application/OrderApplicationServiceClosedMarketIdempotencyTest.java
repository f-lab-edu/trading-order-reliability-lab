package com.trading.orderreliability.order.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.order.adapter.out.persistence.instruction.OrderInstructionRepository;
import com.trading.orderreliability.order.adapter.out.persistence.order.TradeOrderRepository;
import com.trading.orderreliability.order.application.command.PlaceOrderCommand;
import com.trading.orderreliability.order.application.exception.IdempotencyConflictException;
import com.trading.orderreliability.order.application.result.PlaceOrderResult;
import com.trading.orderreliability.order.application.support.HashingService;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.InstructionType;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderInstruction;
import com.trading.orderreliability.order.domain.model.OrderInstructionId;
import com.trading.orderreliability.order.domain.model.OrderInstructionStatus;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import java.math.BigDecimal;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = "order-service.market-state=CLOSED")
@ActiveProfiles("test")
class OrderApplicationServiceClosedMarketIdempotencyTest extends MySqlTestContainerSupport {

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Autowired
    private TradeOrderRepository orderRepository;

    @Autowired
    private OrderInstructionRepository instructionRepository;

    @Autowired
    private HashingService hashingService;

    @Autowired
    private UuidV7Generator uuidGenerator;

    @Test
    void 이미_생성된_주문의_동일_payload_재시도는_시장이_닫혀도_기존_주문을_반환한다() {
        PlaceOrderCommand command = placeCommand("client-order-closed-market-retry-001", "MSFT");
        Order existingOrder = seedExistingPlaceInstruction(command);

        PlaceOrderResult result = orderApplicationService.createOrder(command);

        assertThat(result.created()).isFalse();
        assertThat(result.order().orderId()).isEqualTo(existingOrder.orderId());
    }

    @Test
    void 이미_생성된_주문의_다른_payload_재시도는_시장이_닫혀도_멱등성_충돌로_처리한다() {
        seedExistingPlaceInstruction(placeCommand("client-order-closed-market-retry-002", "AAPL"));

        assertThatThrownBy(() -> orderApplicationService.createOrder(placeCommand("client-order-closed-market-retry-002", "MSFT")))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    private Order seedExistingPlaceInstruction(PlaceOrderCommand command) {
        Instant now = Instant.parse("2026-06-05T00:00:00Z");
        Order order = Order.createPendingAck(
                new OrderId(uuidGenerator.generate()),
                command.accountId(),
                command.market(),
                command.symbol(),
                command.side(),
                command.orderType(),
                command.timeInForce(),
                command.orderQty(),
                command.limitPrice(),
                now
        );
        String payloadJson = hashingService.canonicalJson(PlaceOrderIdempotencyPayload.from(command));
        String payloadHash = hashingService.sha256(payloadJson);
        OrderInstruction instruction = new OrderInstruction(
                new OrderInstructionId(uuidGenerator.generate()),
                order.orderId(),
                order.accountId(),
                InstructionType.PLACE,
                command.clientOrderId(),
                OrderInstructionStatus.REQUESTED,
                0,
                payloadHash,
                null,
                null,
                command.traceId(),
                now,
                now,
                null
        );

        orderRepository.insert(order);
        instructionRepository.insert(instruction, payloadJson);
        return order;
    }

    private static PlaceOrderCommand placeCommand(String clientOrderId, String symbol) {
        return new PlaceOrderCommand(
                clientOrderId,
                new AccountId("ACC-CLOSED-MARKET-001"),
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

    private record PlaceOrderIdempotencyPayload(
            String clientOrderId,
            String accountId,
            String market,
            String symbol,
            String side,
            String orderType,
            String timeInForce,
            long orderQty,
            String limitPrice
    ) {

        static PlaceOrderIdempotencyPayload from(PlaceOrderCommand command) {
            return new PlaceOrderIdempotencyPayload(
                    command.clientOrderId(),
                    command.accountId().value(),
                    command.market().name(),
                    command.symbol().value(),
                    command.side().name(),
                    command.orderType().name(),
                    command.timeInForce().name(),
                    command.orderQty().value(),
                    command.limitPrice().value().toPlainString()
            );
        }
    }
}
