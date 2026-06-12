package com.trading.orderreliability.order.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.common.messaging.OutboxStatus;
import com.trading.orderreliability.order.adapter.out.messaging.outbox.OutboxMessageRecord;
import com.trading.orderreliability.order.adapter.out.messaging.outbox.OutboxMessageRepository;
import com.trading.orderreliability.order.application.command.CancelOrderCommand;
import com.trading.orderreliability.order.application.command.PlaceOrderCommand;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class OrderOutboxIntegrationTest extends MySqlTestContainerSupport {

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Autowired
    private OutboxMessageRepository outboxMessageRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void createOrderStoresSubmitOrderCommandOutboxInSameTransaction() throws Exception {
        PlaceOrderCommand command = placeCommand("m2-client-order-001", "AAPL", "trace-m2-submit-001");

        Order order = orderApplicationService.createOrder(command).order();

        List<OutboxMessageRecord> messages = outboxMessageRepository.findByAggregateId(order.orderId().value());
        assertThat(messages).hasSize(1);
        OutboxMessageRecord message = messages.getFirst();
        assertThat(message.topicName()).isEqualTo(MessagingTopics.BROKER_COMMAND);
        assertThat(message.messageType()).isEqualTo(MessageTypes.SUBMIT_ORDER_COMMAND);
        assertThat(message.messageKey()).isEqualTo(order.orderId().value().toString());
        assertThat(message.status()).isEqualTo(OutboxStatus.READY.name());

        JsonNode payload = objectMapper.readTree(message.payloadJson());
        assertThat(payload.path("orderId").asText()).isEqualTo(order.orderId().value().toString());
        assertThat(payload.path("accountId").asText()).isEqualTo("ACC-M2");
        assertThat(payload.path("symbol").asText()).isEqualTo("AAPL");
        assertThat(payload.path("orderQty").asLong()).isEqualTo(100);
        assertThat(payload.path("limitPrice").isTextual()).isTrue();
        assertThat(new BigDecimal(payload.path("limitPrice").asText())).isEqualByComparingTo("189.50");

        JsonNode headers = objectMapper.readTree(message.headersJson());
        assertThat(headers.path("traceId").asText()).isEqualTo("trace-m2-submit-001");
    }

    @Test
    void idempotentCreateOrderRetryDoesNotCreateAnotherOutboxMessage() {
        PlaceOrderCommand command = placeCommand("m2-client-order-002", "MSFT", "trace-m2-submit-002");

        Order firstOrder = orderApplicationService.createOrder(command).order();
        Order secondOrder = orderApplicationService.createOrder(command).order();

        assertThat(secondOrder.orderId()).isEqualTo(firstOrder.orderId());
        assertThat(outboxMessageRepository.countByAggregateIdAndMessageType(
                firstOrder.orderId().value(),
                MessageTypes.SUBMIT_ORDER_COMMAND
        )).isEqualTo(1);
    }

    @Test
    void cancelOrderStoresCancelOrderCommandOutbox() {
        Order order = orderApplicationService.createOrder(placeCommand("m2-client-order-003", "TSLA", "trace-m2-submit-003")).order();

        orderApplicationService.cancelOrder(
                order.orderId().value(),
                new CancelOrderCommand(new AccountId("ACC-M2"), "m2-cancel-001", "trace-m2-cancel-001")
        );

        assertThat(outboxMessageRepository.countByAggregateIdAndMessageType(
                order.orderId().value(),
                MessageTypes.CANCEL_ORDER_COMMAND
        )).isEqualTo(1);
    }

    private static PlaceOrderCommand placeCommand(String clientOrderId, String symbol, String traceId) {
        return new PlaceOrderCommand(
                clientOrderId,
                new AccountId("ACC-M2"),
                Market.US,
                new Symbol(symbol),
                OrderSide.BUY,
                OrderType.LIMIT,
                TimeInForce.DAY,
                OrderQuantity.positive(100),
                new OrderPrice(new BigDecimal("189.50")),
                traceId
        );
    }
}
