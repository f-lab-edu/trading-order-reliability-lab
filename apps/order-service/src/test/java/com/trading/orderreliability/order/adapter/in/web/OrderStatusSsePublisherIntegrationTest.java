package com.trading.orderreliability.order.adapter.in.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderAcknowledgedPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.order.application.OrderApplicationService;
import com.trading.orderreliability.order.application.broker.BrokerEventApplicationService;
import com.trading.orderreliability.order.application.broker.OrderStatusChangedNotification;
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "order-service.messaging.kafka.broker-event-consumer-enabled=false",
                "order-service.messaging.outbox.enabled=false"
        }
)
@ActiveProfiles("test")
@DisplayName("주문 상태 SSE 알림 통합 흐름")
class OrderStatusSsePublisherIntegrationTest extends MySqlTestContainerSupport {

    @LocalServerPort
    private int port;

    @Autowired
    private OrderStatusSsePublisher publisher;

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Autowired
    private BrokerEventApplicationService brokerEventApplicationService;

    @Autowired
    private ObjectMapper objectMapper;

    @AfterEach
    void completeSseEmitters() {
        publisher.completeAll();
    }

    @Test
    @DisplayName("주문 상태 stream은 order-status-changed 이벤트 형식으로 상태 변경을 전달한다")
    void streamEndpointDeliversOrderStatusChangedEvent() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> received = executor.submit(this::readOneSseEvent);
        Thread.sleep(300);

        UUID orderId = UUID.randomUUID();
        publisher.publish(new OrderStatusChangedNotification(
                orderId,
                OrderStatus.PENDING_ACK,
                OrderStatus.LIVE,
                0,
                100,
                Instant.parse("2026-06-13T01:00:00Z")
        ));

        String eventText = received.get(5, TimeUnit.SECONDS);
        executor.shutdownNow();
        assertThat(eventText).contains("event:order-status-changed");
        assertThat(eventText).contains(orderId.toString());
        assertThat(eventText).contains("LIVE");
    }

    @Test
    @DisplayName("broker event 적용 후 commit된 상태 변경은 SSE stream으로 전달된다")
    void brokerEventAfterCommitDeliversOrderStatusChangedEvent() throws Exception {
        Order order = createOrder("sse-broker-event-order");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> received = executor.submit(this::readOneSseEvent);
        Thread.sleep(300);

        brokerEventApplicationService.apply(ackEnvelope(order.orderId().value()));

        String eventText = received.get(5, TimeUnit.SECONDS);
        executor.shutdownNow();
        assertThat(eventText).contains("event:order-status-changed");
        assertThat(eventText).contains(order.orderId().value().toString());
        assertThat(eventText).contains("PENDING_ACK");
        assertThat(eventText).contains("LIVE");
    }

    private String readOneSseEvent() throws Exception {
        HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:%d/api/orders/stream".formatted(port))
                .toURL()
                .openConnection();
        connection.setReadTimeout(4_000);
        connection.setRequestProperty("Accept", "text/event-stream");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append('\n');
                if (line.isBlank() && builder.toString().contains("order-status-changed")) {
                    return builder.toString();
                }
            }
            return builder.toString();
        } finally {
            connection.disconnect();
        }
    }

    private Order createOrder(String clientOrderId) {
        return orderApplicationService.createOrder(new PlaceOrderCommand(
                clientOrderId,
                new AccountId("ACC-BROKER-SSE"),
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

    private MessageEnvelope<JsonNode> ackEnvelope(UUID orderId) {
        return new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-sse-broker-event-test",
                objectMapper.valueToTree(new BrokerOrderAcknowledgedPayload(
                        orderId,
                        "dedup-sse-" + orderId,
                        "hash-sse-" + orderId,
                        "BRK-" + orderId.toString().substring(0, 8),
                        Instant.parse("2026-06-13T01:00:00Z")
                ))
        );
    }
}
