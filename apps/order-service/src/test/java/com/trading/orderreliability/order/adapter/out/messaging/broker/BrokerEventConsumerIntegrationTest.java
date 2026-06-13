package com.trading.orderreliability.order.adapter.out.messaging.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderAcknowledgedPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.order.application.OrderApplicationService;
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
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.jdbc.Sql;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "order-service.messaging.kafka.broker-event-consumer-enabled=true",
        "order-service.messaging.outbox.enabled=false"
})
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = MessagingTopics.BROKER_EVENT, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@Sql(statements = {
        "DELETE FROM parked_message",
        "DELETE FROM processed_message",
        "DELETE FROM outbox_message",
        "DELETE FROM order_event",
        "DELETE FROM order_instruction",
        "DELETE FROM trade_order"
})
@DisplayName("Order Service broker event Kafka consumer 통합 흐름")
class BrokerEventConsumerIntegrationTest extends MySqlTestContainerSupport {

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry listenerEndpointRegistry;

    @BeforeEach
    void waitForBrokerEventListenerAssignment() {
        listenerEndpointRegistry.getListenerContainers()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container, 1));
    }

    @Test
    @DisplayName("BrokerOrderAcknowledged Kafka event는 processed message를 거쳐 주문을 LIVE로 전환한다")
    void brokerAcknowledgedKafkaEventMovesOrderToLiveThroughProcessedMessage() throws Exception {
        Order order = createOrder("m4-kafka-ack-order");
        MessageEnvelope<JsonNode> envelope = ackEnvelope(order.orderId().value(), "dedup-kafka-ack", "hash-kafka-ack");

        kafkaTemplate.send(MessagingTopics.BROKER_EVENT, order.orderId().value().toString(), envelope).get();

        await(() -> orderApplicationService.getOrder(order.orderId().value()).status() == OrderStatus.LIVE);
        assertThat(processedCount(envelope.messageId())).isEqualTo(1);
        assertThat(orderEventCount(order.orderId().value(), "BrokerOrderAcknowledgedApplied")).isEqualTo(1);
    }

    @Test
    @DisplayName("동일 BrokerOrderAcknowledged Kafka event 재수신은 processed message로 한 번만 적용된다")
    void duplicateBrokerAcknowledgedKafkaEventIsAppliedOnceByProcessedMessage() throws Exception {
        Order order = createOrder("m4-kafka-duplicate-order");
        MessageEnvelope<JsonNode> envelope = ackEnvelope(order.orderId().value(), "dedup-kafka-duplicate", "hash-kafka-duplicate");

        kafkaTemplate.send(MessagingTopics.BROKER_EVENT, order.orderId().value().toString(), envelope).get();
        kafkaTemplate.send(MessagingTopics.BROKER_EVENT, order.orderId().value().toString(), envelope).get();

        await(() -> orderEventCount(order.orderId().value(), "BrokerOrderAcknowledgedApplied") == 1);
        Thread.sleep(300);
        assertThat(processedCount(envelope.messageId())).isEqualTo(1);
        assertThat(orderEventCount(order.orderId().value(), "BrokerOrderAcknowledgedApplied")).isEqualTo(1);
    }

    @Test
    @DisplayName("schema parse 실패 broker event는 상태 변경 없이 parked_message에 격리된다")
    void brokerEventParseFailureIsParkedWithoutStateChange() throws Exception {
        kafkaTemplate.send(MessagingTopics.BROKER_EVENT, "bad-event", "{not-json").get();

        await(() -> parkedCount("SCHEMA_PARSE_FAILED") == 1);
        assertThat(parkedCount("SCHEMA_PARSE_FAILED")).isEqualTo(1);
    }

    private Order createOrder(String clientOrderId) {
        return orderApplicationService.createOrder(new PlaceOrderCommand(
                clientOrderId,
                new AccountId("ACC-M4-KAFKA"),
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

    private MessageEnvelope<JsonNode> ackEnvelope(UUID orderId, String dedupKey, String payloadHash) {
        return new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-broker-event-kafka-test",
                objectMapper.valueToTree(new BrokerOrderAcknowledgedPayload(
                        orderId,
                        dedupKey,
                        payloadHash,
                        "BRK-" + orderId.toString().substring(0, 8),
                        Instant.parse("2026-06-13T01:00:00Z")
                ))
        );
    }

    private long processedCount(UUID messageId) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM processed_message WHERE consumer_name = ? AND message_id = ?",
                Long.class,
                "order-service-broker-event-consumer",
                UuidBytes.toBytes(messageId)
        );
        return count == null ? 0 : count;
    }

    private long orderEventCount(UUID orderId, String eventType) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_event WHERE order_id = ? AND event_type = ?",
                Long.class,
                UuidBytes.toBytes(orderId),
                eventType
        );
        return count == null ? 0 : count;
    }

    private long parkedCount(String errorCode) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM parked_message WHERE error_code = ?",
                Long.class,
                errorCode
        );
        return count == null ? 0 : count;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("condition was not satisfied before timeout");
    }
}
