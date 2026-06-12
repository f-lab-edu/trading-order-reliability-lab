package com.trading.orderreliability.order.adapter.out.messaging.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.common.messaging.OutboxStatus;
import com.trading.orderreliability.order.application.OrderApplicationService;
import com.trading.orderreliability.order.application.command.PlaceOrderCommand;
import com.trading.orderreliability.order.application.result.PlaceOrderResult;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;
import com.trading.orderreliability.order.support.MySqlTestContainerSupport;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "order-service.messaging.outbox.enabled=false",
        "order-service.messaging.kafka.topic-bootstrap-enabled=false"
})
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = MessagingTopics.BROKER_COMMAND, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OutboxPublisherIntegrationTest extends MySqlTestContainerSupport {

    @Autowired
    private OrderApplicationService orderApplicationService;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private OutboxMessageRepository outboxMessageRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void publisherSendsEnvelopeToKafkaAndMarksOutboxSent() throws Exception {
        PlaceOrderResult result = orderApplicationService.createOrder(placeCommand("m2-publisher-order-001", "trace-m2-publisher-001"));

        int published = outboxPublisher.publishAvailable();

        assertThat(published).isGreaterThanOrEqualTo(1);
        OutboxMessageRecord message = outboxMessageRepository.findByAggregateId(result.order().orderId().value()).getFirst();
        assertThat(message.status()).isEqualTo(OutboxStatus.SENT.name());
        assertThat(message.publishedAt()).isNotNull();

        try (Consumer<String, String> consumer = createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, MessagingTopics.BROKER_COMMAND);
            JsonNode envelope = findEnvelope(consumer, message.id());
            assertThat(envelope.path("messageId").asText()).isEqualTo(message.id().toString());
            assertThat(envelope.path("messageType").asText()).isEqualTo(MessageTypes.SUBMIT_ORDER_COMMAND);
            assertThat(envelope.path("messageKey").asText()).isEqualTo(result.order().orderId().value().toString());
            assertThat(envelope.path("traceId").asText()).isEqualTo("trace-m2-publisher-001");
            assertThat(envelope.path("payload").path("orderId").asText()).isEqualTo(result.order().orderId().value().toString());
        }
    }

    @Test
    void publisherFailureMarksOutboxFailedWithRetryMetadata() {
        PlaceOrderResult result = orderApplicationService.createOrder(placeCommand("m2-publisher-order-002", "trace-m2-publisher-002"));
        OutboxPublisherProperties properties = new OutboxPublisherProperties();
        properties.setBatchSize(10);
        properties.setLockTtl(Duration.ofSeconds(30));
        properties.setInitialRetryDelay(Duration.ofSeconds(2));
        OutboxPublisher failingPublisher = new OutboxPublisher(
                outboxMessageRepository,
                objectMapper,
                (topicName, messageKey, envelope) -> {
                    throw new IllegalStateException("broker unavailable");
                },
                properties
        );

        int published = failingPublisher.publishAvailable();

        assertThat(published).isGreaterThanOrEqualTo(1);
        OutboxMessageRecord message = outboxMessageRepository.findByAggregateId(result.order().orderId().value()).getFirst();
        assertThat(message.status()).isEqualTo(OutboxStatus.FAILED.name());
        assertThat(message.retryCount()).isEqualTo(1);
        assertThat(message.nextRetryAt()).isNotNull();
        assertThat(message.lastError()).contains("broker unavailable");
    }

    @Test
    void failedOutboxMessageStopsBeingPublishableWhenMaxRetryCountIsReached() {
        PlaceOrderResult result = orderApplicationService.createOrder(placeCommand("m2-publisher-order-003", "trace-m2-publisher-003"));
        OutboxPublisherProperties properties = new OutboxPublisherProperties();
        properties.setBatchSize(10);
        properties.setLockTtl(Duration.ofSeconds(30));
        properties.setInitialRetryDelay(Duration.ZERO);
        properties.setMaxRetryCount(1);
        OutboxPublisher failingPublisher = new OutboxPublisher(
                outboxMessageRepository,
                objectMapper,
                (topicName, messageKey, envelope) -> {
                    throw new IllegalStateException("broker unavailable");
                },
                properties
        );

        int firstAttempt = failingPublisher.publishAvailable();
        int secondAttempt = failingPublisher.publishAvailable();

        assertThat(firstAttempt).isGreaterThanOrEqualTo(1);
        assertThat(secondAttempt).isEqualTo(0);
        OutboxMessageRecord message = outboxMessageRepository.findByAggregateId(result.order().orderId().value()).getFirst();
        assertThat(message.status()).isEqualTo(OutboxStatus.FAILED.name());
        assertThat(message.retryCount()).isEqualTo(1);
    }

    @Test
    void lateFailureFromAnotherPublisherDoesNotOverwriteSentStatus() {
        PlaceOrderResult result = orderApplicationService.createOrder(placeCommand("m2-publisher-order-004", "trace-m2-publisher-004"));
        Instant now = Instant.now();
        List<OutboxMessageRecord> claimed = outboxMessageRepository.claimPublishable(
                now,
                "publisher-a",
                now.plusSeconds(30),
                10,
                12
        );
        OutboxMessageRecord message = claimed.stream()
                .filter(record -> record.aggregateId().equals(result.order().orderId().value()))
                .findFirst()
                .orElseThrow();

        boolean sent = outboxMessageRepository.markSent(message.id(), "publisher-a", now.plusMillis(100));
        boolean failed = outboxMessageRepository.markFailed(
                message.id(),
                "publisher-b",
                1,
                now.plusSeconds(1),
                "late failure"
        );

        assertThat(sent).isTrue();
        assertThat(failed).isFalse();
        OutboxMessageRecord persisted = outboxMessageRepository.findById(message.id()).orElseThrow();
        assertThat(persisted.status()).isEqualTo(OutboxStatus.SENT.name());
        assertThat(persisted.lastError()).isNull();
    }

    private Consumer<String, String> createConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("m2-publisher-test", "false", embeddedKafkaBroker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
    }

    private JsonNode findEnvelope(Consumer<String, String> consumer, UUID messageId) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                JsonNode envelope = objectMapper.readTree(record.value());
                if (messageId.toString().equals(envelope.path("messageId").asText())) {
                    return envelope;
                }
            }
        }
        throw new AssertionError("Published envelope not found for messageId " + messageId);
    }

    private static PlaceOrderCommand placeCommand(String clientOrderId, String traceId) {
        return new PlaceOrderCommand(
                clientOrderId,
                new AccountId("ACC-M2-PUB"),
                Market.US,
                new Symbol("AAPL"),
                OrderSide.BUY,
                OrderType.LIMIT,
                TimeInForce.DAY,
                OrderQuantity.positive(100),
                new OrderPrice(new BigDecimal("189.50")),
                traceId
        );
    }
}
