package com.trading.orderreliability.gateway.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;
import com.trading.orderreliability.broker.protocol.BrokerParseResult;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.SubmitOrderCommandPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.gateway.messaging.command.BrokerCommandService;
import com.trading.orderreliability.gateway.messaging.outbox.GatewayOutboxMessageRecord;
import com.trading.orderreliability.gateway.messaging.outbox.GatewayOutboxPublisher;
import com.trading.orderreliability.gateway.persistence.GatewayCommandAttemptRecord;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;
import com.trading.orderreliability.gateway.support.GatewayMySqlTestContainerSupport;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.jdbc.Sql;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "gateway.messaging.kafka.consumer-enabled=true",
        "gateway.messaging.outbox.enabled=false",
        "gateway.broker.command-dispatch-enabled=true",
        "gateway.broker.command-dispatch-initial-delay-ms=60000",
        "gateway.broker.command-dispatch-poll-delay-ms=60000"
})
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        topics = {MessagingTopics.BROKER_COMMAND, MessagingTopics.BROKER_EVENT},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@DisplayName("Broker Gateway command dispatch 통합 흐름")
class BrokerGatewayCommandDispatchIntegrationTest extends GatewayMySqlTestContainerSupport {

    private static final FakeBrokerServer BROKER_SERVER = FakeBrokerServer.start();

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private BrokerCommandDispatchScheduler dispatchScheduler;

    @Autowired
    private BrokerCommandService commandService;

    @Autowired
    private GatewayJdbcRepository repository;

    @Autowired
    private GatewayOutboxPublisher outboxPublisher;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @DynamicPropertySource
    static void registerBrokerProperties(DynamicPropertyRegistry registry) {
        registry.add("gateway.broker.port", BROKER_SERVER::port);
    }

    @AfterAll
    static void closeBrokerServer() throws IOException {
        BROKER_SERVER.close();
    }

    @Test
    @DisplayName("SubmitOrderCommand는 Kafka consume 후 TCP ORDR와 ACKN broker event outbox로 이어진다")
    @Sql(statements = {
            "DELETE FROM outbox_message",
            "DELETE FROM broker_message_journal",
            "DELETE FROM broker_command_attempt",
            "DELETE FROM broker_order_binding",
            "DELETE FROM processed_message",
            "DELETE FROM parked_message"
    })
    void submitOrderCommandReachesTcpAndCreatesAcknowledgedBrokerEventOutbox() throws Exception {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope<JsonNode> command = submitEnvelope(orderId);
        kafkaTemplate.send(MessagingTopics.BROKER_COMMAND, orderId.toString(), command).get(10, TimeUnit.SECONDS);
        awaitCreatedAttempt(orderId);

        dispatchScheduler.dispatchCreatedAttempts();

        OrderRequest request = BROKER_SERVER.awaitOrderRequest();
        assertThat(request.header().messageId()).isEqualTo(BrokerMessageId.ORDR);
        assertThat(request.header().orderId()).isEqualTo(orderId);
        assertThat(request.header().traceId()).isEqualTo("trace-gateway-dispatch-smoke");
        assertThat(request.side()).isEqualTo("B");
        assertThat(request.orderType()).isEqualTo("L");

        GatewayOutboxMessageRecord outbox = awaitOutbox(orderId);
        assertThat(repository.countJournalByOrderId(orderId)).isEqualTo(2);

        assertThat(outboxPublisher.publishAvailable()).isGreaterThanOrEqualTo(1);
        try (Consumer<String, String> consumer = createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, MessagingTopics.BROKER_EVENT);
            JsonNode envelope = findEnvelope(consumer, outbox.id());
            assertThat(envelope.path("messageType").asText()).isEqualTo(MessageTypes.BROKER_ORDER_ACKNOWLEDGED);
            assertThat(envelope.path("traceId").asText()).isEqualTo("trace-gateway-dispatch-smoke");
            assertThat(envelope.path("payload").path("brokerOrderId").asText()).isEqualTo("BRK-SMOKE-ACK-001");
        }
    }

    @Test
    @DisplayName("ack deadline이 지난 submit attempt는 재전송하지 않고 UNKNOWN과 parking으로 격리한다")
    @Sql(statements = {
            "DELETE FROM outbox_message",
            "DELETE FROM broker_message_journal",
            "DELETE FROM broker_command_attempt",
            "DELETE FROM broker_order_binding",
            "DELETE FROM processed_message",
            "DELETE FROM parked_message"
    })
    void expiredSubmitAckDeadlineIsParkedAsUnknownWithoutResend() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId));
        assertThat(repository.claimCreatedSubmitAttempts(
                10,
                Instant.parse("2026-06-13T00:00:00Z"),
                Instant.parse("2026-06-13T00:00:01Z")
        )).hasSize(1);

        dispatchScheduler.dispatchCreatedAttempts();

        assertThat(repository.countAttemptsByOrderIdAndState(orderId, "UNKNOWN")).isEqualTo(1);
        assertThat(repository.countParkedByErrorCode("SUBMIT_OUTCOME_UNKNOWN")).isEqualTo(1);
    }

    @Test
    @DisplayName("claim된 CREATED submit attempt는 ack deadline 전까지 dispatch 후보로 다시 조회되지 않는다")
    @Sql(statements = {
            "DELETE FROM outbox_message",
            "DELETE FROM broker_message_journal",
            "DELETE FROM broker_command_attempt",
            "DELETE FROM broker_order_binding",
            "DELETE FROM processed_message",
            "DELETE FROM parked_message"
    })
    void claimedCreatedAttemptIsHiddenFromDispatchUntilAckDeadlineExpires() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId));

        assertThat(repository.claimCreatedSubmitAttempts(
                10,
                Instant.parse("2026-06-13T00:00:00Z"),
                Instant.parse("2099-01-01T00:00:00Z")
        )).hasSize(1);

        assertThat(repository.findCreatedSubmitAttempts(10))
                .noneMatch(attempt -> attempt.orderId().equals(orderId));
        assertThat(repository.findExpiredSubmitOutcomeAttempts(
                Instant.parse("2026-06-13T00:00:01Z"),
                10
        )).noneMatch(attempt -> attempt.orderId().equals(orderId));
    }

    @Test
    @DisplayName("SENT submit attempt는 송신 시각 기준 ack deadline으로 갱신되고 만료 시 UNKNOWN으로 격리된다")
    @Sql(statements = {
            "DELETE FROM outbox_message",
            "DELETE FROM broker_message_journal",
            "DELETE FROM broker_command_attempt",
            "DELETE FROM broker_order_binding",
            "DELETE FROM processed_message",
            "DELETE FROM parked_message"
    })
    void sentAttemptAckDeadlineIsRefreshedFromSentAtAndExpiresAsUnknown() {
        UUID orderId = UUID.randomUUID();
        commandService.handle(submitEnvelope(orderId));
        GatewayCommandAttemptRecord attempt = repository.claimCreatedSubmitAttempts(
                10,
                Instant.parse("2026-06-13T00:00:00Z"),
                Instant.parse("2026-06-13T00:00:01Z")
        ).getFirst();

        Instant sentAt = Instant.parse("2026-06-13T00:00:10Z");
        Instant ackDeadlineAt = sentAt.plusSeconds(30);
        assertThat(repository.markAttemptSent(attempt.id(), sentAt, ackDeadlineAt)).isTrue();
        assertThat(repository.findAttemptAckDeadline(attempt.id())).contains(ackDeadlineAt);

        dispatchScheduler.dispatchCreatedAttempts();

        assertThat(repository.countAttemptsByOrderIdAndState(orderId, "UNKNOWN")).isEqualTo(1);
        assertThat(repository.countParkedByErrorCode("SUBMIT_OUTCOME_UNKNOWN")).isEqualTo(1);
    }

    private void awaitCreatedAttempt(UUID orderId) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (repository.findCreatedSubmitAttempts(10).stream().anyMatch(attempt -> attempt.orderId().equals(orderId))) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("CREATED submit attempt not found for " + orderId);
    }

    private GatewayOutboxMessageRecord awaitOutbox(UUID orderId) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            var outbox = repository.findOutboxByAggregateIdAndMessageType(
                    orderId,
                    MessageTypes.BROKER_ORDER_ACKNOWLEDGED
            );
            if (outbox.isPresent()) {
                return outbox.get();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("BrokerOrderAcknowledged outbox not found for " + orderId);
    }

    private Consumer<String, String> createConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafkaBroker, "gateway-command-dispatch-test", false);
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
        throw new AssertionError("Published broker event envelope not found for messageId " + messageId);
    }

    private MessageEnvelope<JsonNode> submitEnvelope(UUID orderId) {
        return new MessageEnvelope<>(
                UUID.randomUUID(),
                MessageTypes.SUBMIT_ORDER_COMMAND,
                orderId.toString(),
                Instant.parse("2026-06-13T01:00:00Z"),
                "trace-gateway-dispatch-smoke",
                objectMapper.valueToTree(new SubmitOrderCommandPayload(
                        orderId,
                        "ACC-GW-SMOKE",
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

    private static final class FakeBrokerServer implements Closeable {

        private final BrokerFrameCodec codec = new BrokerFrameCodec();
        private final ServerSocket serverSocket;
        private final CountDownLatch requestReceived = new CountDownLatch(1);
        private final AtomicReference<OrderRequest> orderRequest = new AtomicReference<>();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final Thread thread;
        private volatile Socket clientSocket;

        private FakeBrokerServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
            this.thread = new Thread(this::run, "gateway-command-dispatch-fake-broker-server");
            this.thread.setDaemon(true);
        }

        static FakeBrokerServer start() {
            try {
                FakeBrokerServer server = new FakeBrokerServer(new ServerSocket(0));
                server.thread.start();
                return server;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to start fake broker server", e);
            }
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        OrderRequest awaitOrderRequest() throws Exception {
            if (!requestReceived.await(10, TimeUnit.SECONDS)) {
                Exception error = failure.get();
                if (error != null) {
                    throw error;
                }
                throw new AssertionError("ORDR frame not received by fake broker");
            }
            return orderRequest.get();
        }

        private void run() {
            try (Socket socket = serverSocket.accept()) {
                clientSocket = socket;
                byte[] frame = readFrame(socket);
                BrokerParseResult parseResult = codec.decode(frame);
                if (!(parseResult instanceof BrokerParseResult.Success success)
                        || !(success.message() instanceof OrderRequest request)) {
                    throw new IllegalStateException("Expected ORDR frame but received " + parseResult);
                }
                orderRequest.set(request);
                socket.getOutputStream().write(codec.encode(ack(request)));
                socket.getOutputStream().flush();
                requestReceived.countDown();
                Thread.sleep(250);
            } catch (Exception e) {
                failure.set(e);
                requestReceived.countDown();
            }
        }

        private OrderAccepted ack(OrderRequest request) {
            return new OrderAccepted(
                    BrokerCommonHeader.of(
                            BrokerMessageId.ACKN,
                            request.header().wireMessageId(),
                            request.header().orderId(),
                            request.header().traceId(),
                            Instant.parse("2026-06-13T01:01:00Z")
                    ),
                    "BRK-SMOKE-ACK-001",
                    Instant.parse("2026-06-13T01:01:00Z")
            );
        }

        private static byte[] readFrame(Socket socket) throws IOException {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            byte[] lengthHeader = input.readNBytes(8);
            int payloadLength = Integer.parseInt(new String(lengthHeader, StandardCharsets.US_ASCII));
            byte[] payload = input.readNBytes(payloadLength);
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            frame.write(lengthHeader);
            frame.write(payload);
            return frame.toByteArray();
        }

        @Override
        public void close() throws IOException {
            if (clientSocket != null) {
                clientSocket.close();
            }
            serverSocket.close();
        }
    }
}
