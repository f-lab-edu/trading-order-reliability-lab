package com.trading.orderreliability.gateway.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;
import com.trading.orderreliability.broker.protocol.BrokerParseResult;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.gateway.BrokerGatewayServiceApplication;
import com.trading.orderreliability.gateway.messaging.outbox.GatewayOutboxPublisher;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;
import com.trading.orderreliability.order.OrderServiceApplication;
import com.trading.orderreliability.order.adapter.out.messaging.outbox.OutboxPublisher;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.flywaydb.core.Flyway;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("정상 주문 end-to-end 통합 흐름")
class SubmitOrderEndToEndSmokeIntegrationTest {

    private static final MySQLContainer<?> ORDER_MYSQL = mysql("submit_order_e2e_order_db");
    private static final MySQLContainer<?> GATEWAY_MYSQL = mysql("submit_order_e2e_gateway_db");
    private static final EmbeddedKafkaBroker KAFKA = new EmbeddedKafkaKraftBroker(
            1,
            1,
            MessagingTopics.BROKER_COMMAND,
            MessagingTopics.BROKER_EVENT
    );
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();

    static {
        ORDER_MYSQL.start();
        GATEWAY_MYSQL.start();
        migrate(ORDER_MYSQL, "order-service");
        migrate(GATEWAY_MYSQL, "broker-gateway-service");
        KAFKA.afterPropertiesSet();
    }

    @AfterAll
    static void stopInfrastructure() {
        KAFKA.destroy();
        GATEWAY_MYSQL.stop();
        ORDER_MYSQL.stop();
    }

    @Test
    @DisplayName("HTTP 주문 생성은 Gateway ORDR와 ACKN broker event를 거쳐 LIVE 조회로 수렴한다")
    void postOrderReachesGatewayAndReturnsLiveAfterBrokerAck() throws Exception {
        try (FakeBrokerServer brokerServer = FakeBrokerServer.start();
             ConfigurableApplicationContext orderContext = startOrderService();
             ConfigurableApplicationContext gatewayContext = startGatewayService(brokerServer.port())) {
            waitForAssignments(orderContext);
            waitForAssignments(gatewayContext);

            UUID orderId = createOrder(orderContext);
            assertThat(orderStatus(orderContext, orderId)).isEqualTo("PENDING_ACK");

            assertThat(orderContext.getBean(OutboxPublisher.class).publishAvailable()).isGreaterThanOrEqualTo(1);
            GatewayJdbcRepository gatewayRepository = gatewayContext.getBean(GatewayJdbcRepository.class);
            await(() -> gatewayRepository.findCreatedSubmitAttempts(10)
                    .stream()
                    .anyMatch(attempt -> attempt.orderId().equals(orderId)));

            gatewayContext.getBean(BrokerCommandDispatchScheduler.class).dispatchCreatedAttempts();
            OrderRequest request = brokerServer.awaitOrderRequest();
            assertThat(request.header().orderId()).isEqualTo(orderId);
            assertThat(request.header().traceId()).isEqualTo("trace-submit-order-e2e");
            assertThat(request.side()).isEqualTo("B");

            await(() -> gatewayRepository.findOutboxByAggregateIdAndMessageType(
                    orderId,
                    MessageTypes.BROKER_ORDER_ACKNOWLEDGED
            ).isPresent());
            assertThat(gatewayRepository.countJournalByOrderId(orderId)).isEqualTo(2);
            assertThat(gatewayContext.getBean(GatewayOutboxPublisher.class).publishAvailable()).isGreaterThanOrEqualTo(1);

            await(() -> "LIVE".equals(orderStatus(orderContext, orderId)));
            assertThat(orderStatus(orderContext, orderId)).isEqualTo("LIVE");
            assertThat(orderEventCount(orderContext, orderId, "BrokerOrderAcknowledgedApplied")).isEqualTo(1);
        }
    }

    private static ConfigurableApplicationContext startOrderService() {
        return new SpringApplicationBuilder(OrderServiceApplication.class)
                .web(WebApplicationType.SERVLET)
                .initializers(overrideProperties(ORDER_MYSQL, Map.of(
                        "server.port", 0,
                        "spring.application.name", "order-service-submit-order-e2e",
                        "spring.kafka.bootstrap-servers", KAFKA.getBrokersAsString(),
                        "spring.kafka.consumer.group-id", "order-service-submit-order-e2e-" + UUID.randomUUID(),
                        "order-service.messaging.kafka.broker-event-consumer-enabled", true,
                        "order-service.messaging.kafka.topic-bootstrap-enabled", false,
                        "order-service.messaging.outbox.enabled", false,
                        "gateway.messaging.kafka.consumer-enabled", false
                )))
                .run();
    }

    private static ConfigurableApplicationContext startGatewayService(int brokerPort) {
        return new SpringApplicationBuilder(BrokerGatewayServiceApplication.class)
                .web(WebApplicationType.SERVLET)
                .initializers(overrideProperties(GATEWAY_MYSQL, Map.ofEntries(
                        Map.entry("server.port", 0),
                        Map.entry("spring.application.name", "broker-gateway-service-submit-order-e2e"),
                        Map.entry("spring.kafka.bootstrap-servers", KAFKA.getBrokersAsString()),
                        Map.entry("spring.kafka.consumer.group-id", "broker-gateway-service-submit-order-e2e-" + UUID.randomUUID()),
                        Map.entry("gateway.messaging.kafka.consumer-enabled", true),
                        Map.entry("gateway.messaging.outbox.enabled", false),
                        Map.entry("gateway.broker.command-dispatch-enabled", true),
                        Map.entry("gateway.broker.command-dispatch-initial-delay-ms", 60000),
                        Map.entry("gateway.broker.command-dispatch-poll-delay-ms", 60000),
                        Map.entry("gateway.broker.port", brokerPort),
                        Map.entry("order-service.messaging.kafka.broker-event-consumer-enabled", false)
                )))
                .run();
    }

    private static ApplicationContextInitializer<ConfigurableApplicationContext> overrideProperties(
            MySQLContainer<?> mysql,
            Map<String, Object> serviceProperties
    ) {
        Map<String, Object> properties = new LinkedHashMap<>(commonProperties(mysql));
        properties.putAll(serviceProperties);
        return context -> context.getEnvironment()
                .getPropertySources()
                .addFirst(new MapPropertySource("submit-order-e2e-overrides", properties));
    }

    private static Map<String, Object> commonProperties(MySQLContainer<?> mysql) {
        return Map.of(
                "spring.datasource.url", mysql.getJdbcUrl(),
                "spring.datasource.username", mysql.getUsername(),
                "spring.datasource.password", mysql.getPassword(),
                "spring.datasource.driver-class-name", mysql.getDriverClassName(),
                "spring.flyway.enabled", false
        );
    }

    private static void migrate(MySQLContainer<?> mysql, String moduleName) {
        Flyway.configure()
                .dataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
                .locations("filesystem:" + migrationDirectory(moduleName))
                .load()
                .migrate();
    }

    private static Path migrationDirectory(String moduleName) {
        Path current = Path.of("").toAbsolutePath();
        while (current != null && !Files.exists(current.resolve("settings.gradle.kts"))) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IllegalStateException("Project root could not be resolved");
        }
        Path migrationDirectory = current.resolve("apps").resolve(moduleName).resolve("src/main/resources/db/migration");
        if (!Files.isDirectory(migrationDirectory)) {
            throw new IllegalStateException("Migration directory does not exist: " + migrationDirectory);
        }
        return migrationDirectory;
    }

    private static UUID createOrder(ConfigurableApplicationContext orderContext) throws Exception {
        int port = orderContext.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
        HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:%d/api/orders".formatted(port))
                .toURL()
                .openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("X-Trace-Id", "trace-submit-order-e2e");
        connection.setDoOutput(true);
        String body = """
                {
                  "clientOrderId": "submit-order-e2e-order",
                  "accountId": "ACC-SUBMIT-E2E",
                  "market": "US",
                  "symbol": "AAPL",
                  "side": "BUY",
                  "orderType": "LIMIT",
                  "tif": "DAY",
                  "orderQty": 100,
                  "limitPrice": 189.50
                }
                """;
        try (OutputStream output = connection.getOutputStream()) {
            output.write(body.getBytes(StandardCharsets.UTF_8));
        }
        assertThat(connection.getResponseCode()).isEqualTo(201);
        JsonNode response = OBJECT_MAPPER.readTree(connection.getInputStream());
        return UUID.fromString(response.path("orderId").asText());
    }

    private static String orderStatus(ConfigurableApplicationContext orderContext, UUID orderId) throws Exception {
        int port = orderContext.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
        HttpURLConnection connection = (HttpURLConnection) URI.create("http://127.0.0.1:%d/api/orders/%s".formatted(port, orderId))
                .toURL()
                .openConnection();
        connection.setRequestMethod("GET");
        assertThat(connection.getResponseCode()).isEqualTo(200);
        return OBJECT_MAPPER.readTree(connection.getInputStream()).path("status").asText();
    }

    private static long orderEventCount(ConfigurableApplicationContext orderContext, UUID orderId, String eventType) {
        Long count = orderContext.getBean(JdbcTemplate.class).queryForObject(
                "SELECT COUNT(*) FROM order_event WHERE order_id = ? AND event_type = ?",
                Long.class,
                com.trading.orderreliability.common.id.UuidBytes.toBytes(orderId),
                eventType
        );
        return count == null ? 0 : count;
    }

    private static void waitForAssignments(ConfigurableApplicationContext context) {
        KafkaListenerEndpointRegistry registry = context.getBean(KafkaListenerEndpointRegistry.class);
        registry.getListenerContainers()
                .forEach(container -> ContainerTestUtils.waitForAssignment(container, 1));
    }

    private static void await(CheckedBooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("condition was not satisfied before timeout");
    }

    @FunctionalInterface
    private interface CheckedBooleanSupplier {

        boolean getAsBoolean() throws Exception;
    }

    private static MySQLContainer<?> mysql(String databaseName) {
        return new MySQLContainer<>(DockerImageName.parse("mysql:8.4"))
                .withDatabaseName(databaseName)
                .withUsername("trading_app")
                .withPassword("trading_app");
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
            this.thread = new Thread(this::run, "submit-order-e2e-fake-broker-server");
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
                    "BRK-SUBMIT-E2E-ACK",
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
