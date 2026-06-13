package com.trading.orderreliability.simulator;

import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessage;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.Fill;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRejected;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;
import com.trading.orderreliability.broker.protocol.BrokerMessages.StatusQuery;
import com.trading.orderreliability.broker.protocol.BrokerMessages.StatusSnapshot;
import com.trading.orderreliability.broker.protocol.BrokerParseResult;
import com.trading.orderreliability.simulator.domain.SimulatorScenario;
import com.trading.orderreliability.simulator.tcp.BrokerSimulatorTcpServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.Socket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@DisplayName("브로커 시뮬레이터 TCP 통합 흐름")
class BrokerSimulatorTcpIntegrationTest {

    private static final UUID ORDER_ID = UUID.fromString("018f8b7a-4c4e-7b20-9f0e-9dfeb33e92aa");
    private static final UUID JOB_ID = UUID.fromString("018f8b7a-4c4e-7b20-9f0e-9dfeb33e92bb");
    private static final UUID ATTEMPT_ID = UUID.fromString("018f8b7a-4c4e-7b20-9f0e-9dfeb33e92cc");
    private static final Instant REQUEST_TIME = Instant.parse("2026-05-13T01:15:30.123Z");

    private final BrokerFrameCodec codec = new BrokerFrameCodec();
    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Autowired
    private BrokerSimulatorTcpServer tcpServer;

    @Value("${local.server.port}")
    private int httpPort;

    @BeforeEach
    void reset() throws Exception {
        post("/api/simulator/reset");
    }

    @Test
    @DisplayName("ACK_SUCCESS 시나리오는 주문 요청에 ACK를 반환하고 상태조회에 ACCEPTED snapshot을 반환한다")
    void orderRequestReturnsAckAndStatusQueryReturnsSnapshot() throws Exception {
        putScenario(SimulatorScenario.ACK_SUCCESS);

        try (Socket socket = connect()) {
            send(socket, orderRequest("W-GW-ORDER-ACK-001"));

            BrokerMessage ack = receive(socket);
            assertThat(ack).isInstanceOf(OrderAccepted.class);
            OrderAccepted accepted = (OrderAccepted) ack;
            assertThat(accepted.header().wireMessageId()).isEqualTo("W-GW-ORDER-ACK-001");
            assertThat(accepted.brokerOrderId()).startsWith("BRK-SIM-");

            send(socket, statusQuery("W-GW-STATUS-001", ""));

            BrokerMessage snapshot = receive(socket);
            assertThat(snapshot).isInstanceOf(StatusSnapshot.class);
            StatusSnapshot statusSnapshot = (StatusSnapshot) snapshot;
            assertThat(statusSnapshot.header().wireMessageId()).isEqualTo("W-GW-STATUS-001");
            assertThat(statusSnapshot.snapshotStatus()).isEqualTo("ACCEPTED");
            assertThat(statusSnapshot.leavesQty()).isEqualTo(100);
        }
    }

    @Test
    @DisplayName("REJECT_SUCCESS 시나리오는 주문 요청에 RJCT 거절 응답을 반환한다")
    void orderRequestReturnsRejectWhenScenarioIsRejectSuccess() throws Exception {
        putScenario(SimulatorScenario.REJECT_SUCCESS);

        try (Socket socket = connect()) {
            send(socket, orderRequest("W-GW-ORDER-RJCT-001"));

            BrokerMessage rejected = receive(socket);

            assertThat(rejected).isInstanceOf(OrderRejected.class);
            OrderRejected orderRejected = (OrderRejected) rejected;
            assertThat(orderRejected.header().wireMessageId()).isEqualTo("W-GW-ORDER-RJCT-001");
            assertThat(orderRejected.rejectCode()).isEqualTo("MARKET_CLOSED");
        }
    }

    @Test
    @DisplayName("중복 체결 주입은 같은 wireMessageId와 원 주문 traceId를 재사용한다")
    void duplicateFillUsesSameWireMessageId() throws Exception {
        putScenario(SimulatorScenario.ACK_SUCCESS);

        try (Socket socket = connect()) {
            send(socket, orderRequest("W-GW-ORDER-DUP-001"));
            assertThat(receive(socket)).isInstanceOf(OrderAccepted.class);

            post("/api/simulator/orders/%s/duplicate-fill".formatted(ORDER_ID));

            BrokerMessage first = receive(socket);
            BrokerMessage second = receive(socket);

            assertThat(first).isInstanceOf(Fill.class);
            assertThat(second).isInstanceOf(Fill.class);
            assertThat(first.header().wireMessageId()).isEqualTo(second.header().wireMessageId());
            assertThat(first.header().traceId()).isEqualTo("trace-simulator-test");
            assertThat(second.header().traceId()).isEqualTo("trace-simulator-test");
        }
    }

    @Test
    @DisplayName("잘못된 길이 헤더를 받으면 주문 상태를 바꾸지 않고 연결을 닫는다")
    void malformedLengthHeaderClosesConnection() throws Exception {
        try (Socket socket = connect()) {
            sendRaw(socket, "XXXXXXXX".getBytes(StandardCharsets.US_ASCII));

            assertConnectionClosed(socket);
        }
    }

    @Test
    @DisplayName("body malformed 전문을 받으면 주문 상태를 바꾸지 않고 연결을 닫는다")
    void bodyMalformedClosesConnection() throws Exception {
        byte[] frame = codec.encode(orderRequest("W-GW-MALFORMED-BODY"));
        int marketOffset = 8 + 192 + 32;
        frame[marketOffset] = 'K';
        frame[marketOffset + 1] = 'R';

        try (Socket socket = connect()) {
            sendRaw(socket, frame);

            assertConnectionClosed(socket);
        }
    }

    private void putScenario(SimulatorScenario scenario) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(adminUri("/api/simulator/scenario"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"scenario\":\"%s\"}".formatted(scenario.name())))
                .build();
        assertThat(httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(200);
    }

    private void post(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(adminUri(path))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        int statusCode = httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        assertThat(statusCode).isBetween(200, 299);
    }

    private URI adminUri(String path) {
        return URI.create("http://127.0.0.1:%d%s".formatted(httpPort, path));
    }

    private Socket connect() throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", tcpServer.port()), 2_000);
        socket.setSoTimeout(3_000);
        return socket;
    }

    private void send(Socket socket, BrokerMessage message) throws IOException {
        sendRaw(socket, codec.encode(message));
    }

    private void sendRaw(Socket socket, byte[] frame) throws IOException {
        socket.getOutputStream().write(frame);
        socket.getOutputStream().flush();
    }

    private void assertConnectionClosed(Socket socket) throws IOException {
        assertThat(socket.getInputStream().read()).isEqualTo(-1);
    }

    private BrokerMessage receive(Socket socket) throws IOException {
        InputStream inputStream = socket.getInputStream();
        byte[] lengthHeader = readExactly(inputStream, 8);
        int frameLength = Integer.parseInt(new String(lengthHeader, StandardCharsets.US_ASCII));
        byte[] payload = readExactly(inputStream, frameLength);
        byte[] frame = new byte[8 + frameLength];
        System.arraycopy(lengthHeader, 0, frame, 0, lengthHeader.length);
        System.arraycopy(payload, 0, frame, 8, payload.length);

        BrokerParseResult parseResult = codec.decode(frame);
        assertThat(parseResult).isInstanceOf(BrokerParseResult.Success.class);
        return ((BrokerParseResult.Success) parseResult).message();
    }

    private byte[] readExactly(InputStream inputStream, int length) throws IOException {
        byte[] bytes = new byte[length];
        int offset = 0;
        while (offset < length) {
            int read = inputStream.read(bytes, offset, length - offset);
            if (read < 0) {
                throw new IOException("socket closed before frame was fully read");
            }
            offset += read;
        }
        return bytes;
    }

    private OrderRequest orderRequest(String wireMessageId) {
        return new OrderRequest(
                header(BrokerMessageId.ORDR, wireMessageId),
                "ACC-001",
                "US",
                "AAPL",
                "B",
                "L",
                "DAY",
                100,
                new BigDecimal("189.5000")
        );
    }

    private StatusQuery statusQuery(String wireMessageId, String brokerOrderId) {
        return new StatusQuery(
                header(BrokerMessageId.OSTQ, wireMessageId),
                JOB_ID,
                ATTEMPT_ID,
                brokerOrderId,
                "MANUAL"
        );
    }

    private BrokerCommonHeader header(BrokerMessageId messageId, String wireMessageId) {
        return BrokerCommonHeader.of(messageId, wireMessageId, ORDER_ID, "trace-simulator-test", REQUEST_TIME);
    }
}
