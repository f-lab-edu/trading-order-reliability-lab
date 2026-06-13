package com.trading.orderreliability.gateway.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;
import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.SubmitOrderCommandPayload;
import com.trading.orderreliability.gateway.config.GatewayBrokerProperties;
import com.trading.orderreliability.gateway.persistence.GatewayCommandAttemptRecord;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;
import com.trading.orderreliability.gateway.tcp.BrokerGatewayTcpClient;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;

import org.springframework.stereotype.Component;

@Component
public class BrokerCommandDispatcher {

    private final BrokerFrameCodec codec = new BrokerFrameCodec();
    private final GatewayJdbcRepository repository;
    private final BrokerGatewayTcpClient tcpClient;
    private final UuidV7Generator uuidGenerator;
    private final ObjectMapper objectMapper;
    private final GatewayBrokerProperties properties;
    private final Clock clock = Clock.systemUTC();

    public BrokerCommandDispatcher(
            GatewayJdbcRepository repository,
            BrokerGatewayTcpClient tcpClient,
            UuidV7Generator uuidGenerator,
            ObjectMapper objectMapper,
            GatewayBrokerProperties properties
    ) {
        this.repository = repository;
        this.tcpClient = tcpClient;
        this.uuidGenerator = uuidGenerator;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public void dispatchSubmit(GatewayCommandAttemptRecord attempt) {
        SubmitOrderCommandPayload payload = readPayload(attempt);
        Instant now = clock.instant();
        OrderRequest request = new OrderRequest(
                BrokerCommonHeader.of(
                        BrokerMessageId.ORDR,
                        attempt.wireMessageId(),
                        payload.orderId(),
                        attempt.traceId(),
                        now
                ),
                payload.accountId(),
                payload.market(),
                payload.symbol(),
                sideCode(payload.side()),
                orderTypeCode(payload.orderType()),
                payload.tif(),
                payload.orderQty(),
                new BigDecimal(payload.limitPrice())
        );
        byte[] frame = codec.encode(request);
        try {
            repository.insertJournal(
                    uuidGenerator.generate(),
                    attempt.brokerCode(),
                    "OUT",
                    BrokerMessageId.ORDR.code(),
                    attempt.wireMessageId(),
                    attempt.traceId(),
                    null,
                    payload.orderId(),
                    "PARSED",
                    null,
                    null,
                    frame,
                    request,
                    null,
                    now
            );
            tcpClient.send(frame);
        } catch (Exception e) {
            repository.markAttemptFailed(attempt.id(), "TCP_SEND_FAILED", e.getMessage(), clock.instant());
            throw e;
        }
        Instant sentAt = clock.instant();
        if (!repository.markAttemptSent(
                attempt.id(),
                sentAt,
                sentAt.plusMillis(properties.getCommandAckTimeoutMs())
        ) && !repository.attemptStateIs(attempt.id(), "ACKED")) {
            throw new IllegalStateException("Broker command attempt cannot be marked SENT after TCP send: "
                    + attempt.id());
        }
    }

    private SubmitOrderCommandPayload readPayload(GatewayCommandAttemptRecord attempt) {
        try {
            return objectMapper.readValue(attempt.payloadJson(), SubmitOrderCommandPayload.class);
        } catch (Exception e) {
            repository.markAttemptFailed(attempt.id(), "PAYLOAD_DESERIALIZE_FAILED", e.getMessage(), clock.instant());
            throw new IllegalStateException("Failed to deserialize broker command attempt payload: " + attempt.id(), e);
        }
    }

    private static String sideCode(String side) {
        return switch (side) {
            case "BUY" -> "B";
            case "SELL" -> "S";
            default -> side;
        };
    }

    private static String orderTypeCode(String orderType) {
        return "LIMIT".equals(orderType) ? "L" : orderType;
    }
}
