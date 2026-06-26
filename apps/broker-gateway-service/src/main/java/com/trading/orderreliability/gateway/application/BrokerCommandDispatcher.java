package com.trading.orderreliability.gateway.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMessageId;
import com.trading.orderreliability.broker.protocol.BrokerMessages.CancelRequest;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;
import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.CancelOrderCommandPayload;
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
        if (!repository.insertOutboundJournalIfDispatchTokenMatches(
                attempt.id(),
                attempt.dispatchToken(),
                uuidGenerator.generate(),
                attempt.brokerCode(),
                BrokerMessageId.ORDR.code(),
                attempt.wireMessageId(),
                attempt.traceId(),
                null,
                payload.orderId(),
                frame,
                request,
                now
        )) {
            throw new IllegalStateException("Broker submit attempt dispatch token is no longer current: "
                    + attempt.id());
        }
        ensureDispatchTokenIsCurrent(attempt, "submit", clock.instant());
        try {
            tcpClient.send(frame);
        } catch (Exception e) {
            throw e;
        }
        Instant sentAt = clock.instant();
        if (!repository.markAttemptSent(
                attempt.id(),
                attempt.dispatchToken(),
                sentAt,
                sentAt.plusMillis(properties.getCommandAckTimeoutMs())
        ) && !repository.attemptStateIs(attempt.id(), "ACKED")) {
            throw new IllegalStateException("Broker command attempt cannot be marked SENT after TCP send: "
                    + attempt.id());
        }
    }

    public void dispatchCancel(GatewayCommandAttemptRecord attempt) {
        CancelOrderCommandPayload payload = readCancelPayload(attempt);
        Instant now = clock.instant();
        CancelRequest request = new CancelRequest(
                BrokerCommonHeader.of(
                        BrokerMessageId.CXLQ,
                        attempt.wireMessageId(),
                        payload.orderId(),
                        attempt.traceId(),
                        now
                ),
                attempt.brokerOrderId()
        );
        byte[] frame = codec.encode(request);
        if (!repository.insertOutboundJournalIfDispatchTokenMatches(
                attempt.id(),
                attempt.dispatchToken(),
                uuidGenerator.generate(),
                attempt.brokerCode(),
                BrokerMessageId.CXLQ.code(),
                attempt.wireMessageId(),
                attempt.traceId(),
                attempt.brokerOrderId(),
                payload.orderId(),
                frame,
                request,
                now
        )) {
            throw new IllegalStateException("Broker cancel attempt dispatch token is no longer current: "
                    + attempt.id());
        }
        ensureDispatchTokenIsCurrent(attempt, "cancel", clock.instant());
        try {
            tcpClient.send(frame);
        } catch (Exception e) {
            throw e;
        }
        Instant sentAt = clock.instant();
        if (!repository.markAttemptSent(
                attempt.id(),
                attempt.dispatchToken(),
                sentAt,
                sentAt.plusMillis(properties.getCommandAckTimeoutMs())
        ) && !repository.attemptStateIs(attempt.id(), "ACKED")) {
            throw new IllegalStateException("Broker cancel attempt cannot be marked SENT after TCP send: "
                    + attempt.id());
        }
    }

    private SubmitOrderCommandPayload readPayload(GatewayCommandAttemptRecord attempt) {
        try {
            return objectMapper.readValue(attempt.payloadJson(), SubmitOrderCommandPayload.class);
        } catch (Exception e) {
            repository.markAttemptFailed(
                    attempt.id(),
                    attempt.dispatchToken(),
                    "PAYLOAD_DESERIALIZE_FAILED",
                    e.getMessage(),
                    clock.instant()
            );
            throw new IllegalStateException("Failed to deserialize broker command attempt payload: " + attempt.id(), e);
        }
    }

    private CancelOrderCommandPayload readCancelPayload(GatewayCommandAttemptRecord attempt) {
        try {
            return objectMapper.readValue(attempt.payloadJson(), CancelOrderCommandPayload.class);
        } catch (Exception e) {
            repository.markAttemptFailed(
                    attempt.id(),
                    attempt.dispatchToken(),
                    "PAYLOAD_DESERIALIZE_FAILED",
                    e.getMessage(),
                    clock.instant()
            );
            throw new IllegalStateException("Failed to deserialize broker cancel attempt payload: " + attempt.id(), e);
        }
    }

    private void ensureDispatchTokenIsCurrent(
            GatewayCommandAttemptRecord attempt,
            String commandType,
            Instant checkedAt
    ) {
        if (!repository.dispatchTokenIsCurrent(attempt.id(), attempt.dispatchToken(), checkedAt)) {
            throw new IllegalStateException("Broker " + commandType
                    + " attempt dispatch token is no longer current before TCP send: " + attempt.id());
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
