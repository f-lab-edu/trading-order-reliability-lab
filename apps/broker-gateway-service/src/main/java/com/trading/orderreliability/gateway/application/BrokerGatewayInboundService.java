package com.trading.orderreliability.gateway.application;

import com.trading.orderreliability.broker.protocol.BrokerCommonHeader;
import com.trading.orderreliability.broker.protocol.BrokerFrameCodec;
import com.trading.orderreliability.broker.protocol.BrokerMalformedType;
import com.trading.orderreliability.broker.protocol.BrokerMessage;
import com.trading.orderreliability.broker.protocol.BrokerMessages.Fill;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderAccepted;
import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRejected;
import com.trading.orderreliability.broker.protocol.BrokerParseResult;
import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderAcknowledgedPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderFilledPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderPartiallyFilledPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderRejectedPayload;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.gateway.config.GatewayBrokerProperties;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class BrokerGatewayInboundService {

    private static final String CONSUMER_NAME = "broker-gateway-inbound-handler";
    private static final String SOURCE_TOPIC = "broker-gateway-tcp-inbound";
    private static final String ERROR_CODE_ATTEMPT_MISMATCH = "BROKER_EVENT_ATTEMPT_MISMATCH";
    private static final String ERROR_CODE_BINDING_MISMATCH = "BROKER_EVENT_BINDING_MISMATCH";

    private final BrokerFrameCodec codec = new BrokerFrameCodec();
    private final GatewayJdbcRepository repository;
    private final GatewayBrokerProperties properties;
    private final UuidV7Generator uuidGenerator;
    private final Clock clock = Clock.systemUTC();

    public BrokerGatewayInboundService(
            GatewayJdbcRepository repository,
            GatewayBrokerProperties properties,
            UuidV7Generator uuidGenerator
    ) {
        this.repository = repository;
        this.properties = properties;
        this.uuidGenerator = uuidGenerator;
    }

    @Transactional
    public void handleFrame(byte[] frame) {
        Instant now = clock.instant();
        BrokerParseResult parseResult = codec.decode(frame);
        if (parseResult instanceof BrokerParseResult.Malformed(
            BrokerMalformedType malformedType, String reason
        )) {
            insertMalformedJournal(malformedType, reason, frame, now);
            return;
        }

        BrokerParseResult.Success success = (BrokerParseResult.Success) parseResult;
        BrokerMessage message = success.message();
        BrokerCommonHeader header = message.header();
        repository.insertJournal(
                uuidGenerator.generate(),
                properties.getCode(),
                "IN",
                header.messageId().code(),
                header.wireMessageId(),
                header.traceId(),
                brokerOrderId(message),
                header.orderId(),
                "PARSED",
                null,
                null,
                frame,
                message,
                success.payloadHash(),
                now
        );

        if (message instanceof OrderAccepted accepted) {
            handleAccepted(header, accepted, success.payloadHash(), frame, now);
        } else if (message instanceof OrderRejected rejected) {
            handleRejected(header, rejected, success.payloadHash(), frame, now);
        } else if (message instanceof Fill fill) {
            handleFill(header, fill, success.payloadHash(), frame, now);
        }
    }

    @Transactional
    public void handleMalformedFrame(BrokerMalformedType malformedType, String reason, byte[] rawBytes) {
        insertMalformedJournal(malformedType, reason, rawBytes, clock.instant());
    }

    private void handleAccepted(
            BrokerCommonHeader header,
            OrderAccepted accepted,
            String payloadHash,
            byte[] frame,
            Instant now
    ) {
        if (!repository.submitAttemptMatches(properties.getCode(), header.wireMessageId(), header.orderId())) {
            parkInboundFrame(ERROR_CODE_ATTEMPT_MISMATCH, "ACKN does not match a known submit attempt", frame, now);
            return;
        }
        if (!repository.updateBindingAccepted(header.orderId(), properties.getCode(), accepted.brokerOrderId(), accepted.acceptedAtUtc())) {
            parkInboundFrame(ERROR_CODE_BINDING_MISMATCH, "ACKN submit attempt has no broker order binding", frame, now);
            return;
        }
        repository.markAttemptAcked(properties.getCode(), header.wireMessageId(), accepted.brokerOrderId(), now);
        repository.appendBrokerEvent(
                uuidGenerator.generate(),
                header.orderId(),
                MessageTypes.BROKER_ORDER_ACKNOWLEDGED,
                new BrokerOrderAcknowledgedPayload(
                        header.orderId(),
                        dedupKey(header.messageId().code(), header.wireMessageId(), header.orderId()),
                        payloadHash,
                        accepted.brokerOrderId(),
                        accepted.acceptedAtUtc()
                ),
                header.traceId(),
                now
        );
    }

    private void handleRejected(
            BrokerCommonHeader header,
            OrderRejected rejected,
            String payloadHash,
            byte[] frame,
            Instant now
    ) {
        if (!repository.submitAttemptMatches(properties.getCode(), header.wireMessageId(), header.orderId())) {
            parkInboundFrame(ERROR_CODE_ATTEMPT_MISMATCH, "RJCT does not match a known submit attempt", frame, now);
            return;
        }
        repository.markAttemptAcked(properties.getCode(), header.wireMessageId(), null, now);
        repository.appendBrokerEvent(
                uuidGenerator.generate(),
                header.orderId(),
                MessageTypes.BROKER_ORDER_REJECTED,
                new BrokerOrderRejectedPayload(
                        header.orderId(),
                        dedupKey(header.messageId().code(), header.wireMessageId(), header.orderId()),
                        payloadHash,
                        rejected.rejectCode(),
                        rejected.rejectReason(),
                        header.sentAtUtc()
                ),
                header.traceId(),
                now
        );
    }

    private void handleFill(BrokerCommonHeader header, Fill fill, String payloadHash, byte[] frame, Instant now) {
        if (repository.findOrderIdByBrokerOrderId(properties.getCode(), fill.brokerOrderId())
                .filter(boundOrderId -> boundOrderId.equals(header.orderId()))
                .isEmpty()) {
            parkInboundFrame(ERROR_CODE_BINDING_MISMATCH, "FILL does not match a known broker order binding", frame, now);
            return;
        }
        String messageType = fill.leavesQty() == 0 || "F".equals(fill.fillStatus())
                ? MessageTypes.BROKER_ORDER_FILLED
                : MessageTypes.BROKER_ORDER_PARTIALLY_FILLED;
        Object payload = MessageTypes.BROKER_ORDER_FILLED.equals(messageType)
                ? new BrokerOrderFilledPayload(
                        header.orderId(),
                        fillDedupKey(fill),
                        payloadHash,
                        fill.lastFillQty(),
                        fill.cumQty(),
                        fill.leavesQty(),
                        fill.brokerOrderId(),
                        fill.executionId(),
                        fill.filledAtUtc()
                )
                : new BrokerOrderPartiallyFilledPayload(
                        header.orderId(),
                        fillDedupKey(fill),
                        payloadHash,
                        fill.lastFillQty(),
                        fill.cumQty(),
                        fill.leavesQty(),
                        fill.brokerOrderId(),
                        fill.executionId(),
                        fill.filledAtUtc()
                );
        repository.appendBrokerEvent(
                uuidGenerator.generate(),
                header.orderId(),
                messageType,
                payload,
                header.traceId(),
                now
        );
    }

    private void insertMalformedJournal(
            BrokerMalformedType malformedType,
            String reason,
            byte[] frame,
            Instant recordedAt
    ) {
        repository.insertJournal(
                uuidGenerator.generate(),
                properties.getCode(),
                "IN",
                null,
                null,
                null,
                null,
                null,
                "MALFORMED_" + malformedType.name(),
                malformedType.name(),
                reason,
                frame,
                null,
                null,
                recordedAt
        );
    }

    private String dedupKey(String msgId, String wireMessageId, UUID orderId) {
        return "%s:%s:%s:%s".formatted(properties.getCode(), msgId, wireMessageId, orderId);
    }

    private String fillDedupKey(Fill fill) {
        return "%s:FILL:%s:%s".formatted(properties.getCode(), fill.brokerOrderId(), fill.executionId());
    }

    private void parkInboundFrame(String errorCode, String errorMessage, byte[] frame, Instant now) {
        repository.parkRawMessage(
                uuidGenerator.generate(),
                SOURCE_TOPIC,
                CONSUMER_NAME,
                Base64.getEncoder().encodeToString(frame),
                errorCode,
                errorMessage,
                now
        );
    }

    private static String brokerOrderId(BrokerMessage message) {
        if (message instanceof OrderAccepted accepted) {
            return accepted.brokerOrderId();
        }
        if (message instanceof Fill fill) {
            return fill.brokerOrderId();
        }
        return null;
    }
}
