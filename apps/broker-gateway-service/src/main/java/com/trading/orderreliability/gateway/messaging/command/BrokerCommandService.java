package com.trading.orderreliability.gateway.messaging.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.CancelOrderCommandPayload;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.SubmitOrderCommandPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.gateway.config.GatewayBrokerProperties;
import com.trading.orderreliability.gateway.persistence.GatewayJdbcRepository;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class BrokerCommandService {

    private static final Logger log = LoggerFactory.getLogger(BrokerCommandService.class);
    private static final String CONSUMER_NAME = "broker-gateway-command-consumer";

    private final GatewayJdbcRepository repository;
    private final ObjectMapper objectMapper;
    private final UuidV7Generator uuidGenerator;
    private final GatewayBrokerProperties properties;
    private final Clock clock = Clock.systemUTC();

    public BrokerCommandService(
            GatewayJdbcRepository repository,
            ObjectMapper objectMapper,
            UuidV7Generator uuidGenerator,
            GatewayBrokerProperties properties
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.uuidGenerator = uuidGenerator;
        this.properties = properties;
    }

    @Transactional
    public BrokerCommandHandlingResult handle(MessageEnvelope<JsonNode> envelope) {
        int inserted = repository.insertProcessedIfAbsent(
                CONSUMER_NAME,
                envelope.messageId(),
                envelope.messageType(),
                envelope.messageKey(),
                clock.instant()
        );
        if (inserted != 1) {
            return BrokerCommandHandlingResult.DUPLICATE_SKIPPED;
        }

        if (MessageTypes.SUBMIT_ORDER_COMMAND.equals(envelope.messageType())) {
            return handleSubmit(envelope);
        }
        if (MessageTypes.CANCEL_ORDER_COMMAND.equals(envelope.messageType())) {
            return handleCancel(envelope);
        }

        repository.parkMessage(
                uuidGenerator.generate(),
                MessagingTopics.BROKER_COMMAND,
                CONSUMER_NAME,
                envelope,
                "UNSUPPORTED_COMMAND",
                "Broker Gateway currently dispatches SubmitOrderCommand and CancelOrderCommand only",
                clock.instant()
        );
        return BrokerCommandHandlingResult.PARKED_UNSUPPORTED;
    }

    private BrokerCommandHandlingResult handleSubmit(MessageEnvelope<JsonNode> envelope) {
        SubmitOrderCommandPayload payload = objectMapper.convertValue(envelope.payload(), SubmitOrderCommandPayload.class);
        Instant now = clock.instant();
        UUID attemptId = uuidGenerator.generate();
        String wireMessageId = wireMessageId();
        repository.insertOrKeepBinding(uuidGenerator.generate(), payload.orderId(), properties.getCode(), now);
        repository.insertSubmitAttempt(
                attemptId,
                envelope.messageId(),
                payload.orderId(),
                properties.getCode(),
                wireMessageId,
                envelope.traceId(),
                payload,
                now
        );
        log.debug("SubmitOrderCommand stored for broker dispatch: orderId={}, wireMessageId={}",
                payload.orderId(),
                wireMessageId);
        return BrokerCommandHandlingResult.HANDLED;
    }

    private BrokerCommandHandlingResult handleCancel(MessageEnvelope<JsonNode> envelope) {
        CancelOrderCommandPayload payload = objectMapper.convertValue(envelope.payload(), CancelOrderCommandPayload.class);
        Instant now = clock.instant();
        UUID attemptId = uuidGenerator.generate();
        String wireMessageId = wireMessageId();
        repository.insertOrKeepBinding(uuidGenerator.generate(), payload.orderId(), properties.getCode(), now);
        repository.insertCancelAttempt(
                attemptId,
                envelope.messageId(),
                payload.orderId(),
                properties.getCode(),
                wireMessageId,
                envelope.traceId(),
                payload,
                now
        );
        log.debug("CancelOrderCommand stored for broker dispatch: orderId={}, wireMessageId={}",
                payload.orderId(),
                wireMessageId);
        return BrokerCommandHandlingResult.HANDLED;
    }

    private static String wireMessageId() {
        return "W-GW-" + UUID.randomUUID();
    }
}
