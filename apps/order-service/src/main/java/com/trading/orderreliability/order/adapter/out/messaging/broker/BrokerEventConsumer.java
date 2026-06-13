package com.trading.orderreliability.order.adapter.out.messaging.broker;

import com.fasterxml.jackson.databind.JsonNode;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.order.adapter.out.messaging.parking.MessageParkingLot;
import com.trading.orderreliability.order.adapter.out.messaging.processed.ProcessedMessageGuard;
import com.trading.orderreliability.order.application.broker.BrokerEventApplicationService;

import java.time.Clock;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "order-service.messaging.kafka", name = "broker-event-consumer-enabled", havingValue = "true")
class BrokerEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(BrokerEventConsumer.class);
    private static final String CONSUMER_NAME = "order-service-broker-event-consumer";

    private final BrokerEventEnvelopeParser parser;
    private final ProcessedMessageGuard processedMessageGuard;
    private final BrokerEventApplicationService applicationService;
    private final MessageParkingLot parkingLot;
    private final Clock clock = Clock.systemUTC();

    BrokerEventConsumer(
            BrokerEventEnvelopeParser parser,
            ProcessedMessageGuard processedMessageGuard,
            BrokerEventApplicationService applicationService,
            MessageParkingLot parkingLot
    ) {
        this.parser = parser;
        this.processedMessageGuard = processedMessageGuard;
        this.applicationService = applicationService;
        this.parkingLot = parkingLot;
    }

    @KafkaListener(topics = MessagingTopics.BROKER_EVENT)
    void consume(ConsumerRecord<String, String> record) {
        MessageEnvelope<JsonNode> envelope;
        try {
            envelope = parser.parse(record.value());
        } catch (Exception e) {
            parkingLot.parkParseFailure(
                    MessagingTopics.BROKER_EVENT,
                    CONSUMER_NAME,
                    record.value(),
                    e.getMessage(),
                    clock.instant()
            );
            return;
        }
        processedMessageGuard.runOnce(
                CONSUMER_NAME,
                envelope,
                () -> log.debug("Broker event apply result={}", applicationService.apply(envelope))
        );
    }
}
