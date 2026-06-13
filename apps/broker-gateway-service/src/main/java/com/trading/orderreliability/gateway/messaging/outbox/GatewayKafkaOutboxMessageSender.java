package com.trading.orderreliability.gateway.messaging.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.trading.orderreliability.common.messaging.MessageEnvelope;

import java.util.concurrent.TimeUnit;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
class GatewayKafkaOutboxMessageSender implements GatewayOutboxMessageSender {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final GatewayOutboxPublisherProperties properties;

    GatewayKafkaOutboxMessageSender(KafkaTemplate<Object, Object> kafkaTemplate, GatewayOutboxPublisherProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Override
    public void send(String topicName, String messageKey, MessageEnvelope<JsonNode> envelope) throws Exception {
        kafkaTemplate.send(topicName, messageKey, envelope)
                .get(properties.getPublishTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }
}
