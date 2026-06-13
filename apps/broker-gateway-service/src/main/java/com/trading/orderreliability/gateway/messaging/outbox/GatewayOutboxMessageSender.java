package com.trading.orderreliability.gateway.messaging.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.trading.orderreliability.common.messaging.MessageEnvelope;

interface GatewayOutboxMessageSender {

    void send(String topicName, String messageKey, MessageEnvelope<JsonNode> envelope) throws Exception;
}
