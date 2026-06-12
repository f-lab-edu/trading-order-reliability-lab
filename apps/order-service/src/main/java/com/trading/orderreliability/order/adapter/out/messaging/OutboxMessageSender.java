package com.trading.orderreliability.order.adapter.out.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.trading.orderreliability.common.messaging.MessageEnvelope;

interface OutboxMessageSender {

    void send(String topicName, String messageKey, MessageEnvelope<JsonNode> envelope) throws Exception;
}
