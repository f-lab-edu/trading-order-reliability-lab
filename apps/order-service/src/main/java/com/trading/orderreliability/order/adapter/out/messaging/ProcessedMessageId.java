package com.trading.orderreliability.order.adapter.out.messaging;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

class ProcessedMessageId implements Serializable {

    private String consumerName;
    private UUID messageId;

    protected ProcessedMessageId() {
    }

    ProcessedMessageId(String consumerName, UUID messageId) {
        this.consumerName = consumerName;
        this.messageId = messageId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ProcessedMessageId that)) {
            return false;
        }
        return Objects.equals(consumerName, that.consumerName)
                && Objects.equals(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerName, messageId);
    }
}
