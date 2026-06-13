package com.trading.orderreliability.broker.protocol;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record BrokerCommonHeader(
        BrokerMessageId messageId,
        int protocolVersion,
        int bodyLength,
        String wireMessageId,
        UUID orderId,
        String traceId,
        Instant sentAtUtc
) {

    public BrokerCommonHeader {
        Objects.requireNonNull(messageId, "messageId must not be null");
        Objects.requireNonNull(wireMessageId, "wireMessageId must not be null");
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(sentAtUtc, "sentAtUtc must not be null");
        traceId = traceId == null ? "" : traceId;
    }

    public static BrokerCommonHeader of(
            BrokerMessageId messageId,
            String wireMessageId,
            UUID orderId,
            String traceId,
            Instant sentAtUtc
    ) {
        return new BrokerCommonHeader(
                messageId,
                BrokerProtocolModule.PROTOCOL_VERSION,
                messageId.bodyLength(),
                wireMessageId,
                orderId,
                traceId,
                sentAtUtc
        );
    }
}
