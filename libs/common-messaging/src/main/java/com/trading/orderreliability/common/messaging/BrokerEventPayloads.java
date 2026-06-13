package com.trading.orderreliability.common.messaging;

import java.time.Instant;
import java.util.UUID;

public final class BrokerEventPayloads {

    private BrokerEventPayloads() {
    }

    public record BrokerOrderAcknowledgedPayload(
            UUID orderId,
            String brokerEventDedupKey,
            String payloadHash,
            String brokerOrderId,
            Instant brokerEventTime
    ) {
    }

    public record BrokerOrderRejectedPayload(
            UUID orderId,
            String brokerEventDedupKey,
            String payloadHash,
            String rejectCode,
            String rejectMessage,
            Instant brokerEventTime
    ) {
    }

    public record BrokerOrderPartiallyFilledPayload(
            UUID orderId,
            String brokerEventDedupKey,
            String payloadHash,
            long lastFillQty,
            long cumQty,
            long leavesQty,
            String brokerOrderId,
            String executionId,
            Instant brokerEventTime
    ) {
    }

    public record BrokerOrderFilledPayload(
            UUID orderId,
            String brokerEventDedupKey,
            String payloadHash,
            long lastFillQty,
            long cumQty,
            long leavesQty,
            String brokerOrderId,
            String executionId,
            Instant brokerEventTime
    ) {
    }
}
