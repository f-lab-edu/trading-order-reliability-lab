package com.trading.orderreliability.simulator.domain;

import java.time.Instant;
import java.util.UUID;

public record SimulatorOrder(
        UUID orderId,
        String brokerOrderId,
        String accountId,
        String symbol,
        String traceId,
        long orderQty,
        long cumQty,
        long leavesQty,
        SimulatorOrderStatus status,
        String rejectCode,
        Instant updatedAtUtc
) {
}
