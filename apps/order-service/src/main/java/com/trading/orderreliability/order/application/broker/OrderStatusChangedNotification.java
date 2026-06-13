package com.trading.orderreliability.order.application.broker;

import com.trading.orderreliability.order.domain.model.OrderStatus;

import java.time.Instant;
import java.util.UUID;

public record OrderStatusChangedNotification(
        UUID orderId,
        OrderStatus previousStatus,
        OrderStatus currentStatus,
        long cumQty,
        long leavesQty,
        Instant occurredAt
) {
}
