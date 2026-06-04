package com.trading.orderreliability.order.domain.state;

import com.trading.orderreliability.order.domain.model.OrderQuantity;

import java.time.Instant;
import java.util.Objects;

public record OrderTransitionRequest(
        OrderTransitionTrigger trigger,
        OrderQuantity cumulativeFilledQuantity,
        Instant occurredAt,
        String reason
) {

    public OrderTransitionRequest {
        Objects.requireNonNull(trigger, "trigger must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
    }

    public static OrderTransitionRequest of(OrderTransitionTrigger trigger, Instant occurredAt, String reason) {
        return new OrderTransitionRequest(trigger, null, occurredAt, reason);
    }

    public static OrderTransitionRequest fill(OrderTransitionTrigger trigger, OrderQuantity cumulativeFilledQuantity, Instant occurredAt, String reason) {
        if (trigger != OrderTransitionTrigger.BROKER_ORDER_PARTIALLY_FILLED
                && trigger != OrderTransitionTrigger.BROKER_ORDER_FILLED) {
            throw new IllegalArgumentException("fill quantity can only be used with fill triggers");
        }
        return new OrderTransitionRequest(trigger, cumulativeFilledQuantity, occurredAt, reason);
    }

    public OrderQuantity requireCumulativeFilledQuantity() {
        if (cumulativeFilledQuantity == null) {
            throw new IllegalArgumentException("cumulative filled quantity is required for " + trigger);
        }
        return cumulativeFilledQuantity;
    }
}
