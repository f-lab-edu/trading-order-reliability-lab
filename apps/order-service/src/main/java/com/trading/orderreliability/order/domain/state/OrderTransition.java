package com.trading.orderreliability.order.domain.state;

import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.model.Order;

import java.util.Objects;

public record OrderTransition(
        Order previousOrder,
        Order nextOrder,
        OrderStatus previousStatus,
        OrderStatus nextStatus,
        OrderTransitionTrigger trigger,
        String reason
) {

    public OrderTransition {
        Objects.requireNonNull(previousOrder, "previousOrder must not be null");
        Objects.requireNonNull(nextOrder, "nextOrder must not be null");
        Objects.requireNonNull(previousStatus, "previousStatus must not be null");
        Objects.requireNonNull(nextStatus, "nextStatus must not be null");
        Objects.requireNonNull(trigger, "trigger must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
    }

    public static OrderTransition of(Order previousOrder, Order nextOrder, OrderTransitionRequest request) {
        return new OrderTransition(
                previousOrder,
                nextOrder,
                previousOrder.status(),
                nextOrder.status(),
                request.trigger(),
                request.reason()
        );
    }

    public boolean changed() {
        return previousStatus != nextStatus;
    }
}
