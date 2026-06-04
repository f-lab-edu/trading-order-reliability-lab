package com.trading.orderreliability.order.domain.state;

import com.trading.orderreliability.order.domain.model.OrderStatus;

public class InvalidOrderTransitionException extends RuntimeException {

    public InvalidOrderTransitionException(OrderStatus currentStatus, OrderTransitionTrigger trigger) {
        super("Invalid order transition: currentStatus=%s, trigger=%s".formatted(currentStatus, trigger));
    }
}
