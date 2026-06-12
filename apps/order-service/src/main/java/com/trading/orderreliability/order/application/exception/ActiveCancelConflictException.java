package com.trading.orderreliability.order.application.exception;

import java.util.UUID;

public class ActiveCancelConflictException extends RuntimeException {

    public ActiveCancelConflictException(UUID orderId) {
        super("Order already has an active cancel instruction: " + orderId);
    }
}
