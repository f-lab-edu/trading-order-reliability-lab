package com.trading.orderreliability.order.application.exception;

import java.util.UUID;

public class OrderAccessDeniedException extends RuntimeException {

    public OrderAccessDeniedException(UUID orderId) {
        super("Account cannot access order: " + orderId);
    }
}
