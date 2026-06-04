package com.trading.orderreliability.order.application;

import java.util.UUID;

public class OrderAccessDeniedException extends RuntimeException {

    public OrderAccessDeniedException(UUID orderId) {
        super("Account cannot access order: " + orderId);
    }
}
