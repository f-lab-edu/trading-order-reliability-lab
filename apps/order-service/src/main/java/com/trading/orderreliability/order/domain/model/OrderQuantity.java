package com.trading.orderreliability.order.domain.model;

public record OrderQuantity(long value) {

    public static OrderQuantity zero() {
        return new OrderQuantity(0);
    }

    public static OrderQuantity positive(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("order quantity must be positive");
        }
        return new OrderQuantity(value);
    }

    public OrderQuantity {
        if (value < 0) {
            throw new IllegalArgumentException("order quantity must not be negative");
        }
    }
}
