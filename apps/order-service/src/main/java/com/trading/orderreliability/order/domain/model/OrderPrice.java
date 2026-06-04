package com.trading.orderreliability.order.domain.model;

import java.math.BigDecimal;
import java.util.Objects;

public record OrderPrice(BigDecimal value) {

    public OrderPrice {
        Objects.requireNonNull(value, "value must not be null");
        if (value.signum() <= 0) {
            throw new IllegalArgumentException("order price must be positive");
        }
        value = value.stripTrailingZeros();
    }
}
