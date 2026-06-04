package com.trading.orderreliability.order.domain.model;

import java.util.Objects;

public record AccountId(String value) {

    public AccountId {
        Objects.requireNonNull(value, "value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("account id must not be blank");
        }
    }
}
