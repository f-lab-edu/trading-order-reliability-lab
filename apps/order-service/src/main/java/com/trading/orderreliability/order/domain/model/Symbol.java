package com.trading.orderreliability.order.domain.model;

import java.util.Locale;
import java.util.Objects;

public record Symbol(String value) {

    public Symbol {
        Objects.requireNonNull(value, "value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("symbol must not be blank");
        }
        value = value.trim().toUpperCase(Locale.ROOT);
    }
}
