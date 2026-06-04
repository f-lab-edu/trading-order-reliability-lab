package com.trading.orderreliability.order.domain.model;

import java.util.Objects;
import java.util.UUID;

public record OrderInstructionId(UUID value) {

    public OrderInstructionId {
        Objects.requireNonNull(value, "value must not be null");
    }
}
