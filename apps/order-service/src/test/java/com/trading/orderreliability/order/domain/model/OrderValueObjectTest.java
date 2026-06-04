package com.trading.orderreliability.order.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderValueObjectTest {

    @Test
    void symbolIsNormalizedToUppercase() {
        assertThat(new Symbol(" aapl ").value()).isEqualTo("AAPL");
    }

    @Test
    void quantityMustNotBeNegative() {
        assertThatThrownBy(() -> new OrderQuantity(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void positiveQuantityRejectsZero() {
        assertThatThrownBy(() -> OrderQuantity.positive(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void priceMustBePositive() {
        assertThatThrownBy(() -> new OrderPrice(BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
