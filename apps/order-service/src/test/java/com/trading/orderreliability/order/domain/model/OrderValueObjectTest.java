package com.trading.orderreliability.order.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("주문 값 객체")
class OrderValueObjectTest {

    @Test
    @DisplayName("종목 코드는 앞뒤 공백을 제거하고 대문자로 정규화한다")
    void symbolIsNormalizedToUppercase() {
        assertThat(new Symbol(" aapl ").value()).isEqualTo("AAPL");
    }

    @Test
    @DisplayName("주문 수량은 음수를 허용하지 않는다")
    void quantityMustNotBeNegative() {
        assertThatThrownBy(() -> new OrderQuantity(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("양수 주문 수량은 0을 허용하지 않는다")
    void positiveQuantityRejectsZero() {
        assertThatThrownBy(() -> OrderQuantity.positive(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("주문 가격은 양수여야 한다")
    void priceMustBePositive() {
        assertThatThrownBy(() -> new OrderPrice(BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
