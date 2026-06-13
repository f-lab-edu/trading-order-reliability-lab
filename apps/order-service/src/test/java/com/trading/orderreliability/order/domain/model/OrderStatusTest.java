package com.trading.orderreliability.order.domain.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("주문 상태")
class OrderStatusTest {

    @Test
    @DisplayName("종결 상태는 terminal로 표시된다")
    void terminalStatusesAreMarkedTerminal() {
        assertThat(OrderStatus.FILLED.isTerminal()).isTrue();
        assertThat(OrderStatus.CANCELED.isTerminal()).isTrue();
        assertThat(OrderStatus.REJECTED.isTerminal()).isTrue();
        assertThat(OrderStatus.EXPIRED.isTerminal()).isTrue();
    }

    @Test
    @DisplayName("비종결 상태는 terminal이 아닌 상태로 표시된다")
    void nonTerminalStatusesAreMarkedNonTerminal() {
        assertThat(OrderStatus.PENDING_ACK.isTerminal()).isFalse();
        assertThat(OrderStatus.LIVE.isTerminal()).isFalse();
        assertThat(OrderStatus.PARTIALLY_FILLED.isTerminal()).isFalse();
        assertThat(OrderStatus.PENDING_CANCEL.isTerminal()).isFalse();
        assertThat(OrderStatus.UNKNOWN.isTerminal()).isFalse();
    }
}
