package com.trading.orderreliability.order.domain.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderStatusTest {

    @Test
    void terminalStatusesAreMarkedTerminal() {
        assertThat(OrderStatus.FILLED.isTerminal()).isTrue();
        assertThat(OrderStatus.CANCELED.isTerminal()).isTrue();
        assertThat(OrderStatus.REJECTED.isTerminal()).isTrue();
        assertThat(OrderStatus.EXPIRED.isTerminal()).isTrue();
    }

    @Test
    void nonTerminalStatusesAreMarkedNonTerminal() {
        assertThat(OrderStatus.PENDING_ACK.isTerminal()).isFalse();
        assertThat(OrderStatus.LIVE.isTerminal()).isFalse();
        assertThat(OrderStatus.PARTIALLY_FILLED.isTerminal()).isFalse();
        assertThat(OrderStatus.PENDING_CANCEL.isTerminal()).isFalse();
        assertThat(OrderStatus.UNKNOWN.isTerminal()).isFalse();
    }
}
