package com.trading.orderreliability.order.domain.model;

public enum OrderStatus {
    PENDING_ACK(false),
    LIVE(false),
    PARTIALLY_FILLED(false),
    PENDING_CANCEL(false),
    UNKNOWN(false),
    FILLED(true),
    CANCELED(true),
    REJECTED(true),
    EXPIRED(true);

    private final boolean terminal;

    OrderStatus(boolean terminal) {
        this.terminal = terminal;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
