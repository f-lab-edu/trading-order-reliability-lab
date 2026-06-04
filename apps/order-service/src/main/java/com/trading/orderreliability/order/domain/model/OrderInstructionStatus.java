package com.trading.orderreliability.order.domain.model;

public enum OrderInstructionStatus {
    REQUESTED(false),
    COMPLETED(true),
    REJECTED(true),
    NOT_APPLIED(true),
    FAILED(true);

    private final boolean terminal;

    OrderInstructionStatus(boolean terminal) {
        this.terminal = terminal;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
