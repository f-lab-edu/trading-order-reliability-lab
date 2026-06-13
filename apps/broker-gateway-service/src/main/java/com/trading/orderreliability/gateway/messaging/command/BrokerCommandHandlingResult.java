package com.trading.orderreliability.gateway.messaging.command;

public enum BrokerCommandHandlingResult {
    HANDLED,
    DUPLICATE_SKIPPED,
    PARKED_UNSUPPORTED
}
