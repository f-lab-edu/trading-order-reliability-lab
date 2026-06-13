package com.trading.orderreliability.order.application.broker;

public enum BrokerEventApplyResult {
    APPLIED,
    DUPLICATE_SKIPPED,
    PAYLOAD_MISMATCH_PARKED
}
