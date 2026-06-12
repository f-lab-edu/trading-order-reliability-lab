package com.trading.orderreliability.common.messaging;

public enum OutboxStatus {
    READY,
    PUBLISHING,
    SENT,
    FAILED
}
