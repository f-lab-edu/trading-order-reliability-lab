package com.trading.orderreliability.broker.protocol;

public enum BrokerBusinessAnomaly {
    FILL_CUM_QTY_LESS_THAN_LAST_FILL_QTY,
    PARTIAL_FILL_WITH_ZERO_LEAVES_QTY,
    EXPIRED_WITH_NON_ZERO_LEAVES_QTY,
    NOT_FOUND_STATUS_WITH_NON_ZERO_QTY
}
