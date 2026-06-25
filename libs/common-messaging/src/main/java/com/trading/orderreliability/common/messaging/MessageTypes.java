package com.trading.orderreliability.common.messaging;

public final class MessageTypes {

    public static final String SUBMIT_ORDER_COMMAND = "SubmitOrderCommand";
    public static final String CANCEL_ORDER_COMMAND = "CancelOrderCommand";
    public static final String QUERY_ORDER_STATUS_COMMAND = "QueryOrderStatusCommand";
    public static final String BROKER_ORDER_ACKNOWLEDGED = "BrokerOrderAcknowledged";
    public static final String BROKER_ORDER_REJECTED = "BrokerOrderRejected";
    public static final String BROKER_ORDER_PARTIALLY_FILLED = "BrokerOrderPartiallyFilled";
    public static final String BROKER_ORDER_FILLED = "BrokerOrderFilled";
    public static final String BROKER_CANCEL_ACKNOWLEDGED = "BrokerCancelAcknowledged";
    public static final String BROKER_CANCEL_REJECTED = "BrokerCancelRejected";

    private MessageTypes() {
    }
}
