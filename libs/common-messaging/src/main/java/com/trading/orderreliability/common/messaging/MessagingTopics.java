package com.trading.orderreliability.common.messaging;

public final class MessagingTopics {

    public static final String BROKER_COMMAND = "trading.broker.command.v1";
    public static final String BROKER_EVENT = "trading.broker.event.v1";
    public static final String RECOVERY_ATTEMPT_REPORT = "trading.recovery.attempt-report.v1";
    public static final String ORDER_LIFECYCLE = "trading.order.lifecycle.v1";
    public static final String RECOVERY_EVENT = "trading.recovery.event.v1";

    private MessagingTopics() {
    }
}
