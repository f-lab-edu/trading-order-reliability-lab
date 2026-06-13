package com.trading.orderreliability.broker.protocol;

public sealed interface BrokerMessage permits
        BrokerMessages.OrderRequest,
        BrokerMessages.OrderAccepted,
        BrokerMessages.OrderRejected,
        BrokerMessages.Fill,
        BrokerMessages.CancelRequest,
        BrokerMessages.CancelAccepted,
        BrokerMessages.CancelRejected,
        BrokerMessages.OrderExpired,
        BrokerMessages.StatusQuery,
        BrokerMessages.StatusSnapshot {

    BrokerCommonHeader header();

    default BrokerMessageId messageId() {
        return header().messageId();
    }
}
