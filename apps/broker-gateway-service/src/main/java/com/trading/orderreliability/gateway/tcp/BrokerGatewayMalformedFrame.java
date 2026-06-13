package com.trading.orderreliability.gateway.tcp;

import com.trading.orderreliability.broker.protocol.BrokerMalformedType;

record BrokerGatewayMalformedFrame(BrokerMalformedType malformedType, String reason, byte[] rawBytes) {
}
