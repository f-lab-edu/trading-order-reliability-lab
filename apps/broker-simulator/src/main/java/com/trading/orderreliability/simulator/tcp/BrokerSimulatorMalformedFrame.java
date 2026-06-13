package com.trading.orderreliability.simulator.tcp;

import com.trading.orderreliability.broker.protocol.BrokerMalformedType;

public record BrokerSimulatorMalformedFrame(BrokerMalformedType malformedType, String reason) {
}
