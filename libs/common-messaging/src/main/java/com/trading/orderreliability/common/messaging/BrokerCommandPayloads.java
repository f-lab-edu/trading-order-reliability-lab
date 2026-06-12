package com.trading.orderreliability.common.messaging;

import java.util.UUID;

public final class BrokerCommandPayloads {

    private BrokerCommandPayloads() {
    }

    public record SubmitOrderCommandPayload(
            UUID orderId,
            String accountId,
            String market,
            String symbol,
            String side,
            String orderType,
            String tif,
            long orderQty,
            String limitPrice
    ) {
    }

    public record CancelOrderCommandPayload(UUID orderId) {
    }
}
