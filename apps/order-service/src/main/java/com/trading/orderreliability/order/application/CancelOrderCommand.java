package com.trading.orderreliability.order.application;

import com.trading.orderreliability.order.domain.model.AccountId;

public record CancelOrderCommand(
        AccountId accountId,
        String clientCancelRequestId,
        String traceId
) {
}
