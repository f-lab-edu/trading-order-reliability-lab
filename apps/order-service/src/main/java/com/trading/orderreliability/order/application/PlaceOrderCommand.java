package com.trading.orderreliability.order.application;

import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;

public record PlaceOrderCommand(
        String clientOrderId,
        AccountId accountId,
        Market market,
        Symbol symbol,
        OrderSide side,
        OrderType orderType,
        TimeInForce timeInForce,
        OrderQuantity orderQty,
        OrderPrice limitPrice,
        String traceId
) {
}
