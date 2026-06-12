package com.trading.orderreliability.order.application.result;

import com.trading.orderreliability.order.domain.model.Order;

public record PlaceOrderResult(Order order, boolean created) {
}
