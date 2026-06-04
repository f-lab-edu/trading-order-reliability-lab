package com.trading.orderreliability.order.application;

import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderInstruction;

public record CancelOrderResult(Order order, OrderInstruction instruction) {
}
