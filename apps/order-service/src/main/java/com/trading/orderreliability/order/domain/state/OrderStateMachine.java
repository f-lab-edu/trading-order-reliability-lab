package com.trading.orderreliability.order.domain.state;

import com.trading.orderreliability.order.domain.model.Order;

public interface OrderStateMachine {

    OrderTransition transition(Order current, OrderTransitionRequest request);
}
