package com.trading.orderreliability.order.domain.state;

import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.model.ReconciliationStatus;

import org.springframework.stereotype.Component;

@Component
public class DefaultOrderStateMachine implements OrderStateMachine {

    @Override
    public OrderTransition transition(Order current, OrderTransitionRequest request) {
        if (current.status().isTerminal()) {
            return OrderTransition.of(current, current, request);
        }

        Order next = switch (request.trigger()) {
            case BROKER_ORDER_ACKNOWLEDGED -> acknowledge(current, request);
            case BROKER_ORDER_REJECTED -> reject(current, request);
            case BROKER_ORDER_PARTIALLY_FILLED -> applyPartialFill(current, request);
            case BROKER_ORDER_FILLED -> applyFullFill(current, request);
            case CANCEL_REQUESTED -> requestCancel(current, request);
            case BROKER_CANCEL_ACKNOWLEDGED -> acknowledgeCancel(current, request);
            case BROKER_CANCEL_REJECTED -> rejectCancel(current, request);
            case BROKER_ORDER_EXPIRED -> expire(current, request);
            case SUBMIT_OUTCOME_TIMED_OUT, CANCEL_OUTCOME_TIMED_OUT -> markUnknown(current, request);
            case RECONCILIATION_SNAPSHOT_APPLIED -> current;
        };

        return OrderTransition.of(current, next, request);
    }

    private Order acknowledge(Order current, OrderTransitionRequest request) {
        if (current.status() == OrderStatus.PENDING_CANCEL) {
            return current.withStatus(OrderStatus.PENDING_CANCEL, request.occurredAt());
        }
        if (current.status() != OrderStatus.PENDING_ACK) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        return current.withStatus(OrderStatus.LIVE, request.occurredAt());
    }

    private Order reject(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.PENDING_ACK && current.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        return current.withStatus(OrderStatus.REJECTED, request.occurredAt());
    }

    private Order applyPartialFill(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.PENDING_ACK
                && current.status() != OrderStatus.LIVE
                && current.status() != OrderStatus.PARTIALLY_FILLED
                && current.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        OrderQuantity nextCumQty = request.requireCumulativeFilledQuantity();
        if (nextCumQty.value() <= current.cumQty().value() || nextCumQty.value() >= current.orderQty().value()) {
            throw new IllegalArgumentException("partial fill quantity must increase cumQty and remain below orderQty");
        }
        if (current.status() == OrderStatus.PENDING_CANCEL) {
            long nextLeavesQty = current.orderQty().value() - nextCumQty.value();
            return new Order(
                    current.orderId(),
                    current.accountId(),
                    current.market(),
                    current.symbol(),
                    current.side(),
                    current.orderType(),
                    current.timeInForce(),
                    current.orderQty(),
                    current.limitPrice(),
                    OrderStatus.PENDING_CANCEL,
                    nextCumQty,
                    new OrderQuantity(nextLeavesQty),
                    current.reconciliationStatus(),
                    current.createdAt(),
                    request.occurredAt(),
                    null
            );
        }
        return current.withFill(nextCumQty, request.occurredAt());
    }

    private Order applyFullFill(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.PENDING_ACK
                && current.status() != OrderStatus.LIVE
                && current.status() != OrderStatus.PARTIALLY_FILLED
                && current.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        OrderQuantity nextCumQty = request.requireCumulativeFilledQuantity();
        if (nextCumQty.value() != current.orderQty().value()) {
            throw new IllegalArgumentException("full fill quantity must equal orderQty");
        }
        return current.withFill(nextCumQty, request.occurredAt());
    }

    private Order requestCancel(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.PENDING_ACK
                && current.status() != OrderStatus.LIVE
                && current.status() != OrderStatus.PARTIALLY_FILLED) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        return current.withStatus(OrderStatus.PENDING_CANCEL, request.occurredAt());
    }

    private Order acknowledgeCancel(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        return current.withStatus(OrderStatus.CANCELED, request.occurredAt());
    }

    private Order rejectCancel(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        if (current.cumQty().value() == 0) {
            return current.withStatus(OrderStatus.LIVE, request.occurredAt());
        }
        if (current.cumQty().value() < current.orderQty().value()) {
            return current.withStatus(OrderStatus.PARTIALLY_FILLED, request.occurredAt());
        }
        return current.withStatus(OrderStatus.FILLED, request.occurredAt());
    }

    private Order expire(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.LIVE
                && current.status() != OrderStatus.PARTIALLY_FILLED
                && current.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        return current.withStatus(OrderStatus.EXPIRED, request.occurredAt());
    }

    private Order markUnknown(Order current, OrderTransitionRequest request) {
        if (current.status() != OrderStatus.PENDING_ACK && current.status() != OrderStatus.PENDING_CANCEL) {
            throw new InvalidOrderTransitionException(current.status(), request.trigger());
        }
        return current.withUnknown(ReconciliationStatus.PENDING, request.occurredAt());
    }
}
