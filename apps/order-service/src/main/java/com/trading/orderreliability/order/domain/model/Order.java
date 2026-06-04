package com.trading.orderreliability.order.domain.model;

import java.time.Instant;
import java.util.Objects;

public record Order(
        OrderId orderId,
        AccountId accountId,
        Market market,
        Symbol symbol,
        OrderSide side,
        OrderType orderType,
        TimeInForce timeInForce,
        OrderQuantity orderQty,
        OrderPrice limitPrice,
        OrderStatus status,
        OrderQuantity cumQty,
        OrderQuantity leavesQty,
        ReconciliationStatus reconciliationStatus,
        Instant createdAt,
        Instant updatedAt,
        Instant terminalAt
) {

    public Order {
        Objects.requireNonNull(orderId, "orderId must not be null");
        Objects.requireNonNull(accountId, "accountId must not be null");
        Objects.requireNonNull(market, "market must not be null");
        Objects.requireNonNull(symbol, "symbol must not be null");
        Objects.requireNonNull(side, "side must not be null");
        Objects.requireNonNull(orderType, "orderType must not be null");
        Objects.requireNonNull(timeInForce, "timeInForce must not be null");
        Objects.requireNonNull(orderQty, "orderQty must not be null");
        Objects.requireNonNull(limitPrice, "limitPrice must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(cumQty, "cumQty must not be null");
        Objects.requireNonNull(leavesQty, "leavesQty must not be null");
        Objects.requireNonNull(reconciliationStatus, "reconciliationStatus must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (orderQty.value() <= 0) {
            throw new IllegalArgumentException("order quantity must be positive");
        }
        if (cumQty.value() > orderQty.value() || leavesQty.value() > orderQty.value()) {
            throw new IllegalArgumentException("cumQty and leavesQty must not exceed orderQty");
        }
        if (!status.isTerminal() && cumQty.value() + leavesQty.value() != orderQty.value()) {
            throw new IllegalArgumentException("non-terminal order must satisfy cumQty plus leavesQty equals orderQty");
        }
        if ((status == OrderStatus.FILLED || status == OrderStatus.CANCELED || status == OrderStatus.EXPIRED)
                && leavesQty.value() != 0) {
            throw new IllegalArgumentException("filled, canceled, and expired orders must have zero leavesQty");
        }
        if (status.isTerminal() && terminalAt == null) {
            throw new IllegalArgumentException("terminalAt must be present for terminal order");
        }
        if (!status.isTerminal() && terminalAt != null) {
            throw new IllegalArgumentException("terminalAt must be null for non-terminal order");
        }
    }

    public static Order createPendingAck(
            OrderId orderId,
            AccountId accountId,
            Market market,
            Symbol symbol,
            OrderSide side,
            OrderType orderType,
            TimeInForce timeInForce,
            OrderQuantity orderQty,
            OrderPrice limitPrice,
            Instant now
    ) {
        return new Order(
                orderId,
                accountId,
                market,
                symbol,
                side,
                orderType,
                timeInForce,
                orderQty,
                limitPrice,
                OrderStatus.PENDING_ACK,
                OrderQuantity.zero(),
                orderQty,
                ReconciliationStatus.NONE,
                now,
                now,
                null
        );
    }

    public Order withStatus(OrderStatus nextStatus, Instant updatedAt) {
        Instant nextTerminalAt = nextStatus.isTerminal() ? updatedAt : null;
        return new Order(
                orderId,
                accountId,
                market,
                symbol,
                side,
                orderType,
                timeInForce,
                orderQty,
                limitPrice,
                nextStatus,
                cumQty,
                nextStatus == OrderStatus.CANCELED || nextStatus == OrderStatus.EXPIRED ? OrderQuantity.zero() : leavesQty,
                reconciliationStatus,
                createdAt,
                updatedAt,
                nextTerminalAt
        );
    }

    public Order withFill(OrderQuantity nextCumQty, Instant updatedAt) {
        long nextLeavesQty = orderQty.value() - nextCumQty.value();
        if (nextLeavesQty < 0) {
            throw new IllegalArgumentException("filled quantity must not exceed order quantity");
        }
        OrderStatus nextStatus = nextLeavesQty == 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
        return new Order(
                orderId,
                accountId,
                market,
                symbol,
                side,
                orderType,
                timeInForce,
                orderQty,
                limitPrice,
                nextStatus,
                nextCumQty,
                new OrderQuantity(nextLeavesQty),
                reconciliationStatus,
                createdAt,
                updatedAt,
                nextStatus.isTerminal() ? updatedAt : null
        );
    }

    public Order withUnknown(ReconciliationStatus reconciliationStatus, Instant updatedAt) {
        if (reconciliationStatus != ReconciliationStatus.PENDING) {
            throw new IllegalArgumentException("UNKNOWN order must enter PENDING reconciliation");
        }
        return new Order(
                orderId,
                accountId,
                market,
                symbol,
                side,
                orderType,
                timeInForce,
                orderQty,
                limitPrice,
                OrderStatus.UNKNOWN,
                cumQty,
                leavesQty,
                reconciliationStatus,
                createdAt,
                updatedAt,
                null
        );
    }
}
