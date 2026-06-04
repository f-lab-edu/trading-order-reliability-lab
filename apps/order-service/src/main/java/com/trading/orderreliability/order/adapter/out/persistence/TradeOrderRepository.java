package com.trading.orderreliability.order.adapter.out.persistence;

import com.trading.orderreliability.common.id.UuidBytes;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.ReconciliationStatus;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class TradeOrderRepository {

    private final JdbcTemplate jdbcTemplate;

    public TradeOrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(Order order) {
        jdbcTemplate.update("""
                        INSERT INTO trade_order (
                            id, account_id, market, symbol, side, order_type, tif,
                            order_qty, limit_price, status, reconciliation_status,
                            cum_qty, leaves_qty, version, created_at, updated_at, terminal_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                UuidBytes.toBytes(order.orderId().value()),
                order.accountId().value(),
                order.market().name(),
                order.symbol().value(),
                order.side().name(),
                order.orderType().name(),
                order.timeInForce().name(),
                order.orderQty().value(),
                order.limitPrice().value(),
                order.status().name(),
                order.reconciliationStatus().name(),
                order.cumQty().value(),
                order.leavesQty().value(),
                0L,
                Timestamp.from(order.createdAt()),
                Timestamp.from(order.updatedAt()),
                toTimestamp(order.terminalAt())
        );
    }

    public void updateState(Order order) {
        jdbcTemplate.update("""
                        UPDATE trade_order
                        SET status = ?,
                            reconciliation_status = ?,
                            cum_qty = ?,
                            leaves_qty = ?,
                            version = version + 1,
                            updated_at = ?,
                            terminal_at = ?
                        WHERE id = ?
                        """,
                order.status().name(),
                order.reconciliationStatus().name(),
                order.cumQty().value(),
                order.leavesQty().value(),
                Timestamp.from(order.updatedAt()),
                toTimestamp(order.terminalAt()),
                UuidBytes.toBytes(order.orderId().value())
        );
    }

    public Optional<Order> findById(OrderId orderId) {
        List<Order> orders = jdbcTemplate.query("""
                        SELECT *
                        FROM trade_order
                        WHERE id = ?
                        """,
                this::mapOrder,
                UuidBytes.toBytes(orderId.value())
        );
        return orders.stream().findFirst();
    }

    public Optional<Order> findByIdForUpdate(OrderId orderId) {
        List<Order> orders = jdbcTemplate.query("""
                        SELECT *
                        FROM trade_order
                        WHERE id = ?
                        FOR UPDATE
                        """,
                this::mapOrder,
                UuidBytes.toBytes(orderId.value())
        );
        return orders.stream().findFirst();
    }

    public List<Order> findByAccount(String accountId, OrderStatus status, int limit) {
        if (status == null) {
            return jdbcTemplate.query("""
                            SELECT *
                            FROM trade_order
                            WHERE account_id = ?
                            ORDER BY created_at DESC
                            LIMIT ?
                            """,
                    this::mapOrder,
                    accountId,
                    limit
            );
        }
        return jdbcTemplate.query("""
                        SELECT *
                        FROM trade_order
                        WHERE account_id = ? AND status = ?
                        ORDER BY created_at DESC
                        LIMIT ?
                        """,
                this::mapOrder,
                accountId,
                status.name(),
                limit
        );
    }

    private Order mapOrder(ResultSet rs, int rowNum) throws SQLException {
        return new Order(
                new OrderId(UuidBytes.fromBytes(rs.getBytes("id"))),
                new AccountId(rs.getString("account_id")),
                Market.valueOf(rs.getString("market")),
                new Symbol(rs.getString("symbol")),
                OrderSide.valueOf(rs.getString("side")),
                OrderType.valueOf(rs.getString("order_type")),
                TimeInForce.valueOf(rs.getString("tif")),
                new OrderQuantity(rs.getLong("order_qty")),
                new OrderPrice(rs.getBigDecimal("limit_price")),
                OrderStatus.valueOf(rs.getString("status")),
                new OrderQuantity(rs.getLong("cum_qty")),
                new OrderQuantity(rs.getLong("leaves_qty")),
                ReconciliationStatus.valueOf(rs.getString("reconciliation_status")),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                toInstant(rs.getTimestamp("terminal_at"))
        );
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
