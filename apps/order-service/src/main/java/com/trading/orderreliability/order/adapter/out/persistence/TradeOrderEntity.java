package com.trading.orderreliability.order.adapter.out.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "trade_order")
@Getter(AccessLevel.PACKAGE)
@Setter(AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
class TradeOrderEntity {

    @Id
    @Column(name = "id", columnDefinition = "BINARY(16)", nullable = false)
    private UUID id;

    @Column(name = "account_id", nullable = false, length = 64)
    private String accountId;

    @Column(name = "market", nullable = false, length = 16)
    private String market;

    @Column(name = "symbol", nullable = false, length = 32)
    private String symbol;

    @Column(name = "side", nullable = false, length = 8)
    private String side;

    @Column(name = "order_type", nullable = false, length = 16)
    private String orderType;

    @Column(name = "tif", nullable = false, length = 8)
    private String tif;

    @Column(name = "order_qty", nullable = false)
    private long orderQty;

    @Column(name = "limit_price", nullable = false, precision = 19, scale = 4)
    private BigDecimal limitPrice;

    @Column(name = "status", nullable = false, length = 32)
    private String status;

    @Column(name = "reconciliation_status", nullable = false, length = 32)
    private String reconciliationStatus;

    @Column(name = "cum_qty", nullable = false)
    private long cumQty;

    @Column(name = "leaves_qty", nullable = false)
    private long leavesQty;

    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "terminal_at")
    private Instant terminalAt;

    @Builder(access = AccessLevel.PACKAGE)
    private TradeOrderEntity(
            UUID id,
            String accountId,
            String market,
            String symbol,
            String side,
            String orderType,
            String tif,
            long orderQty,
            BigDecimal limitPrice,
            String status,
            String reconciliationStatus,
            long cumQty,
            long leavesQty,
            long version,
            Instant createdAt,
            Instant updatedAt,
            Instant terminalAt
    ) {
        this.id = id;
        this.accountId = accountId;
        this.market = market;
        this.symbol = symbol;
        this.side = side;
        this.orderType = orderType;
        this.tif = tif;
        this.orderQty = orderQty;
        this.limitPrice = limitPrice;
        this.status = status;
        this.reconciliationStatus = reconciliationStatus;
        this.cumQty = cumQty;
        this.leavesQty = leavesQty;
        this.version = version;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.terminalAt = terminalAt;
    }
}
