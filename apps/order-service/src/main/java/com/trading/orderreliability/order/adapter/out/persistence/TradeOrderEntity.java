package com.trading.orderreliability.order.adapter.out.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "trade_order")
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

    protected TradeOrderEntity() {
    }

    UUID getId() {
        return id;
    }

    void setId(UUID id) {
        this.id = id;
    }

    String getAccountId() {
        return accountId;
    }

    void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    String getMarket() {
        return market;
    }

    void setMarket(String market) {
        this.market = market;
    }

    String getSymbol() {
        return symbol;
    }

    void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    String getSide() {
        return side;
    }

    void setSide(String side) {
        this.side = side;
    }

    String getOrderType() {
        return orderType;
    }

    void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    String getTif() {
        return tif;
    }

    void setTif(String tif) {
        this.tif = tif;
    }

    long getOrderQty() {
        return orderQty;
    }

    void setOrderQty(long orderQty) {
        this.orderQty = orderQty;
    }

    BigDecimal getLimitPrice() {
        return limitPrice;
    }

    void setLimitPrice(BigDecimal limitPrice) {
        this.limitPrice = limitPrice;
    }

    String getStatus() {
        return status;
    }

    void setStatus(String status) {
        this.status = status;
    }

    String getReconciliationStatus() {
        return reconciliationStatus;
    }

    void setReconciliationStatus(String reconciliationStatus) {
        this.reconciliationStatus = reconciliationStatus;
    }

    long getCumQty() {
        return cumQty;
    }

    void setCumQty(long cumQty) {
        this.cumQty = cumQty;
    }

    long getLeavesQty() {
        return leavesQty;
    }

    void setLeavesQty(long leavesQty) {
        this.leavesQty = leavesQty;
    }

    long getVersion() {
        return version;
    }

    void setVersion(long version) {
        this.version = version;
    }

    Instant getCreatedAt() {
        return createdAt;
    }

    void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    Instant getUpdatedAt() {
        return updatedAt;
    }

    void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    Instant getTerminalAt() {
        return terminalAt;
    }

    void setTerminalAt(Instant terminalAt) {
        this.terminalAt = terminalAt;
    }
}
