package com.trading.orderreliability.order.adapter.out.persistence;

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

import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class TradeOrderRepository {

    private final JpaTradeOrderRepository jpaRepository;

    public TradeOrderRepository(JpaTradeOrderRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    public void insert(Order order) {
        jpaRepository.save(toEntity(order));
    }

    @Transactional
    public void updateState(Order order) {
        TradeOrderEntity entity = jpaRepository.findById(order.orderId().value())
                .orElseThrow(() -> new IllegalStateException("Order not found for update: " + order.orderId().value()));
        entity.setStatus(order.status().name());
        entity.setReconciliationStatus(order.reconciliationStatus().name());
        entity.setCumQty(order.cumQty().value());
        entity.setLeavesQty(order.leavesQty().value());
        entity.setVersion(entity.getVersion() + 1);
        entity.setUpdatedAt(order.updatedAt());
        entity.setTerminalAt(order.terminalAt());
        jpaRepository.save(entity);
    }

    public Optional<Order> findById(OrderId orderId) {
        return jpaRepository.findById(orderId.value()).map(this::toDomain);
    }

    public Optional<Order> findByIdForUpdate(OrderId orderId) {
        return jpaRepository.findByIdForUpdate(orderId.value()).map(this::toDomain);
    }

    public List<Order> findByAccount(String accountId, OrderStatus status, int limit) {
        PageRequest pageRequest = PageRequest.of(0, limit);
        if (status == null) {
            return jpaRepository.findByAccountIdOrderByCreatedAtDesc(accountId, pageRequest)
                    .stream()
                    .map(this::toDomain)
                    .toList();
        }
        return jpaRepository.findByAccountIdAndStatusOrderByCreatedAtDesc(accountId, status.name(), pageRequest)
                .stream()
                .map(this::toDomain)
                .toList();
    }

    private TradeOrderEntity toEntity(Order order) {
        return TradeOrderEntity.builder()
                .id(order.orderId().value())
                .accountId(order.accountId().value())
                .market(order.market().name())
                .symbol(order.symbol().value())
                .side(order.side().name())
                .orderType(order.orderType().name())
                .tif(order.timeInForce().name())
                .orderQty(order.orderQty().value())
                .limitPrice(order.limitPrice().value())
                .status(order.status().name())
                .reconciliationStatus(order.reconciliationStatus().name())
                .cumQty(order.cumQty().value())
                .leavesQty(order.leavesQty().value())
                .version(0L)
                .createdAt(order.createdAt())
                .updatedAt(order.updatedAt())
                .terminalAt(order.terminalAt())
                .build();
    }

    private Order toDomain(TradeOrderEntity entity) {
        return new Order(
                new OrderId(entity.getId()),
                new AccountId(entity.getAccountId()),
                Market.valueOf(entity.getMarket()),
                new Symbol(entity.getSymbol()),
                OrderSide.valueOf(entity.getSide()),
                OrderType.valueOf(entity.getOrderType()),
                TimeInForce.valueOf(entity.getTif()),
                new OrderQuantity(entity.getOrderQty()),
                new OrderPrice(entity.getLimitPrice()),
                OrderStatus.valueOf(entity.getStatus()),
                new OrderQuantity(entity.getCumQty()),
                new OrderQuantity(entity.getLeavesQty()),
                ReconciliationStatus.valueOf(entity.getReconciliationStatus()),
                entity.getCreatedAt(),
                entity.getUpdatedAt(),
                entity.getTerminalAt()
        );
    }
}
