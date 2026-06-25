package com.trading.orderreliability.simulator.domain;

import com.trading.orderreliability.broker.protocol.BrokerMessages.OrderRequest;

import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class BrokerSimulatorState {

    private static final String REJECT_CODE = "MARKET_CLOSED";
    private static final String CANCEL_REJECT_CODE = "TOO_LATE_CANCEL";

    private final Clock clock;
    private final AtomicReference<SimulatorScenario> scenario = new AtomicReference<>(SimulatorScenario.ACK_SUCCESS);
    private final AtomicLong brokerOrderSequence = new AtomicLong();
    private final AtomicLong wireMessageSequence = new AtomicLong();
    private final ConcurrentMap<UUID, SimulatorOrder> ordersById = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UUID> orderIdsByBrokerOrderId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> wireMessageIdsByLogicalEvent = new ConcurrentHashMap<>();

    public BrokerSimulatorState() {
        this(Clock.systemUTC());
    }

    BrokerSimulatorState(Clock clock) {
        this.clock = clock;
    }

    public SimulatorScenario scenario() {
        return scenario.get();
    }

    public SimulatorScenario setScenario(SimulatorScenario nextScenario) {
        scenario.set(nextScenario);
        return nextScenario;
    }

    public SimulatorOrder accept(OrderRequest request) {
        String brokerOrderId = nextBrokerOrderId();
        SimulatorOrder order = new SimulatorOrder(
                request.header().orderId(),
                brokerOrderId,
                request.accountId(),
                request.symbol(),
                traceIdOrGenerated(request.header().traceId(), request.header().orderId()),
                request.orderQty(),
                0,
                request.orderQty(),
                SimulatorOrderStatus.ACCEPTED,
                "",
                Instant.now(clock)
        );
        ordersById.put(order.orderId(), order);
        orderIdsByBrokerOrderId.put(brokerOrderId, order.orderId());
        return order;
    }

    public SimulatorOrder reject(OrderRequest request) {
        SimulatorOrder order = new SimulatorOrder(
                request.header().orderId(),
                "",
                request.accountId(),
                request.symbol(),
                traceIdOrGenerated(request.header().traceId(), request.header().orderId()),
                request.orderQty(),
                0,
                0,
                SimulatorOrderStatus.REJECTED,
                REJECT_CODE,
                Instant.now(clock)
        );
        ordersById.put(order.orderId(), order);
        return order;
    }

    public Optional<SimulatorOrder> findByOrderId(UUID orderId) {
        return Optional.ofNullable(ordersById.get(orderId));
    }

    public Optional<SimulatorOrder> findByBrokerOrderId(String brokerOrderId) {
        if (brokerOrderId == null || brokerOrderId.isBlank()) {
            return Optional.empty();
        }
        UUID orderId = orderIdsByBrokerOrderId.get(brokerOrderId);
        if (orderId == null) {
            return Optional.empty();
        }
        return findByOrderId(orderId);
    }

    public Optional<SimulatorOrder> findForStatusQuery(UUID orderId, String brokerOrderId) {
        return findByBrokerOrderId(brokerOrderId).or(() -> findByOrderId(orderId));
    }

    public SimulatorOrder applyFill(UUID orderId, long lastFillQty) {
        if (lastFillQty <= 0) {
            throw new IllegalArgumentException("lastFillQty must be positive");
        }
        return ordersById.compute(orderId, (ignored, current) -> {
            if (current == null) {
                throw new IllegalArgumentException("simulator order not found");
            }
            if (current.status() == SimulatorOrderStatus.REJECTED
                    || current.status() == SimulatorOrderStatus.FILLED
                    || current.status() == SimulatorOrderStatus.CANCELED) {
                throw new IllegalStateException("fill requires an active accepted order");
            }
            long nextCumQty = current.cumQty() + lastFillQty;
            if (nextCumQty > current.orderQty()) {
                throw new IllegalArgumentException("fill quantity exceeds remaining quantity");
            }
            long nextLeavesQty = current.orderQty() - nextCumQty;
            SimulatorOrderStatus nextStatus = nextLeavesQty == 0
                    ? SimulatorOrderStatus.FILLED
                    : SimulatorOrderStatus.PARTIALLY_FILLED;
            return new SimulatorOrder(
                    current.orderId(),
                    current.brokerOrderId(),
                    current.accountId(),
                    current.symbol(),
                    current.traceId(),
                    current.orderQty(),
                    nextCumQty,
                    nextLeavesQty,
                    nextStatus,
                    current.rejectCode(),
                    Instant.now(clock)
            );
        });
    }

    public SimulatorOrder cancel(UUID orderId, String brokerOrderId) {
        SimulatorOrder order = findForStatusQuery(orderId, brokerOrderId)
                .orElseThrow(() -> new IllegalArgumentException("simulator order not found"));
        return ordersById.compute(order.orderId(), (ignored, current) -> {
            if (current == null) {
                throw new IllegalArgumentException("simulator order not found");
            }
            if (current.status() == SimulatorOrderStatus.REJECTED
                    || current.status() == SimulatorOrderStatus.FILLED
                    || current.status() == SimulatorOrderStatus.CANCELED) {
                throw new IllegalStateException("cancel requires an active accepted order");
            }
            return new SimulatorOrder(
                    current.orderId(),
                    current.brokerOrderId(),
                    current.accountId(),
                    current.symbol(),
                    current.traceId(),
                    current.orderQty(),
                    current.cumQty(),
                    0,
                    SimulatorOrderStatus.CANCELED,
                    current.rejectCode(),
                    Instant.now(clock)
            );
        });
    }

    public SimulatorOrder rejectCancel(UUID orderId, String brokerOrderId) {
        SimulatorOrder order = findForStatusQuery(orderId, brokerOrderId)
                .orElseThrow(() -> new IllegalArgumentException("simulator order not found"));
        return ordersById.compute(order.orderId(), (ignored, current) -> {
            if (current == null) {
                throw new IllegalArgumentException("simulator order not found");
            }
            return new SimulatorOrder(
                    current.orderId(),
                    current.brokerOrderId(),
                    current.accountId(),
                    current.symbol(),
                    current.traceId(),
                    current.orderQty(),
                    current.cumQty(),
                    current.leavesQty(),
                    current.status(),
                    CANCEL_REJECT_CODE,
                    Instant.now(clock)
            );
        });
    }

    public String wireMessageIdForLogicalEvent(String logicalEventKey) {
        return wireMessageIdsByLogicalEvent.computeIfAbsent(
                logicalEventKey,
                ignored -> "W-SIM-%06d".formatted(wireMessageSequence.incrementAndGet())
        );
    }

    public List<SimulatorOrder> orders() {
        return new ArrayList<>(ordersById.values());
    }

    public void reset() {
        ordersById.clear();
        orderIdsByBrokerOrderId.clear();
        wireMessageIdsByLogicalEvent.clear();
        brokerOrderSequence.set(0);
        wireMessageSequence.set(0);
        scenario.set(SimulatorScenario.ACK_SUCCESS);
    }

    private String nextBrokerOrderId() {
        return "BRK-SIM-%06d".formatted(brokerOrderSequence.incrementAndGet());
    }

    private static String traceIdOrGenerated(String traceId, UUID orderId) {
        if (traceId != null && !traceId.isBlank()) {
            return traceId;
        }
        return "trace-simulator-" + orderId;
    }
}
