package com.trading.orderreliability.order.application;

import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.CancelOrderCommandPayload;
import com.trading.orderreliability.common.messaging.BrokerCommandPayloads.SubmitOrderCommandPayload;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.order.adapter.out.messaging.outbox.OutboxMessageRepository;
import com.trading.orderreliability.order.adapter.out.persistence.event.OrderEventRepository;
import com.trading.orderreliability.order.adapter.out.persistence.instruction.OrderInstructionRepository;
import com.trading.orderreliability.order.adapter.out.persistence.order.TradeOrderRepository;
import com.trading.orderreliability.order.application.command.CancelOrderCommand;
import com.trading.orderreliability.order.application.command.PlaceOrderCommand;
import com.trading.orderreliability.order.application.config.MarketState;
import com.trading.orderreliability.order.application.config.OrderServiceProperties;
import com.trading.orderreliability.order.application.exception.ActiveCancelConflictException;
import com.trading.orderreliability.order.application.exception.IdempotencyConflictException;
import com.trading.orderreliability.order.application.exception.OrderAccessDeniedException;
import com.trading.orderreliability.order.application.exception.OrderNotFoundException;
import com.trading.orderreliability.order.application.exception.OrderRequestRejectedException;
import com.trading.orderreliability.order.application.result.CancelOrderResult;
import com.trading.orderreliability.order.application.result.PlaceOrderResult;
import com.trading.orderreliability.order.application.support.HashingService;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.InstructionType;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderInstruction;
import com.trading.orderreliability.order.domain.model.OrderInstructionId;
import com.trading.orderreliability.order.domain.model.OrderInstructionStatus;
import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.state.OrderStateMachine;
import com.trading.orderreliability.order.domain.state.OrderTransition;
import com.trading.orderreliability.order.domain.state.OrderTransitionRequest;
import com.trading.orderreliability.order.domain.state.OrderTransitionTrigger;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderApplicationService {

    private final TradeOrderRepository orderRepository;
    private final OrderInstructionRepository instructionRepository;
    private final OrderEventRepository eventRepository;
    private final OutboxMessageRepository outboxMessageRepository;
    private final OrderStateMachine stateMachine;
    private final UuidV7Generator uuidGenerator;
    private final HashingService hashingService;
    private final OrderServiceProperties properties;
    private final TransactionTemplate duplicateResolutionTransaction;
    private final Clock clock;

    public OrderApplicationService(
            TradeOrderRepository orderRepository,
            OrderInstructionRepository instructionRepository,
            OrderEventRepository eventRepository,
            OutboxMessageRepository outboxMessageRepository,
            OrderStateMachine stateMachine,
            UuidV7Generator uuidGenerator,
            HashingService hashingService,
            OrderServiceProperties properties,
            PlatformTransactionManager transactionManager
    ) {
        this.orderRepository = orderRepository;
        this.instructionRepository = instructionRepository;
        this.eventRepository = eventRepository;
        this.outboxMessageRepository = outboxMessageRepository;
        this.stateMachine = stateMachine;
        this.uuidGenerator = uuidGenerator;
        this.hashingService = hashingService;
        this.properties = properties;
        this.duplicateResolutionTransaction = new TransactionTemplate(transactionManager);
        this.duplicateResolutionTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.duplicateResolutionTransaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.clock = Clock.systemUTC();
    }

    @Transactional
    public PlaceOrderResult createOrder(PlaceOrderCommand command) {
        PlaceOrderIdempotencyPayload idempotencyPayload = PlaceOrderIdempotencyPayload.from(command);
        String payloadJson = hashingService.canonicalJson(idempotencyPayload);
        String payloadHash = hashingService.sha256(payloadJson);
        Optional<OrderInstruction> existingInstruction = instructionRepository.findByIdempotencyKey(
                command.accountId().value(),
                InstructionType.PLACE,
                command.clientOrderId()
        );

        if (existingInstruction.isPresent()) {
            OrderInstruction instruction = existingInstruction.get();
            if (!instruction.requestPayloadHash().equals(payloadHash)) {
                throw new IdempotencyConflictException("PLACE instruction payload does not match previous request");
            }
            Order existingOrder = orderRepository.findById(instruction.orderId())
                    .orElseThrow(() -> new OrderNotFoundException(instruction.orderId().value()));
            return new PlaceOrderResult(existingOrder, false);
        }

        if (properties.marketState() != MarketState.OPEN) {
            throw new OrderRequestRejectedException("MARKET_CLOSED", "Market is closed");
        }

        Instant now = clock.instant();
        Order order = Order.createPendingAck(
                new OrderId(uuidGenerator.generate()),
                command.accountId(),
                command.market(),
                command.symbol(),
                command.side(),
                command.orderType(),
                command.timeInForce(),
                command.orderQty(),
                command.limitPrice(),
                now
        );
        OrderInstruction instruction = new OrderInstruction(
                new OrderInstructionId(uuidGenerator.generate()),
                order.orderId(),
                order.accountId(),
                InstructionType.PLACE,
                command.clientOrderId(),
                OrderInstructionStatus.REQUESTED,
                0,
                payloadHash,
                null,
                null,
                command.traceId(),
                now,
                now,
                null
        );

        try {
            instructionRepository.insert(instruction, payloadJson);
        } catch (DuplicateKeyException ignored) {
            return resolveConcurrentPlaceRetry(command, payloadHash);
        }
        orderRepository.insert(order);
        eventRepository.insert(
                uuidGenerator.generate(),
                order.orderId(),
                "OrderCreated",
                "USER",
                null,
                payloadHash,
                command.traceId(),
                payloadJson,
                now
        );
        eventRepository.insert(
                uuidGenerator.generate(),
                order.orderId(),
                "PlaceInstructionCreated",
                "USER",
                null,
                payloadHash,
                command.traceId(),
                payloadJson,
                now
        );
        outboxMessageRepository.appendBrokerCommand(
                uuidGenerator.generate(),
                order.orderId().value(),
                MessageTypes.SUBMIT_ORDER_COMMAND,
                new SubmitOrderCommandPayload(
                        order.orderId().value(),
                        order.accountId().value(),
                        order.market().name(),
                        order.symbol().value(),
                        order.side().name(),
                        order.orderType().name(),
                        order.timeInForce().name(),
                        order.orderQty().value(),
                        order.limitPrice().value().toPlainString()
                ),
                command.traceId(),
                now
        );

        return new PlaceOrderResult(order, true);
    }

    private PlaceOrderResult resolveConcurrentPlaceRetry(PlaceOrderCommand command, String payloadHash) {
        return Objects.requireNonNull(duplicateResolutionTransaction.execute(status -> {
            OrderInstruction instruction = instructionRepository.findByIdempotencyKey(
                            command.accountId().value(),
                            InstructionType.PLACE,
                            command.clientOrderId()
                    )
                    .orElseThrow(() -> new IdempotencyConflictException("PLACE instruction key was concurrently inserted but is not visible"));
            if (!instruction.requestPayloadHash().equals(payloadHash)) {
                throw new IdempotencyConflictException("PLACE instruction payload does not match previous request");
            }
            Order existingOrder = orderRepository.findById(instruction.orderId())
                    .orElseThrow(() -> new OrderNotFoundException(instruction.orderId().value()));
            return new PlaceOrderResult(existingOrder, false);
        }));
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID orderId) {
        return orderRepository.findById(new OrderId(orderId))
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    @Transactional(readOnly = true)
    public List<Order> listOrders(String accountId, OrderStatus status, int limit) {
        return orderRepository.findByAccount(accountId, status, Math.min(Math.max(limit, 1), 100));
    }

    @Transactional
    public CancelOrderResult cancelOrder(UUID orderIdValue, CancelOrderCommand command) {
        OrderId orderId = new OrderId(orderIdValue);
        Order order = orderRepository.findByIdForUpdate(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderIdValue));

        if (!order.accountId().equals(command.accountId())) {
            throw new OrderAccessDeniedException(orderIdValue);
        }

        CancelOrderIdempotencyPayload idempotencyPayload = CancelOrderIdempotencyPayload.from(orderId, command);
        String payloadJson = hashingService.canonicalJson(idempotencyPayload);
        String payloadHash = hashingService.sha256(payloadJson);
        Optional<OrderInstruction> existingCancelByKey = instructionRepository.findByIdempotencyKey(
                command.accountId().value(),
                InstructionType.CANCEL,
                command.clientCancelRequestId()
        );
        if (existingCancelByKey.isPresent()) {
            OrderInstruction instruction = existingCancelByKey.get();
            if (!instruction.orderId().equals(orderId)) {
                throw new IdempotencyConflictException("CANCEL instruction key is already used for another order");
            }
            if (!instruction.requestPayloadHash().equals(payloadHash)) {
                throw new IdempotencyConflictException("CANCEL instruction payload does not match previous request");
            }
            return new CancelOrderResult(orderRepository.findById(orderId).orElse(order), instruction);
        }

        Optional<OrderInstruction> activeCancel = instructionRepository.findActiveCancel(orderId);
        if (activeCancel.isPresent()) {
            throw new ActiveCancelConflictException(orderIdValue);
        }

        if (order.status() == OrderStatus.UNKNOWN || order.status().isTerminal()) {
            throw new OrderRequestRejectedException("ORDER_NOT_CANCELABLE", "Order cannot be canceled in status " + order.status());
        }

        Instant now = clock.instant();
        OrderTransition transition = stateMachine.transition(
                order,
                OrderTransitionRequest.of(OrderTransitionTrigger.CANCEL_REQUESTED, now, "User cancel requested")
        );
        Order nextOrder = transition.nextOrder();
        OrderInstruction instruction = new OrderInstruction(
                new OrderInstructionId(uuidGenerator.generate()),
                orderId,
                order.accountId(),
                InstructionType.CANCEL,
                command.clientCancelRequestId(),
                OrderInstructionStatus.REQUESTED,
                0,
                payloadHash,
                null,
                null,
                command.traceId(),
                now,
                now,
                null
        );

        try {
            instructionRepository.insert(instruction, payloadJson);
        } catch (DuplicateKeyException ignored) {
            return resolveConcurrentCancelRetry(orderId, command, payloadHash);
        }
        orderRepository.updateState(nextOrder);
        eventRepository.insert(
                uuidGenerator.generate(),
                orderId,
                "CancelInstructionCreated",
                "USER",
                null,
                payloadHash,
                command.traceId(),
                payloadJson,
                now
        );
        eventRepository.insert(
                uuidGenerator.generate(),
                orderId,
                "OrderStatusChanged",
                "USER",
                null,
                payloadHash,
                command.traceId(),
                "{\"previousStatus\":\"%s\",\"currentStatus\":\"%s\"}".formatted(order.status(), nextOrder.status()),
                now
        );
        outboxMessageRepository.appendBrokerCommand(
                uuidGenerator.generate(),
                orderId.value(),
                MessageTypes.CANCEL_ORDER_COMMAND,
                new CancelOrderCommandPayload(orderId.value()),
                command.traceId(),
                now
        );

        return new CancelOrderResult(nextOrder, instruction);
    }

    private CancelOrderResult resolveConcurrentCancelRetry(
            OrderId orderId,
            CancelOrderCommand command,
            String payloadHash
    ) {
        return Objects.requireNonNull(duplicateResolutionTransaction.execute(status -> {
            OrderInstruction instruction = instructionRepository.findByIdempotencyKey(
                            command.accountId().value(),
                            InstructionType.CANCEL,
                            command.clientCancelRequestId()
                    )
                    .orElseThrow(() -> new IdempotencyConflictException("CANCEL instruction key was concurrently inserted but is not visible"));
            if (!instruction.orderId().equals(orderId)) {
                throw new IdempotencyConflictException("CANCEL instruction key is already used for another order");
            }
            if (!instruction.requestPayloadHash().equals(payloadHash)) {
                throw new IdempotencyConflictException("CANCEL instruction payload does not match previous request");
            }
            Order existingOrder = orderRepository.findById(orderId)
                    .orElseThrow(() -> new OrderNotFoundException(orderId.value()));
            return new CancelOrderResult(existingOrder, instruction);
        }));
    }

    private record PlaceOrderIdempotencyPayload(
            String clientOrderId,
            String accountId,
            String market,
            String symbol,
            String side,
            String orderType,
            String timeInForce,
            long orderQty,
            String limitPrice
    ) {

        static PlaceOrderIdempotencyPayload from(PlaceOrderCommand command) {
            return new PlaceOrderIdempotencyPayload(
                    command.clientOrderId(),
                    command.accountId().value(),
                    command.market().name(),
                    command.symbol().value(),
                    command.side().name(),
                    command.orderType().name(),
                    command.timeInForce().name(),
                    command.orderQty().value(),
                    command.limitPrice().value().toPlainString()
            );
        }
    }

    private record CancelOrderIdempotencyPayload(
            String orderId,
            String accountId,
            String clientCancelRequestId
    ) {

        static CancelOrderIdempotencyPayload from(OrderId orderId, CancelOrderCommand command) {
            return new CancelOrderIdempotencyPayload(
                    orderId.value().toString(),
                    command.accountId().value(),
                    command.clientCancelRequestId()
            );
        }
    }
}
