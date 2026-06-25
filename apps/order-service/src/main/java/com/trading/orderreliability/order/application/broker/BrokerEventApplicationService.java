package com.trading.orderreliability.order.application.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.orderreliability.common.id.UuidV7Generator;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerCancelAcknowledgedPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerCancelRejectedPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderAcknowledgedPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderFilledPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderPartiallyFilledPayload;
import com.trading.orderreliability.common.messaging.BrokerEventPayloads.BrokerOrderRejectedPayload;
import com.trading.orderreliability.common.messaging.MessageEnvelope;
import com.trading.orderreliability.common.messaging.MessageTypes;
import com.trading.orderreliability.common.messaging.MessagingTopics;
import com.trading.orderreliability.order.adapter.in.web.OrderStatusSsePublisher;
import com.trading.orderreliability.order.adapter.out.messaging.parking.MessageParkingLot;
import com.trading.orderreliability.order.adapter.out.persistence.event.OrderEventRepository;
import com.trading.orderreliability.order.adapter.out.persistence.instruction.OrderInstructionRepository;
import com.trading.orderreliability.order.adapter.out.persistence.order.TradeOrderRepository;
import com.trading.orderreliability.order.application.exception.OrderNotFoundException;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderId;
import com.trading.orderreliability.order.domain.model.OrderInstructionStatus;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.state.OrderStateMachine;
import com.trading.orderreliability.order.domain.state.OrderTransition;
import com.trading.orderreliability.order.domain.state.OrderTransitionRequest;
import com.trading.orderreliability.order.domain.state.OrderTransitionTrigger;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class BrokerEventApplicationService {

    private static final String CONSUMER_NAME = "order-service-broker-event-consumer";

    private final TradeOrderRepository orderRepository;
    private final OrderInstructionRepository instructionRepository;
    private final OrderEventRepository eventRepository;
    private final MessageParkingLot parkingLot;
    private final OrderStateMachine stateMachine;
    private final UuidV7Generator uuidGenerator;
    private final ObjectMapper objectMapper;
    private final OrderStatusSsePublisher ssePublisher;
    private final Clock clock = Clock.systemUTC();

    public BrokerEventApplicationService(
            TradeOrderRepository orderRepository,
            OrderInstructionRepository instructionRepository,
            OrderEventRepository eventRepository,
            MessageParkingLot parkingLot,
            OrderStateMachine stateMachine,
            UuidV7Generator uuidGenerator,
            ObjectMapper objectMapper,
            OrderStatusSsePublisher ssePublisher
    ) {
        this.orderRepository = orderRepository;
        this.instructionRepository = instructionRepository;
        this.eventRepository = eventRepository;
        this.parkingLot = parkingLot;
        this.stateMachine = stateMachine;
        this.uuidGenerator = uuidGenerator;
        this.objectMapper = objectMapper;
        this.ssePublisher = ssePublisher;
    }

    @Transactional
    public BrokerEventApplyResult apply(MessageEnvelope<JsonNode> envelope) {
        BrokerEvent event = toBrokerEvent(envelope);
        Optional<String> existingPayloadHash = eventRepository.findPayloadHashByDedupKey(event.dedupKey());
        if (existingPayloadHash.isPresent()) {
            if (existingPayloadHash.get().equals(event.payloadHash())) {
                return BrokerEventApplyResult.DUPLICATE_SKIPPED;
            }
            parkingLot.parkEnvelope(
                    MessagingTopics.BROKER_EVENT,
                    CONSUMER_NAME,
                    envelope,
                    MessageParkingLot.ERROR_CODE_BROKER_EVENT_DEDUP_PAYLOAD_MISMATCH,
                    "brokerEventDedupKey payload hash mismatch: " + event.dedupKey(),
                    clock.instant()
            );
            return BrokerEventApplyResult.PAYLOAD_MISMATCH_PARKED;
        }

        OrderId orderId = new OrderId(event.orderId());
        Order current = orderRepository.findByIdForUpdate(orderId)
                .orElseThrow(() -> new OrderNotFoundException(event.orderId()));
        validateFillConsistency(current, event);
        OrderTransition transition = stateMachine.transition(current, event.transitionRequest());
        Order next = transition.nextOrder();
        orderRepository.updateState(next);
        resolveInstructions(orderId, event, transition, next.updatedAt());
        insertBrokerEvent(envelope, event);
        publishAfterCommit(transition, next, event.occurredAt());
        return BrokerEventApplyResult.APPLIED;
    }

    private void resolveInstructions(OrderId orderId, BrokerEvent event, OrderTransition transition, Instant resolvedAt) {
        switch (event.trigger()) {
            case BROKER_ORDER_REJECTED -> {
                instructionRepository.resolveRequestedPlaceInstruction(
                        orderId,
                        OrderInstructionStatus.REJECTED,
                        "BROKER_ORDER_REJECTED",
                        event.resultMessage(),
                        resolvedAt
                );
                instructionRepository.resolveRequestedCancelInstruction(
                        orderId,
                        OrderInstructionStatus.NOT_APPLIED,
                        "BROKER_ORDER_REJECTED_BEFORE_CANCEL",
                        event.resultMessage(),
                        resolvedAt
                );
            }
            case BROKER_ORDER_ACKNOWLEDGED, BROKER_ORDER_PARTIALLY_FILLED ->
                instructionRepository.resolveRequestedPlaceInstruction(
                        orderId,
                        OrderInstructionStatus.COMPLETED,
                        "BROKER_ORDER_ACCEPTED",
                        event.resultMessage(),
                        resolvedAt
                );
            case BROKER_ORDER_FILLED -> {
                instructionRepository.resolveRequestedPlaceInstruction(
                            orderId,
                            OrderInstructionStatus.COMPLETED,
                            "BROKER_ORDER_ACCEPTED",
                            event.resultMessage(),
                            resolvedAt
                );
                instructionRepository.resolveRequestedCancelInstruction(
                        orderId,
                        OrderInstructionStatus.NOT_APPLIED,
                        "ORDER_FILLED_BEFORE_CANCEL",
                        event.resultMessage(),
                        resolvedAt
                );
            }
            case BROKER_CANCEL_ACKNOWLEDGED -> {
                if (transition.changed()) {
                    instructionRepository.resolveRequestedCancelInstruction(
                            orderId,
                            OrderInstructionStatus.COMPLETED,
                            "BROKER_CANCEL_ACK",
                            event.resultMessage(),
                            resolvedAt
                    );
                } else {
                    instructionRepository.resolveRequestedCancelInstruction(
                            orderId,
                            OrderInstructionStatus.NOT_APPLIED,
                            "BROKER_CANCEL_ACK_AFTER_TERMINAL",
                            event.resultMessage(),
                            resolvedAt
                    );
                }
            }
            case BROKER_CANCEL_REJECTED -> {
                if (transition.changed()) {
                    instructionRepository.resolveRequestedCancelInstruction(
                            orderId,
                            OrderInstructionStatus.REJECTED,
                            "BROKER_CANCEL_REJECTED",
                            event.resultMessage(),
                            resolvedAt
                    );
                } else {
                    instructionRepository.resolveRequestedCancelInstruction(
                            orderId,
                            OrderInstructionStatus.NOT_APPLIED,
                            "BROKER_CANCEL_REJECTED_AFTER_TERMINAL",
                            event.resultMessage(),
                            resolvedAt
                    );
                }
            }
            default -> {
            }
        }
    }

    private void insertBrokerEvent(MessageEnvelope<JsonNode> envelope, BrokerEvent event) {
        try {
            eventRepository.insert(
                    uuidGenerator.generate(),
                    new OrderId(event.orderId()),
                    envelope.messageType() + "Applied",
                    "BROKER",
                    envelope.messageId(),
                    event.dedupKey(),
                    event.payloadHash(),
                    envelope.traceId(),
                    objectMapper.writeValueAsString(envelope.payload()),
                    event.occurredAt()
            );
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize broker event payload", e);
        }
    }

    private void publishAfterCommit(OrderTransition transition, Order next, Instant occurredAt) {
        if (!transition.changed()) {
            return;
        }
        OrderStatusChangedNotification notification = new OrderStatusChangedNotification(
                next.orderId().value(),
                transition.previousStatus(),
                transition.nextStatus(),
                next.cumQty().value(),
                next.leavesQty().value(),
                occurredAt
        );
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                ssePublisher.publish(notification);
            }
        });
    }

    private static void validateFillConsistency(Order current, BrokerEvent event) {
        if (event.trigger() != OrderTransitionTrigger.BROKER_ORDER_PARTIALLY_FILLED
                && event.trigger() != OrderTransitionTrigger.BROKER_ORDER_FILLED) {
            return;
        }
        long orderQty = current.orderQty().value();
        long cumQty = event.cumQty();
        long leavesQty = event.leavesQty();
        long lastFillQty = event.lastFillQty();
        if (lastFillQty <= 0) {
            throw new IllegalArgumentException("lastFillQty must be positive");
        }
        if (cumQty <= 0 || cumQty > orderQty) {
            throw new IllegalArgumentException("cumQty must be between 1 and orderQty");
        }
        if (leavesQty < 0 || cumQty + leavesQty != orderQty) {
            throw new IllegalArgumentException("cumQty and leavesQty must match orderQty");
        }
        if (lastFillQty > cumQty) {
            throw new IllegalArgumentException("lastFillQty must not exceed cumQty");
        }
        if (event.trigger() == OrderTransitionTrigger.BROKER_ORDER_PARTIALLY_FILLED && leavesQty == 0) {
            throw new IllegalArgumentException("partial fill must leave remaining quantity");
        }
        if (event.trigger() == OrderTransitionTrigger.BROKER_ORDER_FILLED && leavesQty != 0) {
            throw new IllegalArgumentException("full fill must have zero leavesQty");
        }
    }

    private BrokerEvent toBrokerEvent(MessageEnvelope<JsonNode> envelope) {
        return switch (envelope.messageType()) {
            case MessageTypes.BROKER_ORDER_ACKNOWLEDGED -> acknowledged(envelope);
            case MessageTypes.BROKER_ORDER_REJECTED -> rejected(envelope);
            case MessageTypes.BROKER_ORDER_PARTIALLY_FILLED -> partiallyFilled(envelope);
            case MessageTypes.BROKER_ORDER_FILLED -> filled(envelope);
            case MessageTypes.BROKER_CANCEL_ACKNOWLEDGED -> cancelAcknowledged(envelope);
            case MessageTypes.BROKER_CANCEL_REJECTED -> cancelRejected(envelope);
            default -> throw new IllegalArgumentException("Unsupported broker event type: " + envelope.messageType());
        };
    }

    private BrokerEvent acknowledged(MessageEnvelope<JsonNode> envelope) {
        BrokerOrderAcknowledgedPayload payload = objectMapper.convertValue(envelope.payload(), BrokerOrderAcknowledgedPayload.class);
        return new BrokerEvent(
                payload.orderId(),
                payload.brokerEventDedupKey(),
                payload.payloadHash(),
                payload.brokerEventTime(),
                OrderTransitionTrigger.BROKER_ORDER_ACKNOWLEDGED,
                OrderTransitionRequest.of(
                        OrderTransitionTrigger.BROKER_ORDER_ACKNOWLEDGED,
                        payload.brokerEventTime(),
                        "Broker order acknowledged"
                ),
                "brokerOrderId=" + payload.brokerOrderId(),
                0,
                0,
                0
        );
    }

    private BrokerEvent rejected(MessageEnvelope<JsonNode> envelope) {
        BrokerOrderRejectedPayload payload = objectMapper.convertValue(envelope.payload(), BrokerOrderRejectedPayload.class);
        return new BrokerEvent(
                payload.orderId(),
                payload.brokerEventDedupKey(),
                payload.payloadHash(),
                payload.brokerEventTime(),
                OrderTransitionTrigger.BROKER_ORDER_REJECTED,
                OrderTransitionRequest.of(
                        OrderTransitionTrigger.BROKER_ORDER_REJECTED,
                        payload.brokerEventTime(),
                        "Broker order rejected"
                ),
                payload.rejectCode() + ": " + payload.rejectMessage(),
                0,
                0,
                0
        );
    }

    private BrokerEvent partiallyFilled(MessageEnvelope<JsonNode> envelope) {
        BrokerOrderPartiallyFilledPayload payload = objectMapper.convertValue(envelope.payload(), BrokerOrderPartiallyFilledPayload.class);
        return new BrokerEvent(
                payload.orderId(),
                payload.brokerEventDedupKey(),
                payload.payloadHash(),
                payload.brokerEventTime(),
                OrderTransitionTrigger.BROKER_ORDER_PARTIALLY_FILLED,
                OrderTransitionRequest.fill(
                        OrderTransitionTrigger.BROKER_ORDER_PARTIALLY_FILLED,
                        new OrderQuantity(payload.cumQty()),
                        payload.brokerEventTime(),
                        "Broker order partially filled"
                ),
                "executionId=" + payload.executionId(),
                payload.lastFillQty(),
                payload.cumQty(),
                payload.leavesQty()
        );
    }

    private BrokerEvent filled(MessageEnvelope<JsonNode> envelope) {
        BrokerOrderFilledPayload payload = objectMapper.convertValue(envelope.payload(), BrokerOrderFilledPayload.class);
        return new BrokerEvent(
                payload.orderId(),
                payload.brokerEventDedupKey(),
                payload.payloadHash(),
                payload.brokerEventTime(),
                OrderTransitionTrigger.BROKER_ORDER_FILLED,
                OrderTransitionRequest.fill(
                        OrderTransitionTrigger.BROKER_ORDER_FILLED,
                        new OrderQuantity(payload.cumQty()),
                        payload.brokerEventTime(),
                        "Broker order filled"
                ),
                "executionId=" + payload.executionId(),
                payload.lastFillQty(),
                payload.cumQty(),
                payload.leavesQty()
        );
    }

    private BrokerEvent cancelAcknowledged(MessageEnvelope<JsonNode> envelope) {
        BrokerCancelAcknowledgedPayload payload = objectMapper.convertValue(envelope.payload(), BrokerCancelAcknowledgedPayload.class);
        return new BrokerEvent(
                payload.orderId(),
                payload.brokerEventDedupKey(),
                payload.payloadHash(),
                payload.brokerEventTime(),
                OrderTransitionTrigger.BROKER_CANCEL_ACKNOWLEDGED,
                OrderTransitionRequest.of(
                        OrderTransitionTrigger.BROKER_CANCEL_ACKNOWLEDGED,
                        payload.brokerEventTime(),
                        "Broker cancel acknowledged"
                ),
                "brokerOrderId=" + payload.brokerOrderId(),
                0,
                0,
                0
        );
    }

    private BrokerEvent cancelRejected(MessageEnvelope<JsonNode> envelope) {
        BrokerCancelRejectedPayload payload = objectMapper.convertValue(envelope.payload(), BrokerCancelRejectedPayload.class);
        return new BrokerEvent(
                payload.orderId(),
                payload.brokerEventDedupKey(),
                payload.payloadHash(),
                payload.brokerEventTime(),
                OrderTransitionTrigger.BROKER_CANCEL_REJECTED,
                OrderTransitionRequest.of(
                        OrderTransitionTrigger.BROKER_CANCEL_REJECTED,
                        payload.brokerEventTime(),
                        "Broker cancel rejected"
                ),
                payload.rejectCode() + ": " + payload.rejectMessage(),
                0,
                0,
                0
        );
    }

    private record BrokerEvent(
            java.util.UUID orderId,
            String dedupKey,
            String payloadHash,
            Instant occurredAt,
            OrderTransitionTrigger trigger,
            OrderTransitionRequest transitionRequest,
            String resultMessage,
            long lastFillQty,
            long cumQty,
            long leavesQty
    ) {
    }
}
