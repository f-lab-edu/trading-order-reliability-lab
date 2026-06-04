package com.trading.orderreliability.order.adapter.in.web;

import com.trading.orderreliability.order.application.CancelOrderCommand;
import com.trading.orderreliability.order.application.CancelOrderResult;
import com.trading.orderreliability.order.application.OrderApplicationService;
import com.trading.orderreliability.order.application.PlaceOrderCommand;
import com.trading.orderreliability.order.application.PlaceOrderResult;
import com.trading.orderreliability.order.domain.model.AccountId;
import com.trading.orderreliability.order.domain.model.Market;
import com.trading.orderreliability.order.domain.model.Order;
import com.trading.orderreliability.order.domain.model.OrderPrice;
import com.trading.orderreliability.order.domain.model.OrderQuantity;
import com.trading.orderreliability.order.domain.model.OrderSide;
import com.trading.orderreliability.order.domain.model.OrderStatus;
import com.trading.orderreliability.order.domain.model.OrderType;
import com.trading.orderreliability.order.domain.model.Symbol;
import com.trading.orderreliability.order.domain.model.TimeInForce;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderApplicationService orderApplicationService;

    public OrderController(OrderApplicationService orderApplicationService) {
        this.orderApplicationService = orderApplicationService;
    }

    @PostMapping
    public ResponseEntity<OrderCreatedResponse> createOrder(
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
            @RequestBody CreateOrderRequest request
    ) {
        requireRequestBody(request);
        PlaceOrderCommand command = new PlaceOrderCommand(
                requireText(request.clientOrderId(), "clientOrderId"),
                new AccountId(requireText(request.accountId(), "accountId")),
                requireValue(request.market(), "market"),
                new Symbol(requireText(request.symbol(), "symbol")),
                requireValue(request.side(), "side"),
                requireValue(request.orderType(), "orderType"),
                requireValue(request.tif(), "tif"),
                OrderQuantity.positive(request.orderQty()),
                new OrderPrice(requireValue(request.limitPrice(), "limitPrice")),
                traceIdOrNew(traceId)
        );
        PlaceOrderResult result = orderApplicationService.createOrder(command);
        Order order = result.order();
        if (!result.created()) {
            return ResponseEntity.ok(OrderCreatedResponse.from(order, request.clientOrderId()));
        }
        return ResponseEntity
                .created(URI.create("/api/orders/" + order.orderId().value()))
                .body(OrderCreatedResponse.from(order, request.clientOrderId()));
    }

    @GetMapping("/{orderId}")
    public OrderDetailResponse getOrder(@PathVariable UUID orderId) {
        return OrderDetailResponse.from(orderApplicationService.getOrder(orderId));
    }

    @GetMapping
    public List<OrderDetailResponse> listOrders(
            @RequestParam String accountId,
            @RequestParam(required = false) OrderStatus status,
            @RequestParam(defaultValue = "50") int limit
    ) {
        return orderApplicationService.listOrders(accountId, status, limit)
                .stream()
                .map(OrderDetailResponse::from)
                .toList();
    }

    @PostMapping("/{orderId}/cancellations")
    public CancelOrderResponse cancelOrder(
            @PathVariable UUID orderId,
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId,
            @RequestBody CancelOrderRequest request
    ) {
        requireRequestBody(request);
        CancelOrderCommand command = new CancelOrderCommand(
                new AccountId(requireText(request.accountId(), "accountId")),
                requireText(request.clientCancelRequestId(), "clientCancelRequestId"),
                traceIdOrNew(traceId)
        );
        CancelOrderResult result = orderApplicationService.cancelOrder(orderId, command);
        return CancelOrderResponse.from(result);
    }

    private static void requireRequestBody(Object request) {
        if (request == null) {
            throw new IllegalArgumentException("request body must not be null");
        }
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static <T> T requireValue(T value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        return value;
    }

    private static String traceIdOrNew(String traceId) {
        return traceId == null || traceId.isBlank() ? UUID.randomUUID().toString() : traceId;
    }

    public record CreateOrderRequest(
            String clientOrderId,
            String accountId,
            Market market,
            String symbol,
            OrderSide side,
            OrderType orderType,
            TimeInForce tif,
            long orderQty,
            BigDecimal limitPrice
    ) {
    }

    public record OrderCreatedResponse(
            UUID orderId,
            String clientOrderId,
            OrderStatus status,
            String reconciliationStatus,
            long orderQty,
            long cumQty,
            long leavesQty,
            Instant createdAt
    ) {

        static OrderCreatedResponse from(Order order, String clientOrderId) {
            return new OrderCreatedResponse(
                    order.orderId().value(),
                    clientOrderId,
                    order.status(),
                    order.reconciliationStatus().name(),
                    order.orderQty().value(),
                    order.cumQty().value(),
                    order.leavesQty().value(),
                    order.createdAt()
            );
        }
    }

    public record OrderDetailResponse(
            UUID orderId,
            String symbol,
            OrderSide side,
            OrderType orderType,
            TimeInForce tif,
            long orderQty,
            BigDecimal limitPrice,
            OrderStatus status,
            String reconciliationStatus,
            long cumQty,
            long leavesQty,
            boolean cancelPending,
            Instant createdAt,
            Instant updatedAt,
            Instant terminalAt
    ) {

        static OrderDetailResponse from(Order order) {
            return new OrderDetailResponse(
                    order.orderId().value(),
                    order.symbol().value(),
                    order.side(),
                    order.orderType(),
                    order.timeInForce(),
                    order.orderQty().value(),
                    order.limitPrice().value(),
                    order.status(),
                    order.reconciliationStatus().name(),
                    order.cumQty().value(),
                    order.leavesQty().value(),
                    order.status() == OrderStatus.PENDING_CANCEL,
                    order.createdAt(),
                    order.updatedAt(),
                    order.terminalAt()
            );
        }
    }

    public record CancelOrderRequest(
            String accountId,
            String clientCancelRequestId
    ) {
    }

    public record CancelOrderResponse(
            UUID orderId,
            String clientCancelRequestId,
            OrderStatus orderStatus,
            String cancelStatus,
            Instant requestedAt
    ) {

        static CancelOrderResponse from(CancelOrderResult result) {
            return new CancelOrderResponse(
                    result.order().orderId().value(),
                    result.instruction().clientInstructionId(),
                    result.order().status(),
                    result.instruction().status().name(),
                    result.instruction().createdAt()
            );
        }
    }
}
