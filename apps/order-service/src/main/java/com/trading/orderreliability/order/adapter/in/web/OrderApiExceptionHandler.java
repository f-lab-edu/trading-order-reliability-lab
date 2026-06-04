package com.trading.orderreliability.order.adapter.in.web;

import com.trading.orderreliability.order.application.ActiveCancelConflictException;
import com.trading.orderreliability.order.application.IdempotencyConflictException;
import com.trading.orderreliability.order.application.OrderAccessDeniedException;
import com.trading.orderreliability.order.application.OrderNotFoundException;
import com.trading.orderreliability.order.application.OrderRequestRejectedException;
import com.trading.orderreliability.order.domain.state.InvalidOrderTransitionException;

import java.time.Instant;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class OrderApiExceptionHandler {

    @ExceptionHandler(OrderNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse notFound(RuntimeException exception) {
        return ErrorResponse.of("ORDER_NOT_FOUND", exception.getMessage());
    }

    @ExceptionHandler(OrderAccessDeniedException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ErrorResponse forbidden(RuntimeException exception) {
        return ErrorResponse.of("ORDER_ACCESS_DENIED", exception.getMessage());
    }

    @ExceptionHandler({IdempotencyConflictException.class, ActiveCancelConflictException.class})
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse conflict(RuntimeException exception) {
        return ErrorResponse.of("CONFLICT", exception.getMessage());
    }

    @ExceptionHandler({OrderRequestRejectedException.class, InvalidOrderTransitionException.class})
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse rejected(RuntimeException exception) {
        if (exception instanceof OrderRequestRejectedException rejected) {
            return ErrorResponse.of(rejected.code(), rejected.getMessage());
        }
        return ErrorResponse.of("ORDER_TRANSITION_REJECTED", exception.getMessage());
    }

    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse badRequest(RuntimeException exception) {
        return ErrorResponse.of("BAD_REQUEST", exception.getMessage());
    }

    public record ErrorResponse(String code, String message, Instant occurredAt) {

        static ErrorResponse of(String code, String message) {
            return new ErrorResponse(code, message, Instant.now());
        }
    }
}
