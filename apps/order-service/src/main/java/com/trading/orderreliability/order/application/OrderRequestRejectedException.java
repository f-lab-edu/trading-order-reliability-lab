package com.trading.orderreliability.order.application;

public class OrderRequestRejectedException extends RuntimeException {

    private final String code;

    public OrderRequestRejectedException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
