package com.trading.orderreliability.order.application;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "order-service")
public record OrderServiceProperties(MarketState marketState) {

    public OrderServiceProperties {
        if (marketState == null) {
            marketState = MarketState.OPEN;
        }
    }
}
